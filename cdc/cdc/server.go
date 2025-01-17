// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/tikv/migration/cdc/cdc/capture"
	"github.com/tikv/migration/cdc/cdc/kv"
	"github.com/tikv/migration/cdc/cdc/sorter/unified"
	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/etcd"
	"github.com/tikv/migration/cdc/pkg/fsutil"
	"github.com/tikv/migration/cdc/pkg/httputil"
	"github.com/tikv/migration/cdc/pkg/tcpserver"
	"github.com/tikv/migration/cdc/pkg/util"
	"github.com/tikv/migration/cdc/pkg/version"
)

const (
	defaultDataDir = "/tmp/cdc_data"
	// dataDirThreshold is used to warn if the free space of the specified data-dir is lower than it, unit is GB
	dataDirThreshold = 500
)

// Server is the capture server
// TODO: we need to make Server more unit testable and add more test cases.
// Especially we need to decouple the HTTPServer out of Server.
type Server struct {
	capture      *capture.Capture
	tcpServer    tcpserver.TCPServer
	statusServer *http.Server
	pdClient     pd.Client
	etcdClient   *etcd.CDCEtcdClient
	kvStorage    tidbkv.Storage
	pdEndpoints  []string
}

// NewServer creates a Server instance.
func NewServer(pdEndpoints []string) (*Server, error) {
	conf := config.GetGlobalServerConfig()
	log.Info("creating CDC server",
		zap.Strings("pd-addrs", pdEndpoints),
		zap.Stringer("config", conf),
	)

	// This is to make communication between nodes possible.
	// In other words, the nodes have to trust each other.
	if len(conf.Security.CertAllowedCN) != 0 {
		err := conf.Security.AddSelfCommonName()
		if err != nil {
			log.Error("status server set tls config failed", zap.Error(err))
			return nil, errors.Trace(err)
		}
	}

	// tcpServer is the unified frontend of the CDC server that serves
	// both RESTful APIs and gRPC APIs.
	// Note that we pass the TLS config to the tcpServer, so there is no need to
	// configure TLS elsewhere.
	tcpServer, err := tcpserver.NewTCPServer(conf.Addr, conf.Security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &Server{
		pdEndpoints: pdEndpoints,
		tcpServer:   tcpServer,
	}

	return s, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()

	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}

	pdClient, err := pd.NewClientWithContext(
		ctx, s.pdEndpoints, conf.Security.PDSecurityOption(),
		pd.WithGRPCDialOptions(
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		))
	if err != nil {
		return cerror.WrapError(cerror.ErrServerNewPDClient, err)
	}
	s.pdClient = pdClient

	tlsConfig, err := conf.Security.ToTLSConfig()
	if err != nil {
		return errors.Trace(err)
	}

	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	createEtcdClient := func(ctx context.Context) (*etcd.CDCEtcdClient, error) {
		etcdCli, err := clientv3.New(clientv3.Config{
			Endpoints:   s.pdEndpoints,
			TLS:         tlsConfig,
			Context:     ctx,
			LogConfig:   &logConfig,
			DialTimeout: 5 * time.Second,
			DialOptions: []grpc.DialOption{
				grpcTLSOption,
				grpc.WithBlock(),
				grpc.WithConnectParams(grpc.ConnectParams{
					Backoff: backoff.Config{
						BaseDelay:  time.Second,
						Multiplier: 1.1,
						Jitter:     0.1,
						MaxDelay:   3 * time.Second,
					},
					MinConnectTimeout: 3 * time.Second,
				}),
			},
		})
		if err != nil {
			return nil, errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "new etcd client")
		}

		cdcEtcdClient := etcd.NewCDCEtcdClient(ctx, etcdCli)
		return &cdcEtcdClient, nil
	}

	cdcEtcdClient, err := createEtcdClient(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	s.etcdClient = cdcEtcdClient

	err = s.initDir(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// To not block CDC server startup, we need to warn instead of error
	// when TiKV is incompatible.
	errorTiKVIncompatible := false
	err = version.CheckClusterVersion(ctx, s.pdClient, s.pdEndpoints, conf.Security, errorTiKVIncompatible)
	if err != nil {
		return err
	}

	kv.InitWorkerPool()
	kvStore, err := kv.CreateTiStore(strings.Join(s.pdEndpoints, ","), conf.Security)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err := kvStore.Close()
		if err != nil {
			log.Warn("kv store close failed", zap.Error(err))
		}
	}()
	s.kvStorage = kvStore
	ctx = util.PutKVStorageInCtx(ctx, kvStore)

	s.capture = capture.NewCapture(s.pdClient, s.kvStorage, createEtcdClient)

	err = s.startStatusHTTP(s.tcpServer.HTTP1Listener())
	if err != nil {
		return err
	}

	return s.run(ctx)
}

func (s *Server) etcdHealthChecker(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	conf := config.GetGlobalServerConfig()

	httpCli, err := httputil.NewClient(conf.Security)
	if err != nil {
		return err
	}
	defer httpCli.CloseIdleConnections()
	metrics := make(map[string]prometheus.Observer)
	for _, pdEndpoint := range s.pdEndpoints {
		metrics[pdEndpoint] = etcdHealthCheckDuration.WithLabelValues(conf.AdvertiseAddr, pdEndpoint)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for _, pdEndpoint := range s.pdEndpoints {
				start := time.Now()
				ctx, cancel := context.WithTimeout(ctx, time.Second*10)
				req, err := http.NewRequestWithContext(
					ctx, http.MethodGet, fmt.Sprintf("%s/health", pdEndpoint), nil)
				if err != nil {
					log.Warn("etcd health check failed", zap.Error(err))
					cancel()
					continue
				}
				_, err = httpCli.Do(req)
				if err != nil {
					log.Warn("etcd health check error", zap.Error(err))
				} else {
					metrics[pdEndpoint].Observe(float64(time.Since(start)) / float64(time.Second))
				}
				cancel()
			}
		}
	}
}

func (s *Server) run(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, cctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return s.capture.Run(cctx)
	})

	wg.Go(func() error {
		return s.etcdHealthChecker(cctx)
	})

	wg.Go(func() error {
		return unified.RunWorkerPool(cctx)
	})

	wg.Go(func() error {
		return kv.RunWorkerPool(cctx)
	})

	wg.Go(func() error {
		return s.tcpServer.Run(cctx)
	})

	return wg.Wait()
}

// Close closes the server.
func (s *Server) Close() {
	if s.capture != nil {
		s.capture.AsyncClose()
	}
	if s.statusServer != nil {
		err := s.statusServer.Close()
		if err != nil {
			log.Error("close status server", zap.Error(err))
		}
		s.statusServer = nil
	}
	if s.tcpServer != nil {
		err := s.tcpServer.Close()
		if err != nil {
			log.Error("close tcp server", zap.Error(err))
		}
		s.tcpServer = nil
	}
}

func (s *Server) initDir(ctx context.Context) error {
	if err := s.setUpDir(ctx); err != nil {
		return errors.Trace(err)
	}
	conf := config.GetGlobalServerConfig()
	// Ensure data dir exists and read-writable.
	diskInfo, err := checkDir(conf.DataDir)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info(fmt.Sprintf("%s is set as data-dir (%dGB available), sort-dir=%s. "+
		"It is recommended that the disk for data-dir at least have %dGB available space",
		conf.DataDir, diskInfo.Avail, conf.Sorter.SortDir, dataDirThreshold))

	// Ensure sorter dir exists and read-writable.
	_, err = checkDir(conf.Sorter.SortDir)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *Server) setUpDir(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()
	if conf.DataDir != "" {
		conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
		config.StoreGlobalServerConfig(conf)

		return nil
	}

	// data-dir will be decided by exist changefeed for backward compatibility
	allInfo, err := s.etcdClient.GetAllChangeFeedInfo(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	candidates := make([]string, 0, len(allInfo))
	for _, info := range allInfo {
		if info.SortDir != "" {
			candidates = append(candidates, info.SortDir)
		}
	}

	conf.DataDir = defaultDataDir
	best, ok := findBestDataDir(candidates)
	if ok {
		conf.DataDir = best
	}

	conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
	config.StoreGlobalServerConfig(conf)
	return nil
}

func checkDir(dir string) (*fsutil.DiskInfo, error) {
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := fsutil.IsDirReadWritable(dir); err != nil {
		return nil, errors.Trace(err)
	}
	return fsutil.GetDiskInfo(dir)
}

// try to find the best data dir by rules
// at the moment, only consider available disk space
func findBestDataDir(candidates []string) (result string, ok bool) {
	var low uint64 = 0

	for _, dir := range candidates {
		info, err := checkDir(dir)
		if err != nil {
			log.Warn("check the availability of dir", zap.String("dir", dir), zap.Error(err))
			continue
		}
		if info.Avail > low {
			result = dir
			low = info.Avail
			ok = true
		}
	}

	if !ok && len(candidates) != 0 {
		log.Warn("try to find directory for data-dir failed, use `/tmp/cdc_data` as data-dir", zap.Strings("candidates", candidates))
	}

	return result, ok
}
