// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package readiness registers collab's dependencies with go-buildinfo's
// health checker. Postgres and RabbitMQ are required: without them collab
// can't load or save a room or audit what happens in it. Core and identity are
// optional: live rooms keep relaying and checkpoints requeue while core is
// down, and only new tokens need identity, so either degrades collab instead
// of draining it.
package readiness

import (
	"context"
	"errors"
	"strings"

	"github.com/Bugs5382/go-buildinfo/health"
	postgres "github.com/Bugs5382/go-postgres"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Dependency names, as they appear in the report and the
// steward-depstate-<name> headers.
const (
	Postgres = "postgres"
	RabbitMQ = "rabbitmq"
	Identity = "identity"
	Core     = "core"
	// WorkloadAuth is the service-to-service token verifier's key set.
	WorkloadAuth = "workloadauth"
)

// Database is the Postgres the service runs on.
type Database interface {
	Ping(ctx context.Context) error
	ServerVersion(ctx context.Context) (string, error)
}

// Broker is the RabbitMQ connection; go-rabbitmq's Conn reports it.
type Broker interface{ Healthy() bool }

// Deps are the dependencies to report.
type Deps struct {
	Postgres Database
	Broker   Broker
	Identity func(ctx context.Context) error
	Core     func(ctx context.Context) error
	// WorkloadKeys loads the caller-token verifier's key set; required.
	WorkloadKeys func(ctx context.Context) error
	// WorkloadAuthDisabled reports WORKLOAD_AUTH=disabled as degraded.
	WorkloadAuthDisabled bool
}

var (
	errBrokerDown = errors.New("rabbitmq connection is down")
	errNotServing = errors.New("peer is not serving")
	errAuthOff    = errors.New("service-to-service authentication is disabled")
)

// New returns a checker with deps registered.
func New(d Deps, opts ...health.Option) (*health.Checker, error) {
	c := health.New(opts...)
	deps := []health.Dependency{
		{Name: Postgres, Required: true, Check: d.Postgres.Ping, Version: d.Postgres.ServerVersion},
		{Name: RabbitMQ, Required: true, Check: func(context.Context) error {
			if !d.Broker.Healthy() {
				return errBrokerDown
			}
			return nil
		}},
		{Name: Identity, Check: d.Identity},
		{Name: Core, Check: d.Core},
	}
	switch {
	case d.WorkloadKeys != nil:
		deps = append(deps, health.Dependency{Name: WorkloadAuth, Required: true, Check: d.WorkloadKeys})
	case d.WorkloadAuthDisabled:
		deps = append(deps, health.Dependency{Name: WorkloadAuth, Check: func(context.Context) error { return errAuthOff }})
	}
	return c, c.Register(deps...)
}

// GRPCPeer checks a peer service with the standard gRPC health check.
func GRPCPeer(conn grpc.ClientConnInterface) func(ctx context.Context) error {
	client := healthpb.NewHealthClient(conn)
	return func(ctx context.Context) error {
		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return errNotServing
		}
		return nil
	}
}

// PostgresDB adapts go-postgres's DB.
func PostgresDB(db *postgres.DB) Database { return pgDB{db} }

type pgDB struct{ db *postgres.DB }

func (p pgDB) Ping(ctx context.Context) error { return p.db.Ping(ctx) }

// ServerVersion drops the build suffix ("16.4 (Debian 16.4-1)"), which the
// header would redact.
func (p pgDB) ServerVersion(ctx context.Context) (string, error) {
	var v string
	if err := p.db.Pool().QueryRow(ctx, "SHOW server_version").Scan(&v); err != nil {
		return "", err
	}
	if f := strings.Fields(v); len(f) > 0 {
		return f[0], nil
	}
	return v, nil
}
