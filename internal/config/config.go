// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package config reads the collab service's settings from the environment.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
)

// MinTokenSecretLen is the shortest COLLAB_TOKEN_SECRET accepted, in bytes.
const MinTokenSecretLen = 32

// DefaultTokenFile is where the release mounts collab's projected
// service-account token.
const DefaultTokenFile = workloadauth.DefaultTokenFile

// Config is every setting the service runs with.
type Config struct {
	DatabaseDSN string
	// MigrateDSN is a direct connection for migrations; defaults to
	// DatabaseDSN.
	MigrateDSN    string
	MigrationsDir string
	RabbitURL     string
	GRPCPort      string
	// HTTPPort serves the websocket upgrades, /livez and /readyz.
	HTTPPort     string
	OTLPEndpoint string
	CoreAddr     string
	IdentityAddr string
	// TokenSecret signs and verifies the co-editing tokens.
	TokenSecret []byte
	TokenTTL    time.Duration
	// WsBase is an origin put in front of the issued websocket path; empty
	// gives a same-origin relative path.
	WsBase string
	// AllowedOrigins are the browser origins allowed to upgrade directly on
	// collab's own listener. Empty refuses every browser: they come through
	// the gateway's proxy.
	AllowedOrigins []string
	// WorkloadAuth is false only for WORKLOAD_AUTH=disabled, on local runs.
	WorkloadAuth bool
	// Workload verifies the callers' service-account tokens.
	Workload workloadauth.Config
	// TokenFile is collab's own projected token, sent on every call to core
	// and identity; empty when WorkloadAuth is off.
	TokenFile string
}

// Load reads the settings through getenv (os.Getenv in production).
func Load(getenv func(string) string) (Config, error) {
	or := func(k, d string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return d
	}
	c := Config{
		DatabaseDSN:    getenv("DATABASE_DSN"),
		MigrationsDir:  or("MIGRATIONS_DIR", "migrations"),
		RabbitURL:      getenv("RABBITMQ_URL"),
		GRPCPort:       or("GRPC_PORT", "9090"),
		HTTPPort:       or("HTTP_PORT", "8081"),
		OTLPEndpoint:   or("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		CoreAddr:       or("CORE_GRPC_ADDR", "core:9090"),
		IdentityAddr:   or("IDENTITY_GRPC_ADDR", "identity:9090"),
		TokenSecret:    []byte(getenv("COLLAB_TOKEN_SECRET")),
		WsBase:         getenv("COLLAB_WS_BASE"),
		AllowedOrigins: list(getenv("COLLAB_ALLOWED_ORIGINS")),
	}
	c.MigrateDSN = or("MIGRATE_DSN", c.DatabaseDSN)

	var errs []error
	if c.DatabaseDSN == "" {
		errs = append(errs, errors.New("DATABASE_DSN is required"))
	}
	if c.RabbitURL == "" {
		errs = append(errs, errors.New("RABBITMQ_URL is required"))
	}
	switch {
	case len(c.TokenSecret) == 0:
		errs = append(errs, errors.New("COLLAB_TOKEN_SECRET is required"))
	case len(c.TokenSecret) < MinTokenSecretLen:
		errs = append(errs, fmt.Errorf("COLLAB_TOKEN_SECRET must be at least %d bytes", MinTokenSecretLen))
	}
	secs, err := strconv.Atoi(or("COLLAB_TOKEN_TTL_SECONDS", "300"))
	if err != nil || secs <= 0 {
		errs = append(errs, errors.New("COLLAB_TOKEN_TTL_SECONDS must be a whole number of seconds above 0"))
	}
	c.TokenTTL = time.Duration(secs) * time.Second
	if c.Workload, c.WorkloadAuth, err = workloadauth.ServerConfigFromEnv(getenv); err != nil {
		errs = append(errs, err)
	}
	if c.WorkloadAuth {
		c.TokenFile = or(workloadauth.EnvTokenFile, DefaultTokenFile)
	}
	return c, errors.Join(errs...)
}

// list splits a comma-separated value, dropping blanks, so a stray comma
// never adds an empty entry.
func list(raw string) []string {
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
