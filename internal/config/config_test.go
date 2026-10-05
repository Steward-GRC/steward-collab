// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func required() map[string]string {
	return withWorkload(base())
}

func base() map[string]string {
	return map[string]string{
		"DATABASE_DSN":        "postgres://collab:collab@localhost:5432/collab",
		"RABBITMQ_URL":        "amqp://guest:guest@localhost:5672/",
		"COLLAB_TOKEN_SECRET": "test-secret-32-bytes-long-------",
	}
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(required()))
	require.NoError(t, err)
	require.Equal(t, "9090", c.GRPCPort)
	require.Equal(t, "8081", c.HTTPPort)
	require.Equal(t, "migrations", c.MigrationsDir)
	require.Equal(t, c.DatabaseDSN, c.MigrateDSN)
	require.Equal(t, "core:9090", c.CoreAddr)
	require.Equal(t, "identity:9090", c.IdentityAddr)
	require.Equal(t, 5*time.Minute, c.TokenTTL)
	require.Empty(t, c.WsBase, "the default ws_url is same-origin relative")
	require.Empty(t, c.AllowedOrigins, "no browser may upgrade directly by default")
	require.Equal(t, []byte("test-secret-32-bytes-long-------"), c.TokenSecret)
}

func TestLoadMissingRequired(t *testing.T) {
	for _, k := range []string{"DATABASE_DSN", "RABBITMQ_URL", "COLLAB_TOKEN_SECRET"} {
		m := required()
		delete(m, k)
		_, err := Load(env(m))
		require.ErrorContains(t, err, k)
	}
}

// A short secret would make every token easy to forge.
func TestLoadRejectsAShortTokenSecret(t *testing.T) {
	m := required()
	m["COLLAB_TOKEN_SECRET"] = "too-short"
	_, err := Load(env(m))
	require.ErrorContains(t, err, "COLLAB_TOKEN_SECRET")
}

func TestLoadTokenTTL(t *testing.T) {
	m := required()
	m["COLLAB_TOKEN_TTL_SECONDS"] = "30"
	c, err := Load(env(m))
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, c.TokenTTL)

	for _, bad := range []string{"soon", "0", "-5"} {
		m["COLLAB_TOKEN_TTL_SECONDS"] = bad
		_, err = Load(env(m))
		require.ErrorContains(t, err, "COLLAB_TOKEN_TTL_SECONDS", bad)
	}
}

func TestLoadAllowedOrigins(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "unset", raw: "", want: nil},
		{name: "single", raw: "https://a.example", want: []string{"https://a.example"}},
		{name: "multiple with whitespace", raw: " https://a.example , https://b.example ", want: []string{"https://a.example", "https://b.example"}},
		// A stray comma must not produce an empty entry: an empty allowlist
		// entry would match the empty Origin header check by accident.
		{name: "stray commas", raw: ",https://a.example,,", want: []string{"https://a.example"}},
		{name: "only commas", raw: ",,", want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := required()
			m["COLLAB_ALLOWED_ORIGINS"] = tc.raw
			c, err := Load(env(m))
			require.NoError(t, err)
			require.Equal(t, tc.want, c.AllowedOrigins)
		})
	}
}

const issuer = "https://issuer.example.org"

func withWorkload(m map[string]string) map[string]string {
	m["WORKLOAD_OIDC_ISSUER"] = issuer
	m["WORKLOAD_ALLOWED_SERVICEACCOUNTS"] = "steward/steward-gateway"
	return m
}

func TestLoadWorkloadAuth(t *testing.T) {
	c, err := Load(env(required()))
	require.NoError(t, err)
	require.True(t, c.WorkloadAuth)
	require.Equal(t, issuer, c.Workload.Issuer)
	require.Equal(t, "steward", c.Workload.Audience, "Steward's audience, not the package default")
	require.Equal(t, []string{"steward/steward-gateway"}, c.Workload.AllowedServiceAccounts)
	require.Equal(t, DefaultTokenFile, c.TokenFile)
}

// Authentication fails closed: no issuer is a start-up error unless it is
// switched off on purpose.
func TestLoadWorkloadAuthFailsClosed(t *testing.T) {
	_, err := Load(env(base()))
	require.ErrorContains(t, err, "WORKLOAD_OIDC_ISSUER")

	m := base()
	m["WORKLOAD_AUTH"] = "off"
	_, err = Load(env(m))
	require.ErrorContains(t, err, "WORKLOAD_AUTH")
}

func TestLoadWorkloadAuthDisabled(t *testing.T) {
	m := base()
	m["WORKLOAD_AUTH"] = "disabled"
	c, err := Load(env(m))
	require.NoError(t, err)
	require.False(t, c.WorkloadAuth)
	require.Empty(t, c.TokenFile, "a disabled run sends no token")
}

func TestLoadTokenFile(t *testing.T) {
	m := required()
	m["WORKLOAD_TOKEN_FILE"] = "/run/token"
	c, err := Load(env(m))
	require.NoError(t, err)
	require.Equal(t, "/run/token", c.TokenFile)
}
