// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	workloadidentity "github.com/Bugs5382/go-workload-identity"
	"github.com/stretchr/testify/require"
)

func TestWorkloadEnvDefaultsTheAudienceToSteward(t *testing.T) {
	env := map[string]string{workloadidentity.EnvIssuer: "https://issuer.example.org"}
	getenv := workloadEnv(func(k string) string { return env[k] })
	require.Equal(t, WorkloadAudience, getenv(workloadidentity.EnvAudience))
	env[workloadidentity.EnvAudience] = "other"
	require.Equal(t, "other", getenv(workloadidentity.EnvAudience))
	require.Equal(t, "https://issuer.example.org", getenv(workloadidentity.EnvIssuer), "other variables pass through")
}

func TestStewardWorkloadMapsTheServiceAccountPrefix(t *testing.T) {
	c := StewardWorkload(workloadidentity.Config{Issuer: "https://issuer.example.org"})
	require.Equal(t, "steward", c.Audience)
	require.Equal(t, "steward-", c.ServiceAccountPrefix)

	c = StewardWorkload(workloadidentity.Config{Audience: "other", ServiceAccountPrefix: "app-"})
	require.Equal(t, "other", c.Audience, "a set audience stays")
	require.Equal(t, "steward-", c.ServiceAccountPrefix, "the prefix is always steward-")
}

func TestServerWorkloadConfigCarriesStewardValues(t *testing.T) {
	env := map[string]string{
		workloadidentity.EnvIssuer:                 "https://issuer.example.org",
		workloadidentity.EnvAllowedServiceAccounts: "steward/steward-gateway",
	}
	c, enabled, err := serverWorkloadConfig(func(k string) string { return env[k] })
	require.NoError(t, err)
	require.True(t, enabled)
	require.Equal(t, WorkloadAudience, c.Audience)
	require.Equal(t, WorkloadServiceAccountPrefix, c.ServiceAccountPrefix)

	env = map[string]string{workloadidentity.EnvAuthMode: workloadidentity.AuthDisabled}
	_, enabled, err = serverWorkloadConfig(func(k string) string { return env[k] })
	require.NoError(t, err)
	require.False(t, enabled)
}
