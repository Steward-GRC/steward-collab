// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"strings"

	workloadidentity "github.com/Bugs5382/go-workload-identity"
)

// Steward's workload-identity values. go-workload-identity carries no product
// defaults, so the service supplies them here.
const (
	// WorkloadAudience is the token audience required when WORKLOAD_AUDIENCE
	// is unset, and the audience every Steward caller's token is minted for.
	WorkloadAudience = "steward"
	// WorkloadServiceAccountPrefix is stripped from a caller's service account
	// to give its caller name: "steward-gateway" is the caller "gateway".
	WorkloadServiceAccountPrefix = "steward-"
)

// workloadEnv is getenv with WORKLOAD_AUDIENCE defaulting to WorkloadAudience.
func workloadEnv(getenv func(string) string) func(string) string {
	return func(k string) string {
		v := getenv(k)
		if k == workloadidentity.EnvAudience && strings.TrimSpace(v) == "" {
			return WorkloadAudience
		}
		return v
	}
}

// StewardWorkload fills in Steward's audience, when unset, and its
// service-account to caller-name mapping.
func StewardWorkload(c workloadidentity.Config) workloadidentity.Config {
	if c.Audience == "" {
		c.Audience = WorkloadAudience
	}
	c.ServiceAccountPrefix = WorkloadServiceAccountPrefix
	return c
}

// serverWorkloadConfig is workloadidentity.ServerConfigFromEnv with Steward's
// values filled in.
func serverWorkloadConfig(getenv func(string) string) (workloadidentity.Config, bool, error) {
	c, enabled, err := workloadidentity.ServerConfigFromEnv(workloadEnv(getenv))
	if err != nil || !enabled {
		return c, enabled, err
	}
	return StewardWorkload(c), true, nil
}
