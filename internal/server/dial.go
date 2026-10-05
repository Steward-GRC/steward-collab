// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	grpcactor "github.com/Bugs5382/go-grpc-actor"
	gootel "github.com/Bugs5382/go-otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Steward-GRC/steward-collab/internal/workloadauth"
)

// DialOptions are the options for every outbound connection (core and
// identity). Each call carries collab's projected service-account token,
// read from tokenFile on every call, and go-grpc-actor puts the request's
// actor, and during act-as the real admin, on it. An empty tokenFile is
// WORKLOAD_AUTH=disabled and sends no token. A token file that can't be read
// now fails, so a missing mount stops the boot instead of every call.
func DialOptions(tokenFile string) ([]grpc.DialOption, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(gootel.GRPCClientStatsHandler()),
		grpc.WithChainUnaryInterceptor(grpcactor.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(grpcactor.StreamClientInterceptor()),
	}
	if tokenFile == "" {
		return opts, nil
	}
	token, _, err := workloadauth.DialOptionFromEnv(func(k string) string {
		if k == workloadauth.EnvTokenFile {
			return tokenFile
		}
		return ""
	})
	if err != nil {
		return nil, err
	}
	return append(opts, token), nil
}
