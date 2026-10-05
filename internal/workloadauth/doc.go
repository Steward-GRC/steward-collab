// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package workloadauth authenticates gRPC calls between Steward services with
// Kubernetes workload identity.
//
// A caller sends its projected ServiceAccount token (audience "steward") as
// "authorization: Bearer <token>" metadata, read from WORKLOAD_TOKEN_FILE on
// every call (NewTokenCredentials, DialOptionFromEnv). A callee verifies the
// token against the cluster issuer's JWKS (Verifier), maps the service account
// "<namespace>/steward-<name>" to the caller name "<name>", and checks a
// per-method allow-list in code (Policy) before the handler runs
// (UnaryServerInterceptor, StreamServerInterceptor):
//
//   - a caller missing from a method's list is refused with PermissionDenied;
//   - a caller listed as Self acts only as itself, so a request from it that
//     carries an `actor` field is refused with PermissionDenied;
//   - a caller listed as OnBehalf may pass an end-user actor.
//
// The package is self-contained: it imports no service code or protos, and the
// same files are copied byte-identical into every Steward service under
// internal/workloadauth. The copy in steward-core is the origin one.
package workloadauth
