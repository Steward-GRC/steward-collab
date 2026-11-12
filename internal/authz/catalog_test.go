// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package authz_test

import (
	"os"
	"strings"
	"testing"

	"github.com/Bugs5382/go-authz/rbac"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-collab/internal/authz"
)

// The embedded catalog is a copy of the canonical Steward catalog. A drift in
// either copy fails here; refresh both with `task catalog:refresh COMMIT=<sha>`.
func TestEmbeddedCatalogMatchesTheCanonicalOne(t *testing.T) {
	raw, err := os.ReadFile("testdata/catalog.canonical.json")
	require.NoError(t, err)
	canonical, err := rbac.Parse(raw)
	require.NoError(t, err)
	require.Empty(t, rbac.Diff(authz.Catalog, canonical))
}

func TestEmbeddedCatalogIsByteIdenticalToTheCanonicalFile(t *testing.T) {
	embedded, err := os.ReadFile("catalog.json")
	require.NoError(t, err)
	canonical, err := os.ReadFile("testdata/catalog.canonical.json")
	require.NoError(t, err)
	require.Equal(t, string(canonical), string(embedded))
}

func TestCatalogSourceNamesTheCommit(t *testing.T) {
	raw, err := os.ReadFile("testdata/CATALOG_SOURCE")
	require.NoError(t, err)
	src := string(raw)
	require.Regexp(t, `(?m)^commit: [0-9a-f]{40}$`, src)
	require.True(t, strings.Contains(src, "path: internal/authz/catalog.json"))
	require.True(t, strings.Contains(src, "repo: Steward-GRC/steward-gateway"))
}

func TestCatalogNamesThePermissionTheEditCheckUses(t *testing.T) {
	p, err := authz.Catalog.ParsePermission(string(authz.PolicyAuthor))
	require.NoError(t, err)
	require.Equal(t, authz.PolicyAuthor, p)
}
