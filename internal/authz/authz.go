// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package authz is collab's copy of the Steward permission catalog. The
// canonical catalog lives in the gateway (internal/authz/catalog.json); this
// service embeds a byte-identical copy and a test requires it to match
// testdata/catalog.canonical.json, which `task catalog:refresh` re-fetches. The
// mechanism is go-authz's rbac package. Category rule decisions are not made
// here; they come from go-authz/ordered.
package authz

import (
	_ "embed"

	"github.com/Bugs5382/go-authz/rbac"
)

//go:embed catalog.json
var catalogJSON []byte

// Catalog is the parsed catalog. It is read-only; one that does not parse
// stops the process at start-up.
var Catalog = rbac.MustParse(catalogJSON)

// PolicyAuthor is the catalog permission to edit a draft.
const PolicyAuthor rbac.Permission = "policy.author"

// RoleSiteAdmin is the catalog role that bypasses scoped permissions. The edit
// check does not honour that bypass: a site admin edits through a rule.
const RoleSiteAdmin rbac.Role = "site-admin"
