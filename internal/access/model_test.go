// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package access

import (
	"testing"

	"github.com/Bugs5382/go-authz/ordered"
	"github.com/stretchr/testify/require"
)

// The root seat is exempt for Read, Author and Manage only; Approve and Ack
// follow the rows for root too.
func TestModelRootSeatActions(t *testing.T) {
	su := model().Superusers
	require.NotNil(t, su)
	require.Equal(t, []string{"role:" + rootRole}, su.Subjects)
	require.ElementsMatch(t, []string{actRead, actAuthor, actManage}, su.Actions)
}

func TestModelRootApproveFollowsTheRows(t *testing.T) {
	root := ordered.Person{ID: "user-root", Roles: []string{rootRole}}
	build := func(entries ...ordered.Entry) ordered.RuleSet {
		return ordered.RuleSet{
			Categories: map[string]ordered.Category{"c": {Entries: entries}},
			Parents:    map[string]string{},
		}
	}
	m := model()
	require.False(t, m.Decide(build(), root, "c", actApprove).Allowed, "no row: root does not approve")
	require.False(t, m.Decide(build(ordered.Entry{Subject: ordered.Everyone, Action: actApprove, Effect: ordered.Deny}), root, "c", actApprove).Allowed)
	require.True(t, m.Decide(build(ordered.Entry{Subject: "user:user-root", Action: actApprove, Effect: ordered.Allow}), root, "c", actApprove).Allowed)
	// Author is still exempt, whatever the rows say.
	require.True(t, m.Decide(build(ordered.Entry{Subject: ordered.Everyone, Action: actAuthor, Effect: ordered.Deny}), root, "c", actAuthor).Allowed)
}
