// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package access decides whether a user may co-edit a policy's draft: the
// policy's owner, or a user steward-authz allows policy.author in the policy's
// home category or one of its ancestors. The user comes from steward-identity
// and the policy and its categories from steward-core.
package access

import (
	"context"
	"errors"
	"fmt"

	stewardauthz "github.com/Steward-GRC/steward-authz"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/core/v1"
	identityv1 "github.com/Steward-GRC/steward-collab/gen/go/thirdparty/identity/v1"
)

var (
	// ErrPolicyNotFound means core has no such policy.
	ErrPolicyNotFound = errors.New("access: policy not found")
	// ErrUnavailable means core or identity couldn't answer in time.
	ErrUnavailable = errors.New("access: a peer service is unavailable")
)

// maxDepth bounds the walk up the category tree. Core keeps the tree three
// levels deep; the bound only stops a corrupt parent loop.
const maxDepth = 16

// UserReader is the part of identity's read service the check calls.
type UserReader interface {
	GetUser(ctx context.Context, in *identityv1.GetUserRequest, opts ...grpc.CallOption) (*identityv1.GetUserResponse, error)
}

// PolicyReader is the part of core's PolicyService the check calls.
type PolicyReader interface {
	GetPolicy(ctx context.Context, in *corev1.GetPolicyRequest, opts ...grpc.CallOption) (*corev1.GetPolicyResponse, error)
}

// CategoryReader is the part of core's CategoryService the check calls.
type CategoryReader interface {
	GetCategory(ctx context.Context, in *corev1.GetCategoryRequest, opts ...grpc.CallOption) (*corev1.GetCategoryResponse, error)
}

// Checker makes the edit decision.
type Checker struct {
	users      UserReader
	policies   PolicyReader
	categories CategoryReader
}

// New returns a Checker.
func New(users UserReader, policies PolicyReader, categories CategoryReader) *Checker {
	return &Checker{users: users, policies: policies, categories: categories}
}

// Result is the decision, and the user's display name for presence.
type Result struct {
	Allowed bool
	Name    string
}

// CanEdit decides for userID on policyID. A user identity doesn't know is
// denied, not an error.
func (c *Checker) CanEdit(ctx context.Context, userID, policyID string) (Result, error) {
	pr, err := c.policies.GetPolicy(ctx, &corev1.GetPolicyRequest{Id: policyID})
	if status.Code(err) == codes.NotFound {
		return Result{}, ErrPolicyNotFound
	}
	if err != nil {
		return Result{}, peerErr("core GetPolicy", err)
	}
	policy := pr.GetPolicy()

	ur, err := c.users.GetUser(ctx, &identityv1.GetUserRequest{UserId: userID})
	if status.Code(err) == codes.NotFound {
		return Result{}, nil
	}
	if err != nil {
		return Result{}, peerErr("identity GetUser", err)
	}
	user := ur.GetUser()
	res := Result{Name: user.GetName()}
	if policy.GetOwnerUserId() != "" && policy.GetOwnerUserId() == user.GetId() {
		res.Allowed = true
		return res, nil
	}

	lineage, err := c.lineage(ctx, policy.GetHomeCategoryId())
	if err != nil {
		return Result{}, err
	}
	resource := &stewardauthz.Resource{ID: policy.GetNumber(), CategoryLineage: lineage}
	if len(lineage) > 0 {
		resource.Category = lineage[0]
	}
	res.Allowed = stewardauthz.Authorize(subject(user), stewardauthz.PolicyAuthor, resource).Allowed()
	return res, nil
}

// lineage returns the category names from id up to the root: steward-authz
// scopes grants by category name.
func (c *Checker) lineage(ctx context.Context, id string) ([]string, error) {
	var names []string
	for depth := 0; id != "" && depth < maxDepth; depth++ {
		resp, err := c.categories.GetCategory(ctx, &corev1.GetCategoryRequest{Id: id})
		if err != nil {
			return nil, peerErr("core GetCategory", err)
		}
		names = append(names, resp.GetCategory().GetName())
		id = resp.GetCategory().GetParentId()
	}
	return names, nil
}

// subject turns an identity user into a steward-authz subject. Names
// steward-authz doesn't know are left out.
func subject(u *identityv1.User) stewardauthz.Subject {
	s := stewardauthz.Subject{
		UserID:        u.GetId(),
		Groups:        u.GetIdpGroups(),
		ReadSensitive: u.GetReadSensitiveGrant(),
		Root:          u.GetIsRoot(),
	}
	for _, name := range u.GetRoles() {
		if r, err := stewardauthz.ParseRole(name); err == nil {
			s.Roles = append(s.Roles, r)
		}
	}
	for _, sr := range u.GetScopedRoles() {
		if r, err := stewardauthz.ParseRole(sr.GetRole()); err == nil {
			s.ScopedGrants = append(s.ScopedGrants, stewardauthz.ScopedGrant{Role: r, Category: sr.GetCategory()})
		}
	}
	return s
}

func peerErr(call string, err error) error {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return fmt.Errorf("%s: %w: %w", call, ErrUnavailable, err)
	}
	return fmt.Errorf("%s: %w", call, err)
}
