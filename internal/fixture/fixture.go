// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package fixture holds the sample data the tests in this repo use, from the
// Steward design brief. Nothing here is a real person, group or document.
package fixture

// People, by user id.
const (
	Alice = "user-alice" // site admin, root
	Bob   = "user-bob"   // author
	Carol = "user-carol" // approver
	Dave  = "user-dave"  // category owner
	Erin  = "user-erin"  // reader
	Frank = "user-frank" // template admin
	Grace = "user-grace" // compliance admin
	Heidi = "user-heidi" // group manager
)

// Categories, by id.
const (
	Workplace  = "cat-workplace"
	Facilities = "cat-facilities"
	Finance    = "cat-finance"
	Expenses   = "cat-expenses"
)

// Directory groups.
const (
	FacilitiesTeam = "facilities-team"
	FinanceTeam    = "finance-team"
	Approvers      = "approvers"
)

// Documents.
const (
	DeskBookingPolicy       = "Desk Booking Policy"
	DeskBookingPolicyNumber = "POL-FACILITIES-000001"
	ExpenseClaimsPolicy     = "Expense Claims Policy"
)

// Workflows.
const (
	Standard2Stage = "Standard 2-stage"
	FinanceSignOff = "Finance sign-off"
)
