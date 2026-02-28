//go:build goexperiment.jsonv2

// Package priority provides priority tier logic for artist batch updates.
package priority

import "github.com/pocketbase/pocketbase/core"

type Tier int

const (
	P0_Queued Tier = iota
	P1_RockRecent
	P2_OtherRecent
	P3_RockNotAdded
	P4_OtherNotAdded
	P5_RockIncluded
	P6_OtherIncluded
	P_Unknown
)

type Job struct {
	Record   *core.Record
	Priority Tier
}

func Determine(r *core.Record) Tier {
	if r.GetString("fetch_status") == "pending" || r.GetString("list_status") == "waiting" {
		return P0_Queued
	}

	genre := r.GetString("genre_group")
	status := r.GetString("list_status")
	isRock := genre == "rock_metal"

	switch status {
	case "recently_added":
		if isRock {
			return P1_RockRecent
		}
		return P2_OtherRecent
	case "not_added":
		if isRock {
			return P3_RockNotAdded
		}
		return P4_OtherNotAdded
	case "included":
		if isRock {
			return P5_RockIncluded
		}
		return P6_OtherIncluded
	}

	return P_Unknown
}

func (t Tier) String() string {
	switch t {
	case P0_Queued:
		return "P0_Queued"
	case P1_RockRecent:
		return "P1_RockRecent"
	case P2_OtherRecent:
		return "P2_OtherRecent"
	case P3_RockNotAdded:
		return "P3_RockNotAdded"
	case P4_OtherNotAdded:
		return "P4_OtherNotAdded"
	case P5_RockIncluded:
		return "P5_RockIncluded"
	case P6_OtherIncluded:
		return "P6_OtherIncluded"
	default:
		return "P_Unknown"
	}
}
