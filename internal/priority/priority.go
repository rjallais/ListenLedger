// Package priority provides priority tier logic for artist batch updates.
package priority

import "github.com/pocketbase/pocketbase/core"

type Tier int

const (
	P0Queued Tier = iota
	P1RockRecent
	P2OtherRecent
	P3RockNotAdded
	P4OtherNotAdded
	P5RockIncluded
	P6OtherIncluded
	PUnknown
)

type Job struct {
	Record   *core.Record
	Priority Tier
}

func Determine(r *core.Record) Tier {
	if r.GetString("fetch_status") == "pending" || r.GetString("list_status") == "waiting" {
		return P0Queued
	}

	genre := r.GetString("genre_group")
	status := r.GetString("list_status")
	isRock := genre == "rock_metal"

	switch status {
	case "recently_added":
		if isRock {
			return P1RockRecent
		}
		return P2OtherRecent
	case "not_added":
		if isRock {
			return P3RockNotAdded
		}
		return P4OtherNotAdded
	case "included":
		if isRock {
			return P5RockIncluded
		}
		return P6OtherIncluded
	}

	return PUnknown
}

func (t Tier) String() string {
	switch t {
	case P0Queued:
		return "P0Queued"
	case P1RockRecent:
		return "P1RockRecent"
	case P2OtherRecent:
		return "P2OtherRecent"
	case P3RockNotAdded:
		return "P3RockNotAdded"
	case P4OtherNotAdded:
		return "P4OtherNotAdded"
	case P5RockIncluded:
		return "P5RockIncluded"
	case P6OtherIncluded:
		return "P6OtherIncluded"
	default:
		return "PUnknown"
	}
}
