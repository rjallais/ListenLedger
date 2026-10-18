// Package correctionproposal models the correction proposal aggregate:
// an auditable proposal for catalog enrichment, correction, linking, or duplicate marking.
package correctionproposal

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"ListenLedger/internal/eventsourcing"
)

// StreamTypeCorrectionProposal identifies proposal event streams.
const StreamTypeCorrectionProposal = "correctionproposal"

const (
	EventTypeCorrectionProposed = "CorrectionProposed"
	EventTypeCorrectionApproved = "CorrectionApproved"
	EventTypeCorrectionRejected = "CorrectionRejected"
	EventTypeCorrectionApplied  = "CorrectionApplied"
)

const (
	StatusProposed = "proposed"
	StatusApproved = "approved"
	StatusRejected = "rejected"
	StatusApplied  = "applied"
)

// ProposedPayload captures a new change proposal.
type ProposedPayload struct {
	ProposalID   string         `json:"proposal_id"`
	RunID        string         `json:"run_id"`
	TargetType   string         `json:"target_type"`
	TargetID     string         `json:"target_id"`
	Kind         string         `json:"kind"`
	Before       map[string]any `json:"before"`
	After        map[string]any `json:"after"`
	Evidence     string         `json:"evidence"`
	Confidence   float64        `json:"confidence"`
	TidalTrackID string         `json:"tidal_track_id,omitempty"`
	ProposedAt   time.Time      `json:"proposed_at"`
}

// ApprovedPayload records proposal approval.
type ApprovedPayload struct {
	ProposalID string    `json:"proposal_id"`
	ApprovedAt time.Time `json:"approved_at"`
	Reason     string    `json:"reason,omitempty"`
}

// RejectedPayload records proposal rejection.
type RejectedPayload struct {
	ProposalID string    `json:"proposal_id"`
	RejectedAt time.Time `json:"rejected_at"`
	Reason     string    `json:"reason,omitempty"`
}

// AppliedPayload records execution of an approved proposal.
type AppliedPayload struct {
	ProposalID string    `json:"proposal_id"`
	AppliedAt  time.Time `json:"applied_at"`
}

// Proposal is the aggregate root for a correction proposal.
type Proposal struct {
	eventsourcing.BaseAggregate

	runID        string
	targetType   string
	targetID     string
	kind         string
	before       map[string]any
	after        map[string]any
	evidence     string
	confidence   float64
	tidalTrackID string
	status       string

	proposedAt time.Time
	approvedAt time.Time
	rejectedAt time.Time
	appliedAt  time.Time
}

// NewProposal creates a new correction proposal aggregate.
func NewProposal(id, runID, targetType, targetID, kind string, before, after map[string]any, evidence string, confidence float64, tidalTrackID string, proposedAt time.Time, opts ...eventsourcing.AggregateOption) (*Proposal, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("correctionproposal: id cannot be empty")
	}
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return nil, errors.New("correctionproposal: kind cannot be empty")
	}

	agg := &Proposal{
		BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeCorrectionProposal, opts...),
	}

	evt, err := agg.RecordThat(EventTypeCorrectionProposed, ProposedPayload{
		ProposalID:   id,
		RunID:        runID,
		TargetType:   targetType,
		TargetID:     targetID,
		Kind:         kind,
		Before:       before,
		After:        after,
		Evidence:     evidence,
		Confidence:   confidence,
		TidalTrackID: tidalTrackID,
		ProposedAt:   proposedAt,
	}, nil)
	if err != nil {
		return nil, err
	}
	if err := agg.apply(evt); err != nil {
		return nil, err
	}
	return agg, nil
}

// Approve marks the proposal as approved.
func (p *Proposal) Approve(approvedAt time.Time, reason string) error {
	if p.status != StatusProposed {
		return fmt.Errorf("correctionproposal: cannot approve from status %s", p.status)
	}
	evt, err := p.RecordThat(EventTypeCorrectionApproved, ApprovedPayload{
		ProposalID: p.AggregateID(),
		ApprovedAt: approvedAt,
		Reason:     reason,
	}, nil)
	if err != nil {
		return err
	}
	return p.apply(evt)
}

// Reject marks the proposal as rejected.
func (p *Proposal) Reject(rejectedAt time.Time, reason string) error {
	if p.status != StatusProposed {
		return fmt.Errorf("correctionproposal: cannot reject from status %s", p.status)
	}
	evt, err := p.RecordThat(EventTypeCorrectionRejected, RejectedPayload{
		ProposalID: p.AggregateID(),
		RejectedAt: rejectedAt,
		Reason:     reason,
	}, nil)
	if err != nil {
		return err
	}
	return p.apply(evt)
}

// MarkApplied marks the proposal as applied to the target aggregate.
func (p *Proposal) MarkApplied(appliedAt time.Time) error {
	if p.status != StatusApproved {
		return fmt.Errorf("correctionproposal: cannot apply from status %s (must be approved first)", p.status)
	}
	evt, err := p.RecordThat(EventTypeCorrectionApplied, AppliedPayload{
		ProposalID: p.AggregateID(),
		AppliedAt:  appliedAt,
	}, nil)
	if err != nil {
		return err
	}
	return p.apply(evt)
}

// Getters
func (p *Proposal) RunID() string             { return p.runID }
func (p *Proposal) TargetType() string        { return p.targetType }
func (p *Proposal) TargetID() string          { return p.targetID }
func (p *Proposal) Kind() string              { return p.kind }
func (p *Proposal) Before() map[string]any    { return p.before }
func (p *Proposal) After() map[string]any     { return p.after }
func (p *Proposal) Evidence() string          { return p.evidence }
func (p *Proposal) Confidence() float64       { return p.confidence }
func (p *Proposal) TidalTrackID() string      { return p.tidalTrackID }
func (p *Proposal) Status() string            { return p.status }

// Apply implements eventsourcing.Aggregate.
func (p *Proposal) Apply(evt eventsourcing.Event) error { return p.apply(evt) }

func (p *Proposal) apply(evt eventsourcing.Event) error {
	p.SetVersion(evt.Version)
	switch evt.EventType {
	case EventTypeCorrectionProposed:
		var pl ProposedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		p.runID = pl.RunID
		p.targetType = pl.TargetType
		p.targetID = pl.TargetID
		p.kind = pl.Kind
		p.before = pl.Before
		p.after = pl.After
		p.evidence = pl.Evidence
		p.confidence = pl.Confidence
		p.tidalTrackID = pl.TidalTrackID
		p.proposedAt = pl.ProposedAt
		p.status = StatusProposed
	case EventTypeCorrectionApproved:
		var pl ApprovedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		p.approvedAt = pl.ApprovedAt
		p.status = StatusApproved
	case EventTypeCorrectionRejected:
		var pl RejectedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		p.rejectedAt = pl.RejectedAt
		p.status = StatusRejected
	case EventTypeCorrectionApplied:
		var pl AppliedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		p.appliedAt = pl.AppliedAt
		p.status = StatusApplied
	default:
		return fmt.Errorf("correctionproposal: unknown event type %q", evt.EventType)
	}
	return nil
}

// Replay reconstructs a Proposal aggregate from its events.
func Replay(id string, events []eventsourcing.Event) (*Proposal, error) {
	agg := &Proposal{BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeCorrectionProposal)}
	for _, evt := range events {
		if err := agg.Apply(evt); err != nil {
			return nil, fmt.Errorf("replaying correctionproposal event %s v%d: %w", evt.EventType, evt.Version, err)
		}
	}
	return agg, nil
}
