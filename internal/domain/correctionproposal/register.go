package correctionproposal

import "ListenLedger/internal/eventsourcing"

// init registers the correction proposal event payloads. The registry is the
// census of the event contract: cmd/audit uses it to flag stored events whose
// payload this binary cannot decode.
func init() {
	eventsourcing.RegisterPayload[ProposedPayload](EventTypeCorrectionProposed)
	eventsourcing.RegisterPayload[ApprovedPayload](EventTypeCorrectionApproved)
	eventsourcing.RegisterPayload[RejectedPayload](EventTypeCorrectionRejected)
	eventsourcing.RegisterPayload[AppliedPayload](EventTypeCorrectionApplied)
}
