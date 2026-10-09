package templates

import "strconv"

// QueueStats holds the metrics displayed in the queue health widget.
type QueueStats struct {
	QueuePending      uint64
	QueueAckPending   uint64
	QueueRedelivered  uint64
	JobsQueued        int64
	JobsProcessing    int64
	JobsFailed        int64
	ArtistsPending    int64
	StreamAvailable   bool
	ConsumerAvailable bool
}

// FormatUint formats an unsigned integer as a string.
func FormatUint(n uint64) string {
	return strconv.FormatUint(n, 10)
}

// FormatInt64 formats a signed 64-bit integer as a string.
func FormatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}
