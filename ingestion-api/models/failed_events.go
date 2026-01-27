package models

type FailedEvent struct {
	EventID         string `json:"event_id"`
	Topic           string `json:"topic"`
	PartitionID     int    `json:"partition_id"`
	OffsetID        int64  `json:"offset_id"`
	ConsumerName    string `json:"consumer_name"`
	ExceptionType   string `json:"exception_type"`
	ErrorMessage    string `json:"error_message"`
	Status          string `json:"status"`
	OriginalPayload string `json:"original_payload"`
}
