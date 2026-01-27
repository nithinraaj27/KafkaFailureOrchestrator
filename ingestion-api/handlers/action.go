package handlers

import (
	"context"
	"ingestion-api/db"
	"log"
	"time"
)

func (h *FailureHandler) PerformAction(eventID string, decision string) {
	switch decision {
	case "RETRY":
		h.handleRetryAction(eventID)
	case "DLQ":
		h.handleDLQAction(eventID)
	default:
		log.Printf("⚠️ No automated action defined for decision: %s", decision)
	}
}

func (h *FailureHandler) handleRetryAction(eventID string) {
	log.Printf("♻️ Handling RETRY action for EventID: %s", eventID)

	// Using a 5-second timeout for DB/Kafka operations
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Get current retry count
	var currentAttempts int
	err := db.DB.QueryRow(ctx,
		"SELECT COUNT(*) FROM retry_history WHERE event_id = $1",
		eventID).Scan(&currentAttempts)

	if err != nil {
		log.Printf("⚠️ Warning: Could not fetch retry history for %s, assuming 0: %v", eventID, err)
		currentAttempts = 0
	}

	nextAttempt := currentAttempts + 1

	// 2. Insert into retry_history
	_, err = db.DB.Exec(ctx,
		"INSERT INTO retry_history (event_id, retry_attempt, decision_source) VALUES ($1, $2, $3)",
		eventID, nextAttempt, "MCP_BRAIN")
	if err != nil {
		log.Printf("❌ CRITICAL: Failed to log retry_history: %v", err)
	}

	// 3. Publish to Kafka (This triggers the Healer)
	err = h.KafkaProducer.PublishRetry(eventID)
	if err != nil {
		log.Printf("❌ Failed to publish retry for %s: %v", eventID, err)
		db.DB.Exec(ctx, "UPDATE failed_events SET status = 'RETRY_FAILED' WHERE event_id = $1", eventID)
		return
	}

	// 4. Update main table status
	_, err = db.DB.Exec(ctx,
		"UPDATE failed_events SET status = 'RETRY_EXECUTED', last_updated_at = NOW() WHERE event_id = $1",
		eventID)

	log.Printf("✅ Successfully processed RETRY and logged history for %s", eventID)
}

func (h *FailureHandler) handleDLQAction(eventID string) {
	log.Printf("🗑️ Handling DLQ action for EventID: %s", eventID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := h.KafkaProducer.PublishToDLQ(eventID)
	if err != nil {
		log.Printf("❌ Failed to publish to DLQ for EventID %s: %v", eventID, err)
		db.DB.Exec(ctx, "UPDATE failed_events SET status = 'DLQ_FAILED' WHERE event_id = $1", eventID)
		return
	}

	// Mark as QUARANTINED
	db.DB.Exec(ctx, "UPDATE failed_events SET status = 'QUARANTINED', last_updated_at = NOW() WHERE event_id = $1", eventID)
	log.Printf("✅ Event %s successfully moved to DLQ (Quarantined)", eventID)
}
