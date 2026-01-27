package handlers

import (
	"context"
	"ingestion-api/db"
	"ingestion-api/kafka"
	"ingestion-api/models"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type FailureHandler struct {
	KafkaProducer *kafka.Producer
}

// PHASE 3: Ingestion
func (h *FailureHandler) CreateFailedEvent(c *gin.Context) {
	var event models.FailedEvent

	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	// Use a timeout context for DB and Kafka operations
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Save to Postgres
	query := `INSERT INTO failed_events (event_id, topic, partition_id, offset_id, consumer_name, exception_type, error_message, status, original_payload) 
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	_, err := db.DB.Exec(ctx, query,
		event.EventID, event.Topic, event.PartitionID, event.OffsetID,
		event.ConsumerName, event.ExceptionType, event.ErrorMessage, event.Status, event.OriginalPayload)

	if err != nil {
		log.Printf("❌ DB Insert Error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save to Database"})
		return
	}

	// 2. Publish to Kafka (Triggers the Executor/Brain)
	err = h.KafkaProducer.Publish(ctx, event.EventID, event)
	if err != nil {
		log.Printf("⚠️ Kafka Publish Failed for %s: %v", event.EventID, err)
		c.JSON(http.StatusCreated, gin.H{"message": "Saved to DB, but Kafka failed", "id": event.EventID})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"message": "Logged and Published", "id": event.EventID})
}

// PHASE 3: MCP Action Tool
func (h *FailureHandler) MCPDecision(c *gin.Context) {
	var req struct {
		EventID  string `json:"event_id"`
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid decision format"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Record the decision (Audit)
	_, _ = db.DB.Exec(ctx,
		"INSERT INTO decision_audit (event_id, decision, reason) VALUES ($1, $2, $3)",
		req.EventID, req.Decision, req.Reason)

	// 2. Update status
	result, err := db.DB.Exec(ctx,
		"UPDATE failed_events SET status = $1, last_updated_at = NOW() WHERE event_id = $2",
		req.Decision, req.EventID)

	if err != nil {
		log.Printf("❌ Update Error: %v", err)
		c.JSON(500, gin.H{"error": "Database update failed"})
		return
	}

	if result.RowsAffected() == 0 {
		c.JSON(404, gin.H{"error": "Event ID not found"})
		return
	}

	// 3. Trigger Kafka Action (PerformAction in handlers/action.go)
	go h.PerformAction(req.EventID, req.Decision)

	c.JSON(http.StatusOK, gin.H{
		"status": "Decision recorded and action triggered",
		"action": req.Decision,
	})
}

// PHASE 4: MCP Read Tool
func (h *FailureHandler) GetFailureContext(c *gin.Context) {
	eventID := c.Param("eventId")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. Fetch main event
	var event models.FailedEvent
	err := db.DB.QueryRow(ctx, `SELECT event_id, topic, exception_type, error_message, status FROM failed_events WHERE event_id=$1`, eventID).Scan(
		&event.EventID, &event.Topic, &event.ExceptionType, &event.ErrorMessage, &event.Status)

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Event not found"})
		return
	}

	// 2. Fetch history
	rows, err := db.DB.Query(ctx, "SELECT retry_attempt FROM retry_history WHERE event_id = $1 ORDER BY retry_attempt DESC", eventID)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"event": event, "history": []int{}, "retry_count": 0})
		return
	}
	defer rows.Close()

	var attempts []int
	for rows.Next() {
		var attempt int
		if err := rows.Scan(&attempt); err == nil {
			attempts = append(attempts, attempt)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"event":       event,
		"history":     attempts,
		"retry_count": len(attempts),
	})
}
