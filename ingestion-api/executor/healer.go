package executor

import (
	"context"
	"ingestion-api/db"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func StartHealer(brokers []string, topic string) {
	// 1. Setup the Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     "retry-healer-group", // Fixed typo in 'retry'
		StartOffset: kafka.FirstOffset,
		// Add these for better Docker stability
		MaxWait: 5 * time.Second,
	})

	defer reader.Close()

	log.Printf("🩺 Healer is watching for RETRY events on topic: %s", topic)

	for {
		// 2. Read Message with a background context
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("⚠️ Healer Reader Error: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue // Don't 'break', or the healer dies forever!
		}

		eventID := string(m.Key)
		log.Printf("🩺 Healer detected RETRY event for EventID: %s", eventID)

		// 3. Simulate "Healing" delay (Wait for the transient issue to clear)
		time.Sleep(2 * time.Second)

		// 4. Update Database with Timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		_, err = db.DB.Exec(ctx,
			"UPDATE failed_events SET status = 'RESOLVED', last_updated_at = NOW() WHERE event_id = $1",
			eventID)

		cancel() // Release context resources

		if err != nil {
			log.Printf("❌ Healer failed to update status for %s: %v", eventID, err)
			// Optional: you might want to NOT commit the offset here if DB fails
			continue
		}

		log.Printf("✅ Healer successfully marked EventID %s as RESOLVED", eventID)
	}
}
