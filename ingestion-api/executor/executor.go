package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"ingestion-api/models"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func Start() {
	// 1. Load env - ignore error if file missing (Docker provides env directly)
	_ = godotenv.Load()

	// 2. Setup Kafka Reader using Environment Variables
	broker := os.Getenv("KAFKA_BROKERS")
	if broker == "" {
		broker = "localhost:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "failed-events-topic",
		GroupID: "mcp-execution-engine",
	})

	log.Printf("🤖 Execution Engine is watching %s on broker: %s", "failed-events-topic", broker)

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Reader Error: %v", err)
			continue
		}

		var event models.FailedEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("failed to unmarshal event: %v", err)
			continue
		}

		log.Printf("📢 New Failure Detected: %s. Notifying MCP...", event.EventID)

		// 3. TRIGGER MCP
		TriggerMCPDecisionFlow(event)
	}
}

func TriggerMCPDecisionFlow(event models.FailedEvent) {
	log.Printf("🧠 MCP is now analyzing %s (Error: %s)...", event.EventID, event.ExceptionType)

	// 1. Prepare the payload for the MCP Server
	payload := map[string]interface{}{
		"event_id": event.EventID,
	}
	jsonData, _ := json.Marshal(payload)

	// 2. Define the URL of your Python MCP Tool
	// Use BRAIN_URL from environment (e.g., http://mcp-brain:8000)
	brainURL := os.Getenv("BRAIN_URL")
	if brainURL == "" {
		brainURL = "http://localhost:8000"
	}
	url := fmt.Sprintf("%s/tools/handle_failure_event", brainURL)

	// 3. Send the POST request
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ Failed to reach MCP Brain at %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
		log.Printf("✅ MCP Brain successfully received %s and is processing...", event.EventID)
	} else {
		log.Printf("⚠️ MCP Brain returned status: %d", resp.StatusCode)
	}
}
