package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"ingestion-api/db"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	Writer *kafka.Writer
}

func NewProducer(brokers []string, defaultTopic string) *Producer {
	return &Producer{
		Writer: &kafka.Writer{
			Addr: kafka.TCP(brokers...),
			// Topic: defaultTopic, <--- REMOVE OR COMMENT OUT THIS LINE
			Async:                  false,
			AllowAutoTopicCreation: true,
			Balancer:               &kafka.LeastBytes{},
			WriteTimeout:           10 * time.Second,
			ReadTimeout:            10 * time.Second,
			RequiredAcks:           kafka.RequireAll,
			MaxAttempts:            5,
		},
	}
}

// Publish sends data to the ingestion topic (failed-events-topic)
func (p *Producer) Publish(ctx context.Context, key string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	return p.Writer.WriteMessages(ctx, kafka.Message{
		Topic: "failed-events-topic", // <--- ADD THIS LINE
		Key:   []byte(key),
		Value: payload,
	})
}

func (p *Producer) PublishRetry(eventID string) error {
	var payload string
	// Use a context with timeout for the DB query
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := db.DB.QueryRow(ctx,
		"SELECT original_payload FROM failed_events WHERE event_id = $1",
		eventID).Scan(&payload)

	if err != nil {
		return fmt.Errorf("could not find original payload for %s: %w", eventID, err)
	}

	return p.PublishToTopic("retry-events-topic", eventID, payload)
}

func (p *Producer) PublishToDLQ(eventID string) error {
	var payload string
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := db.DB.QueryRow(ctx,
		"SELECT original_payload FROM failed_events WHERE event_id = $1",
		eventID).Scan(&payload)

	if err != nil {
		return fmt.Errorf("could not find original payload for %s: %w", eventID, err)
	}

	log.Printf("📡 Attempting Kafka Write to DLQ for %s...", eventID)
	return p.PublishToTopic("failed-events-dlq", eventID, payload)
}

func (p *Producer) PublishToTopic(topic, key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	}

	log.Printf("📤 Kafka Publishing to %s | Key: %s", topic, key)

	err := p.Writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("kafka write error to topic %s: %w", topic, err)
	}

	return nil
}
