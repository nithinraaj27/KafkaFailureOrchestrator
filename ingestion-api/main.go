package main

import (
	"ingestion-api/db"       // Your database package
	"ingestion-api/executor" // Your executor package
	"ingestion-api/handlers"
	"ingestion-api/kafka"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env if it exists, ignore if it doesn't (standard Docker practice)
	_ = godotenv.Load()

	// 1. Start Postgres (Ensure db.Connect uses os.Getenv("DB_URL"))
	if err := db.Connect(); err != nil {
		log.Fatal("❌ Database connection failed: ", err)
	}
	defer db.DB.Close()

	// 2. Initialize Kafka Producer
	// Inside Docker, this env should be "kafka:29092"
	kafkaHost := os.Getenv("KAFKA_BROKERS")
	if kafkaHost == "" {
		kafkaHost = "localhost:9092"
	}
	brokers := []string{kafkaHost}

	producer := kafka.NewProducer(brokers, "failed-events-topic")
	defer producer.Writer.Close()

	// 3. Background Services
	go executor.Start()
	go executor.StartHealer(brokers, "retry-events-topic")

	// 4. Initialize Handler
	h := &handlers.FailureHandler{
		KafkaProducer: producer,
	}

	// 5. Setup Router
	r := gin.Default()

	// Routes...
	r.POST("/failures", h.CreateFailedEvent)
	r.GET("/tools/failures/:eventId", h.GetFailureContext)
	r.POST("/tools/decisions", h.MCPDecision)

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ready", "mcp_enabled": true})
	})

	// 6. Start Server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("🚀 Ingestion API live on :%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("❌ Failed to start server: %v", err)
	}
}
