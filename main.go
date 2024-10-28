package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
)

// Redis configuration and Kafka setup
var (
	ctx            = context.Background()
	redisClient    = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	kafkaProducer  *kafka.Producer
	kafkaTopic     = "unique-request-count"
	requestLogFile *os.File
	mutex          = &sync.Mutex{}
)

func init() {
	// Initialize Kafka producer
	var err error
	kafkaProducer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	// Open the log file for writing
	requestLogFile, err = os.OpenFile("request_count.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
}

func main() {
	defer kafkaProducer.Close()
	defer requestLogFile.Close() // Ensure the log file is closed on exit

	http.HandleFunc("/api/verve/accept", RequestHandler)
	go LogUniqueRequests()

	fmt.Println("Starting server on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}

// RequestHandler handles the /api/verve/accept endpoint
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	// Parse 'id' parameter and validate it
	idParam := r.URL.Query().Get("id")
	if idParam == "" {
		http.Error(w, "failed", http.StatusBadRequest)
		return
	}

	id, err := parseID(idParam)
	if err != nil {
		http.Error(w, "failed", http.StatusBadRequest)
		return
	}

	// Track unique request in Redis with a 1 minute expiration
	isUnique, err := trackUniqueRequest(id)
	if err != nil {
		http.Error(w, "failed", http.StatusInternalServerError)
		return
	}

	if !isUnique {
		http.Error(w, "failed: Duplicate", http.StatusConflict)
		return
	}

	// If an endpoint is provided, make a POST request with unique count data
	endpoint := r.URL.Query().Get("endpoint")
	if endpoint != "" {
		if err := triggerEndpoint(endpoint); err != nil {
			http.Error(w, "failed", http.StatusInternalServerError)
			return
		}
	}

	_, _ = w.Write([]byte("ok"))
}

// trackUniqueRequest stores the ID in Redis and returns true if it was unique (not present in the last minute)
func trackUniqueRequest(id int) (bool, error) {
	key := fmt.Sprintf("request_id:%d", id)
	isUnique, err := redisClient.SetNX(ctx, key, true, time.Minute).Result()
	if err != nil {
		log.Printf("Error interacting with Redis: %v", err)
		return false, err
	}
	return isUnique, nil
}

// triggerEndpoint sends a POST request to the specified endpoint with a message payload
func triggerEndpoint(endpoint string) error {
	data := map[string]string{
		"message": "Unique request data",
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error encoding JSON data: %v", err)
		return err
	}

	resp, err := http.Post(endpoint, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		log.Printf("Error making request to endpoint: %v", err)
		return err
	}
	defer resp.Body.Close()

	log.Printf("Triggered endpoint: %s with status code: %d", endpoint, resp.StatusCode)
	return nil
}

// LogUniqueRequests sends the unique count of requests to Kafka and logs it to a file every minute
func LogUniqueRequests() {
	for range time.Tick(time.Minute) {
		mutex.Lock()
		keys, err := redisClient.Keys(ctx, "request_id:*").Result()
		if err != nil {
			log.Printf("Error fetching keys from Redis: %v", err)
			mutex.Unlock()
			continue
		}

		uniqueCount := len(keys)
		log.Printf("Sending unique request count to Kafka: %d", uniqueCount)

		// Prepare Kafka message
		message := kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(fmt.Sprintf("Unique request count in last minute: %d", uniqueCount)),
		}

		// Produce message to Kafka
		err = kafkaProducer.Produce(&message, nil)
		if err != nil {
			log.Printf("Error sending message to Kafka: %v", err)
		}

		// Log the unique count to the file
		_, err = fmt.Fprintf(requestLogFile, "Unique request count in last minute: %d\n", uniqueCount)
		if err != nil {
			log.Printf("Error writing to log file: %v", err)
		}

		// Cleanup Redis keys after logging
		for _, key := range keys {
			redisClient.Del(ctx, key)
		}
		mutex.Unlock()
	}
}

// Helper to parse integer ID
func parseID(id string) (int, error) {
	return strconv.Atoi(id)
}
