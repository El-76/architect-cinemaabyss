package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/shopspring/decimal"
)

const (
	timeLayout = "2006-01-02T15:04:05.000Z"

	kafkaConsumerGroupID = "events-service"

	movieEventsTopic   = "movie-events"
	userEventsTopic    = "user-events"
	paymentEventsTopic = "payment-events"
)

// Models
type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id"`
	Rating      float64  `json:"rating"`
	Genres      []string `json:"genres"`
	Description string   `json:"description"`
}

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int             `json:"payment_id"`
	UserID     int             `json:"user_id"`
	Amount     decimal.Decimal `json:"amount"`
	Status     string          `json:"status"`
	Timestamp  string          `json:"timestamp"`
	MethodType string          `json:"method_type"`
}

type Event[T MovieEvent | UserEvent | PaymentEvent] struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	Payload   T      `json:"payload"`
}

type EventResponse[T MovieEvent | UserEvent | PaymentEvent] struct {
	Status    string   `json:"status"`
	Partition int32    `json:"user_id"`
	Offset    int64    `json:"user_id"`
	Event     Event[T] `json:"event"`
}

type server struct {
	kafkaProducer sarama.SyncProducer
	kafkaConsumer sarama.ConsumerGroup
}

func main() {
	ctx := context.Background()

	// Init Kafka

	producer := initKafkaProducer()
	defer producer.Close()

	consumer := initKafkaConsumer(kafkaConsumerGroupID)
	defer consumer.Close()

	s := &server{
		kafkaProducer: producer,
		kafkaConsumer: consumer,
	}

	s.startConsuming(ctx, []string{movieEventsTopic, userEventsTopic, paymentEventsTopic})

	// Set up HTTP routes
	http.HandleFunc("/api/events/movie", s.handleMovie)
	http.HandleFunc("/api/events/user", s.handleUser)
	http.HandleFunc("/api/events/payment", s.handlePayment)
	http.HandleFunc("/api/events/health", s.handleHealth)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082" // Note: Using a different port than the monolith
	}

	log.Printf("Starting events microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func makeResponse[T MovieEvent | UserEvent | PaymentEvent](
	event T, id string, type_ string, timestamp string, partition int32, offset int64) EventResponse[T] {

	e := Event[T]{
		ID:        id,
		Type:      type_,
		Timestamp: timestamp,
		Payload:   event,
	}

	er := EventResponse[T]{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     e,
	}

	return er
}

type consumer struct {
}

func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Println("Message channel closed")
				return nil
			}

			log.Printf("Consumed message: value = %s, timestamp = %v, topic = %s, partition = %d, offset = %d\n",
				string(message.Value), message.Timestamp, message.Topic, message.Partition, message.Offset)

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (s *server) startConsuming(ctx context.Context, topics []string) {
	handler := &consumer{}

	go func() {
		for {
			if err := s.kafkaConsumer.Consume(ctx, topics, handler); err != nil {
				log.Fatalf("Error from consumer loop: %v", err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Sarama consumer group is active.")
}

func (s *server) handleMovie(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		var m MovieEvent

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatalf("Failed to read response body: %v", err)
		}

		err = json.Unmarshal(body, &m)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		movieID := fmt.Sprintf("%d", m.MovieID)

		message := &sarama.ProducerMessage{
			Topic: movieEventsTopic,
			Key:   sarama.StringEncoder(movieID),
			Value: sarama.ByteEncoder(body),
		}

		partition, offset, err := s.kafkaProducer.SendMessage(message)

		if err == nil {
			log.Printf("Published movie event: %#v\n", m)
		} else {
			log.Fatalf("Failed to send message: %v", err)
		}

		er := makeResponse(m, movieID, "movie", time.Now().Format(timeLayout), partition, offset)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(er)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// User handler
func (s *server) handleUser(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		var u UserEvent

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatalf("Failed to read response body: %v", err)
		}

		err = json.Unmarshal(body, &u)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		_, err = time.Parse(timeLayout, u.Timestamp)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		userID := fmt.Sprintf("%d", u.UserID)

		message := &sarama.ProducerMessage{
			Topic: userEventsTopic,
			Key:   sarama.StringEncoder(userID),
			Value: sarama.ByteEncoder(body),
		}

		partition, offset, err := s.kafkaProducer.SendMessage(message)

		if err == nil {
			log.Printf("Published user event: %#v\n", u)
		} else {
			log.Fatalf("Failed to send message: %v", err)
		}

		er := makeResponse(u, userID, "user", time.Now().Format(timeLayout), partition, offset)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(er)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Payment handler
func (s *server) handlePayment(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		var p PaymentEvent

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatalf("Failed to read response body: %v", err)
		}

		err = json.Unmarshal(body, &p)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		_, err = time.Parse(timeLayout, p.Timestamp)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		paymentID := fmt.Sprintf("%d", p.PaymentID)

		message := &sarama.ProducerMessage{
			Topic: paymentEventsTopic,
			Key:   sarama.StringEncoder(paymentID),
			Value: sarama.ByteEncoder(body),
		}

		partition, offset, err := s.kafkaProducer.SendMessage(message)

		if err == nil {
			log.Printf("Published payment event: %#v\n", p)
		} else {
			log.Fatalf("Failed to send message: %v", err)
		}

		er := makeResponse(p, paymentID, "payment", time.Now().Format(timeLayout), partition, offset)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(er)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
