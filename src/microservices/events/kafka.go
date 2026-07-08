package main

import (
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

func getBrokers() []string {
	brokers := os.Getenv("KAFKA_BROKERS")

	if brokers == "" {
		brokers = "kafka:9092"
	}

	return strings.Split(brokers, ",")
}

func initKafkaProducer() sarama.SyncProducer {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(getBrokers(), config)

	if err != nil {
		log.Fatal(err)
	}

	return producer
}

func initKafkaConsumer(groupID string) sarama.ConsumerGroup {
	config := sarama.NewConfig()

	config.Version = sarama.DefaultVersion
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}

	group, err := sarama.NewConsumerGroup(getBrokers(), groupID, config)

	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}

	return group
}
