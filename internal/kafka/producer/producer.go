package producer

import (
	"context"
	"encoding/json"
	"log"

	"async-event-rest/internal/models"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func New(brokerAddress string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	return &Producer{writer: writer}
}

func (p *Producer) ProduceEvent(ctx context.Context, event models.Event) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Value: eventBytes,
	}

	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("Failed to produce msg to Kafka: %v", err)
		return err
	}
	log.Printf("Msg produced to Kafka: uID: %d, type: %s", event.UserID, event.Type)
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
