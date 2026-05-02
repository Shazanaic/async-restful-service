package consumer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func New(brokerAddress string, topic string, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	return &Consumer{reader: r}
}

func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
