package kafka

import (
	"encoding/json"
	"sync"

	"github.com/Shopify/sarama"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
)

type KafkaHandler struct {
	*sync.Mutex
	Logger   *zap.SugaredLogger
	GroupId  string
	Producer sarama.AsyncProducer
	Handlers map[string]func(*sarama.ConsumerMessage, *cloudevents.Event) (*Transaction, error)
}

func (h *KafkaHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *KafkaHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *KafkaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			h.Logger.Infow("Received message",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset))

			var event *cloudevents.Event
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				h.Logger.Errorw("Cannot unmarshal cloudevent", zap.Error(err))
				continue
			}

			if handler, ok := h.Handlers[msg.Topic]; ok {
				transaction, err := handler(msg, event)
				if err != nil {
					h.Logger.Errorw("Failed to process message, skipping", zap.Error(err))
					continue
				}

				transaction.Logger = h.Logger

				h.Logger.Infow("Begin transaction",
					zap.Int("messages", len(transaction.Messages)),
					zap.String("topic", msg.Topic),
					zap.Int32("partition", msg.Partition),
					zap.Int64("offset", transaction.Offset))

				if err := h.withLock(func() error {
					return transaction.Commit(h.Producer, h.GroupId, msg, session)
				}); err != nil {
					h.Logger.Errorw("Transaction error", zap.Error(err))
				}

				h.Logger.Infow("End transaction",
					zap.String("topic", msg.Topic),
					zap.Int32("partition", msg.Partition),
					zap.Int64("offset", transaction.Offset))
			} else {
				h.Logger.Warnw("Unsupported topic", zap.String("topic", msg.Topic))
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *KafkaHandler) withLock(f func() error) error {
	h.Lock()
	defer h.Unlock()

	return f()
}
