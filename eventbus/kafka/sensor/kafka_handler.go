package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type KafkaHandler struct {
	*sync.Mutex
	Logger    *zap.SugaredLogger
	GroupName string
	Producer  sarama.AsyncProducer
	Handlers  map[string]func(*sarama.ConsumerMessage) *Transaction
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

			if handler, ok := h.Handlers[msg.Topic]; ok {
				transaction := handler(msg)
				if err := h.commit(transaction, msg, session); err != nil {
					h.Logger.Errorw("Transaction error", zap.Error(err))
				}
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *KafkaHandler) commit(transaction *Transaction, msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	h.Lock()
	defer h.Unlock()

	return transaction.Commit(h.Producer, h.GroupName, msg, session, h.Logger)
}
