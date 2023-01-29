package kafka

import (
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type Transaction struct {
	Messages []*sarama.ProducerMessage
	Offsets  map[string][]*sarama.PartitionOffsetMetadata
}

func (t *Transaction) Commit(producer sarama.AsyncProducer, msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, groupId string, logger *zap.SugaredLogger) error {
	if err := producer.BeginTxn(); err != nil {
		return err
	}

	for _, msg := range t.Messages {
		producer.Input() <- msg
	}

	if err := producer.AddOffsetsToTxn(t.Offsets, groupId); err != nil {
		logger.Errorw("Kafka transaction error", zap.Error(err))
		t.handleTxnError(producer, msg, session, logger, func() error {
			return producer.AddOffsetsToTxn(t.Offsets, groupId)
		})
		return nil // why?
	}

	if err := producer.CommitTxn(); err != nil {
		logger.Errorw("Kafka transaction error", zap.Error(err))
		t.handleTxnError(producer, msg, session, logger, func() error {
			return producer.CommitTxn()
		})
		return nil // why?
	}

	return nil
}

// todo: go over this carefully
func (t *Transaction) handleTxnError(producer sarama.AsyncProducer, msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, logger *zap.SugaredLogger, defaulthandler func() error) {
	for {
		if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			logger.Info("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "")
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			if err := producer.AbortTxn(); err != nil {
				logger.Errorw("Message consumer: unable to abort transaction", zap.Error(err))
				continue
			}
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "")
			return
		}

		// if not you can retry
		if err := defaulthandler(); err == nil {
			return
		}
	}
}
