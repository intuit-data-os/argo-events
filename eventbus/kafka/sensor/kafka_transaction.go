package kafka

import (
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type Transaction struct {
	Messages []*sarama.ProducerMessage
	Offset   int64
	Metadata string
}

func (t *Transaction) Commit(producer sarama.AsyncProducer, groupName string, msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, logger *zap.SugaredLogger) error {
	// no need for a transaction if no messages
	if len(t.Messages) == 0 {
		session.MarkOffset(msg.Topic, msg.Partition, t.Offset, t.Metadata)
		session.Commit()
		return nil
	}

	logger.Infow("Begin transaction",
		zap.Int("messages", len(t.Messages)),
		zap.String("topic", msg.Topic),
		zap.Int32("partition", msg.Partition),
		zap.Int64("offset", t.Offset))

	if err := producer.BeginTxn(); err != nil {
		return err
	}

	for _, msg := range t.Messages {
		producer.Input() <- msg
	}

	offsets := map[string][]*sarama.PartitionOffsetMetadata{
		msg.Topic: {{
			Partition: msg.Partition,
			Offset:    t.Offset,
			Metadata:  &t.Metadata,
		}},
	}

	if err := producer.AddOffsetsToTxn(offsets, groupName); err != nil {
		logger.Errorw("Kafka transaction error", zap.Error(err))
		t.handleTxnError(producer, msg, session, logger, func() error {
			return producer.AddOffsetsToTxn(offsets, groupName)
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

	logger.Infow("End transaction",
		zap.String("topic", msg.Topic),
		zap.Int32("partition", msg.Partition),
		zap.Int64("offset", t.Offset))

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
