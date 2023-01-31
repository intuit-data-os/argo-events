package kafka

import (
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type Transaction struct {
	Logger   *zap.SugaredLogger
	Messages []*sarama.ProducerMessage
	Offset   int64
	Metadata string
}

func (t *Transaction) Commit(producer sarama.AsyncProducer, groupId string, msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
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

	if err := producer.AddOffsetsToTxn(offsets, groupId); err != nil {
		t.Logger.Errorw("Kafka transaction error", zap.Error(err))
		t.handleTxnError(producer, msg, session, func() error {
			return producer.AddOffsetsToTxn(offsets, groupId)
		})
		return nil // why?
	}

	if err := producer.CommitTxn(); err != nil {
		t.Logger.Errorw("Kafka transaction error", zap.Error(err))
		t.handleTxnError(producer, msg, session, func() error {
			return producer.CommitTxn()
		})
		return nil // why?
	}

	return nil
}

// todo: go over this carefully
func (t *Transaction) handleTxnError(producer sarama.AsyncProducer, msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, defaulthandler func() error) {
	for {
		if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			t.Logger.Info("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "")
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			if err := producer.AbortTxn(); err != nil {
				t.Logger.Errorw("Message consumer: unable to abort transaction", zap.Error(err))
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
