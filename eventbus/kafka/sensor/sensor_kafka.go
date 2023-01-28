package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/argoproj/argo-events/eventbus/common"
	"github.com/argoproj/argo-events/eventbus/kafka/base"
	sensorv1alpha1 "github.com/argoproj/argo-events/pkg/apis/sensor/v1alpha1"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
)

type KafkaSensor struct {
	*base.Kafka
	*sync.Mutex
	*sync.Once
	sensor   *sensorv1alpha1.Sensor
	topics   *Topics
	hostname string
	client   sarama.Client
	// offsetManager sarama.OffsetManager
	consumer     sarama.ConsumerGroup
	producer     sarama.AsyncProducer
	dependencies map[string][]common.Dependency
	handlers     map[string]*KafkaTriggerConnection
}

type Topics struct {
	event   string
	trigger string
	action  string
}

func (t *Topics) List() []string {
	return []string{t.event, t.trigger, t.action}
}

func NewKafkaSensor(brokers []string, sensor *sensorv1alpha1.Sensor, hostname string, logger *zap.SugaredLogger) *KafkaSensor {
	return &KafkaSensor{
		base.NewKafka(brokers, logger),
		&sync.Mutex{},
		&sync.Once{},
		sensor,
		&Topics{},
		hostname,
		nil,
		nil,
		nil,
		map[string][]common.Dependency{},
		map[string]*KafkaTriggerConnection{},
	}
}

func (s *KafkaSensor) Initialize() error {
	config := sarama.NewConfig()

	// consumer config
	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Offsets.AutoCommit.Enable = false
	// config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// producer config for exactly once
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	// config.Producer.Transaction.ID = s.sensor.Name
	config.Producer.Transaction.ID = s.hostname
	config.Net.MaxOpenRequests = 1

	client, err := sarama.NewClient(s.Brokers, config)
	if err != nil {
		return err
	}

	// offsetManager, err := sarama.NewOffsetManagerFromClient(s.sensor.Name, client)
	// if err != nil {
	// 	return err
	// }

	consumer, err := sarama.NewConsumerGroupFromClient(s.sensor.Name, client)
	if err != nil {
		return err
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return err
	}

	s.client = client
	// s.offsetManager = offsetManager
	s.consumer = consumer
	s.producer = producer

	return nil
}

func (s *KafkaSensor) Connect(triggerName string, depExpression string, dependencies []common.Dependency) (common.TriggerConnection, error) {
	conn := &KafkaTriggerConnection{
		base.NewKafkaConnection(s.Logger),
		s,
		s.sensor.Name,
		triggerName,
		depExpression,
		dependencies,
		[]EventData{},
		nil,
		nil,
		nil,
	}

	return conn, nil
}

func (s *KafkaSensor) Register(ctx context.Context, topic string, conn *KafkaTriggerConnection) {
	s.Lock()
	defer s.Unlock()

	s.dependencies[conn.triggerName] = conn.dependencies
	s.handlers[conn.triggerName] = conn

	// connect once all triggers have registered
	if len(s.handlers) == len(s.sensor.Spec.Triggers) {
		go s.Do(s.Subscribe(ctx, topic))
	}
}

func (s *KafkaSensor) Subscribe(ctx context.Context, eventTopic string) func() {
	return func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		s.topics = &Topics{
			eventTopic,
			fmt.Sprintf("%s-%s-trigger", eventTopic, s.sensor.Name),
			fmt.Sprintf("%s-%s-action", eventTopic, s.sensor.Name),
		}

		for {
			if err := s.consumer.Consume(ctx, s.topics.List(), s); err != nil {
				s.Logger.Errorw("Failed to consume from kafka", zap.Error(err))
				return
			}

			if err := ctx.Err(); err != nil {
				s.Logger.Errorw("Kafka error", zap.Error(err))
				return
			}
		}
	}
}

func (s *KafkaSensor) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (s *KafkaSensor) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (s *KafkaSensor) Close() error {
	if err := s.consumer.Close(); err != nil {
		return err
	}

	if err := s.producer.Close(); err != nil {
		return err
	}

	return s.client.Close()
}

func (s *KafkaSensor) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			s.Logger.Infow("Received message from kafka", zap.String("topic", msg.Topic), zap.Int32("partition", msg.Partition), zap.Int64("offset", msg.Offset))

			var err error

			switch msg.Topic {
			case s.topics.event:
				err = s.Event(msg, session)
			case s.topics.trigger:
				err = s.Trigger(msg, session)
			case s.topics.action:
				err = s.Action(msg, session)
			default:
				s.Logger.Warnw("Unsupported topic", zap.String("topic", msg.Topic))
			}

			if err != nil {
				s.Logger.Errorw("Handling error", zap.Error(err))
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (s *KafkaSensor) Event(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	var event *cloudevents.Event

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()

	if err := s.producer.BeginTxn(); err != nil {
		return err
	}

	if err := s.producer.AddMessageToTxn(msg, s.sensor.Name, nil); err != nil {
		s.Logger.Errorw("Kafka transaction error", zap.Error(err))
		s.handleTxnError(msg, session, func() error {
			return s.producer.AddMessageToTxn(msg, s.sensor.Name, nil)
		})
		return nil
	}

	// todo: clean this up (seriously)
	for _, trigger := range s.sensor.Spec.Triggers {
		for _, dependency := range s.dependencies[trigger.Template.Name] {
			if dependency.EventSourceName == event.Source() && dependency.EventName == event.Subject() {
				if handler, ok := s.handlers[trigger.Template.Name]; ok {
					value, err := handler.TransformAndFilter(dependency.Name, event)
					if err != nil {
						s.Logger.Errorw("Could not transform message", zap.Error(err))
						continue
					}

					if value != nil {
						s.producer.Input() <- &sarama.ProducerMessage{
							Topic: s.topics.trigger,
							Key:   sarama.StringEncoder(trigger.Template.Name),
							Value: sarama.ByteEncoder(value),
						}
					}
				}
			}
		}
	}

	if err := s.producer.CommitTxn(); err != nil {
		s.Logger.Errorw("Kafka transaction error", zap.Error(err))
		s.handleTxnError(msg, session, func() error {
			return s.producer.CommitTxn()
		})
	}

	return nil
}

func (s *KafkaSensor) Trigger(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	s.Lock()
	defer s.Unlock()

	if err := s.producer.BeginTxn(); err != nil {
		return err
	}

	triggerName := string(msg.Key)
	offset := msg.Offset + 1

	if handler, ok := s.handlers[triggerName]; ok {
		if err := handler.Update(msg); err != nil {
			return err
		}

		if handler.Satisfied() {
			value, err := handler.Action()
			if err != nil {
				return err
			}

			s.producer.Input() <- &sarama.ProducerMessage{
				Topic: s.topics.action,
				Key:   sarama.StringEncoder(triggerName),
				Value: sarama.ByteEncoder(value),
			}

			handler.Reset()
		}

		offset = handler.Offset(msg.Partition, offset)
	}

	offsets := map[string][]*sarama.PartitionOffsetMetadata{
		msg.Topic: {{
			Partition: msg.Partition,
			Offset:    offset,
			Metadata:  nil,
		}},
	}

	if err := s.producer.AddOffsetsToTxn(offsets, s.sensor.Name); err != nil {
		s.Logger.Errorw("Kafka transaction error", zap.Error(err))
		s.handleTxnError(msg, session, func() error {
			return s.producer.AddMessageToTxn(msg, s.sensor.Name, nil)
		})
		return nil
	}

	if err := s.producer.CommitTxn(); err != nil {
		s.Logger.Errorw("Kafka transaction error", zap.Error(err))
		s.handleTxnError(msg, session, func() error {
			return s.producer.CommitTxn()
		})
	}

	return nil
}

func (s *KafkaSensor) Action(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	if handler, ok := s.handlers[string(msg.Key)]; ok {
		if err := handler.Execute(msg); err != nil {
			return err
		}
	}

	session.MarkMessage(msg, "")
	session.Commit()

	return nil
}

func (s *KafkaSensor) handleTxnError(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, defaulthandler func() error) {
	for {
		if s.producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			s.Logger.Info("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "")
			return
		}
		if s.producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			if err := s.producer.AbortTxn(); err != nil {
				s.Logger.Errorw("Message consumer: unable to abort transaction", zap.Error(err))
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
