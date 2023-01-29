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
	consumer sarama.ConsumerGroup
	producer sarama.AsyncProducer
	handlers *Handlers
}

type Handlers struct {
	byName  map[string]*KafkaTriggerHandler
	byEvent map[string][]*HandlerWithDepName
}

type HandlerWithDepName struct {
	*KafkaTriggerHandler
	depName string
}

func NewHandlers() *Handlers {
	return &Handlers{
		map[string]*KafkaTriggerHandler{},
		map[string][]*HandlerWithDepName{},
	}
}

func (h *Handlers) Register(handler *KafkaTriggerHandler) {
	h.byName[handler.triggerName] = handler

	// todo: make idemotent
	for _, d := range handler.dependencies {
		key := h.eventKey(d.EventSourceName, d.EventName)
		h.byEvent[key] = append(h.byEvent[key], &HandlerWithDepName{handler, d.Name})
	}
}

func (h *Handlers) GetHandlerByName(name string) *KafkaTriggerHandler {
	return h.byName[name]
}

func (h *Handlers) GetHandlersByEvent(event *cloudevents.Event) []*HandlerWithDepName {
	return h.byEvent[h.eventKey(event.Source(), event.Subject())]
}

func (h *Handlers) Size() int {
	return len(h.byName)
}

func (h *Handlers) eventKey(source string, subject string) string {
	return fmt.Sprintf("%s/%s", source, subject)
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
		Kafka:    base.NewKafka(brokers, logger),
		Mutex:    &sync.Mutex{},
		Once:     &sync.Once{},
		sensor:   sensor,
		hostname: hostname,
		handlers: NewHandlers(),
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
		KafkaConnection: base.NewKafkaConnection(s.Logger),
		sensorName:      s.sensor.Name,
		triggerName:     triggerName,
		depExpression:   depExpression,
		dependencies:    dependencies,
		register:        s.Register,
	}

	return conn, nil
}

func (s *KafkaSensor) Register(ctx context.Context, topic string, handler *KafkaTriggerHandler) {
	s.Lock()
	defer s.Unlock()

	s.handlers.Register(handler)

	// connect once all triggers have registered
	if s.handlers.Size() == len(s.sensor.Spec.Triggers) {
		go s.Do(s.Subscribe(ctx, topic))
	}
}

// todo: ensure correct behaviour when failure occurs
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
				s.Logger.Errorw("Failed to consume", zap.Error(err))
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
			func() {
				s.Logger.Infow("Received message", zap.String("topic", msg.Topic), zap.Int32("partition", msg.Partition), zap.Int64("offset", msg.Offset))

				var event *cloudevents.Event
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					s.Logger.Errorw("Cannot unmarshal cloudevent", zap.Error(err))
					return
				}

				var transaction *Transaction
				var err error

				switch msg.Topic {
				case s.topics.event:
					transaction, err = s.Event(msg, event)
				case s.topics.trigger:
					transaction, err = s.Trigger(msg, event)
				case s.topics.action:
					transaction, err = s.Action(msg, event)
				default:
					s.Logger.Warnw("Unsupported topic", zap.String("topic", msg.Topic))
					return
				}

				if err != nil {
					s.Logger.Errorw("Failed to process message", zap.Error(err))
					return
				}

				s.Logger.Infow("Begin transaction", zap.Int("messages", len(transaction.Messages)), zap.Int32("partition", msg.Partition), zap.Int64("offset", transaction.Offset))

				s.Lock() // lock for transaction
				defer s.Unlock()

				if err := transaction.Commit(s.producer, msg, session, s.sensor.Name, s.Logger); err != nil {
					s.Logger.Errorw("Kafka transaction error", zap.Error(err))
				}

				s.Logger.Info("Finished transaction")
			}()
		case <-session.Context().Done():
			return nil
		}
	}
}

func (s *KafkaSensor) Event(msg *sarama.ConsumerMessage, event *cloudevents.Event) (*Transaction, error) {
	messages := []*sarama.ProducerMessage{}

	for _, handler := range s.handlers.GetHandlersByEvent(event) {
		event, err := handler.Transform(handler.depName, *event)
		if err != nil {
			s.Logger.Errorw("Failed to transform message", zap.Error(err))
			continue
		}

		if handler.Filter(handler.depName, *event) {
			value, err := json.Marshal(event)
			if err != nil {
				return nil, err
			}

			messages = append(messages, &sarama.ProducerMessage{
				Topic: s.topics.trigger,
				Key:   sarama.StringEncoder(handler.triggerName),
				Value: sarama.ByteEncoder(value),
			})
		}
	}

	return &Transaction{messages, msg.Offset, ""}, nil
}

func (s *KafkaSensor) Trigger(msg *sarama.ConsumerMessage, event *cloudevents.Event) (*Transaction, error) {
	messages := []*sarama.ProducerMessage{}
	offset := msg.Offset + 1

	if handler := s.handlers.GetHandlerByName(string(msg.Key)); handler != nil {
		event, err := handler.Update(&EventWithPartitionAndOffset{event, msg.Partition, msg.Offset})
		if err != nil {
			return nil, err
		}

		value, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}

		offset = handler.Offset(msg.Partition, offset)
		messages = append(messages, &sarama.ProducerMessage{
			Topic: s.topics.action,
			Key:   sarama.StringEncoder(handler.triggerName),
			Value: sarama.ByteEncoder(value),
		})
	}

	return &Transaction{messages, offset, ""}, nil
}

func (s *KafkaSensor) Action(msg *sarama.ConsumerMessage, event *cloudevents.Event) (*Transaction, error) {
	if handler := s.handlers.GetHandlerByName(string(msg.Key)); handler != nil {
		if err := handler.Action(*event); err != nil {
			return nil, err
		}
	}

	return &Transaction{Offset: msg.Offset, Metadata: ""}, nil
}
