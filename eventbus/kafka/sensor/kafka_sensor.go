package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/argoproj/argo-events/eventbus/common"
	"github.com/argoproj/argo-events/eventbus/kafka/base"
	eventbusv1alpha1 "github.com/argoproj/argo-events/pkg/apis/eventbus/v1alpha1"
	sensorv1alpha1 "github.com/argoproj/argo-events/pkg/apis/sensor/v1alpha1"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
)

type KafkaSensor struct {
	*base.Kafka
	*sync.Mutex
	sensor   *sensorv1alpha1.Sensor
	topics   *Topics
	hostname string

	// kafka
	config   *sarama.Config
	client   sarama.Client
	consumer sarama.ConsumerGroup
	producer sarama.AsyncProducer
	// offsetManager sarama.OffsetManager

	// triggers
	triggers map[string]KafkaTriggerHandler
}

func NewKafkaSensor(kafkaConfig *eventbusv1alpha1.KafkaConfig, sensor *sensorv1alpha1.Sensor, hostname string, logger *zap.SugaredLogger) *KafkaSensor {
	topics := &Topics{
		event:   kafkaConfig.Topic,
		trigger: fmt.Sprintf("%s.%s.%s", kafkaConfig.Topic, sensor.Name, "trigger"),
		action:  fmt.Sprintf("%s.%s.%s", kafkaConfig.Topic, sensor.Name, "action"),
	}

	return &KafkaSensor{
		Kafka:    base.NewKafka(strings.Split(kafkaConfig.URL, ","), logger),
		Mutex:    &sync.Mutex{},
		sensor:   sensor,
		topics:   topics,
		hostname: hostname,
		triggers: map[string]KafkaTriggerHandler{},
	}
}

type Topics struct {
	event   string
	trigger string
	action  string
}

func (t *Topics) List() []string {
	return []string{t.event, t.trigger, t.action}
}

func (s *KafkaSensor) Initialize() error {
	s.config = sarama.NewConfig()

	// consumer config
	s.config.Consumer.IsolationLevel = sarama.ReadCommitted
	s.config.Consumer.Offsets.AutoCommit.Enable = false
	s.config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// producer config for exactly once
	s.config.Producer.Idempotent = true
	s.config.Producer.RequiredAcks = sarama.WaitForAll
	s.config.Producer.Transaction.ID = s.hostname
	s.config.Net.MaxOpenRequests = 1

	return nil
}

func (s *KafkaSensor) Connect(ctx context.Context, triggerName string, depExpression string, dependencies []common.Dependency) (common.TriggerConnection, error) {
	s.Lock()
	defer s.Unlock()

	if s.IsClosed() {
		client, err := sarama.NewClient(s.Brokers, s.config)
		if err != nil {
			return nil, err
		}

		consumer, err := sarama.NewConsumerGroupFromClient(s.sensor.Name, client)
		if err != nil {
			return nil, err
		}

		producer, err := sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			return nil, err
		}

		// offsetManager, err := sarama.NewOffsetManagerFromClient(s.sensor.Name, client)
		// if err != nil {
		// 	return err
		// }

		s.client = client
		s.consumer = consumer
		s.producer = producer
		// s.offsetManager = offsetManager

		go s.Subscribe(ctx)
	}

	if _, ok := s.triggers[triggerName]; !ok {
		s.triggers[triggerName] = &KafkaTriggerConnection{
			KafkaConnection: base.NewKafkaConnection(s.Logger),
			sensorName:      s.sensor.Name,
			triggerName:     triggerName,
			depExpression:   depExpression,
			dependencies:    dependencies,
			close:           s.Close,
			isClosed:        s.IsClosed,
		}
	}

	return s.triggers[triggerName], nil
}

func (s *KafkaSensor) Close() error {
	s.Lock()
	defer s.Unlock()

	// protect against being called multiple times
	if s.IsClosed() {
		return nil
	}

	if err := s.consumer.Close(); err != nil {
		return err
	}

	if err := s.producer.Close(); err != nil {
		return err
	}

	return s.client.Close()
}

func (s *KafkaSensor) IsClosed() bool {
	return s.client == nil || s.client.Closed()
}

func (s *KafkaSensor) Subscribe(ctx context.Context) {
	for {
		if !s.ready() {
			s.Logger.Info("Not ready to consume, waiting...")
			time.Sleep(3 * time.Second)
			continue
		}

		handler := &KafkaHandler{
			Mutex:    &sync.Mutex{},
			Logger:   s.Logger,
			GroupId:  s.sensor.Name,
			Producer: s.producer,
			Handlers: map[string]func(*sarama.ConsumerMessage, *cloudevents.Event) (*Transaction, error){
				s.topics.event:   s.Event,
				s.topics.trigger: s.Trigger,
				s.topics.action:  s.Action,
			},
		}

		if err := s.consumer.Consume(ctx, s.topics.List(), handler); err != nil {
			s.Logger.Errorw("Failed to consume", zap.Error(err))
			return
		}

		if err := ctx.Err(); err != nil {
			s.Logger.Errorw("Kafka error", zap.Error(err))
			return
		}
	}
}

func (s *KafkaSensor) Event(msg *sarama.ConsumerMessage, event *cloudevents.Event) (*Transaction, error) {
	messages := []*sarama.ProducerMessage{}

	for _, trigger := range s.listTriggers(event) {
		event, err := trigger.Transform(trigger.depName, *event)
		if err != nil {
			s.Logger.Errorw("Failed to transform message", zap.Error(err))
			continue
		}

		if trigger.Filter(trigger.depName, *event) {
			value, err := json.Marshal(event)
			if err != nil {
				return nil, err
			}

			messages = append(messages, &sarama.ProducerMessage{
				Topic: s.topics.trigger,
				Key:   sarama.StringEncoder(trigger.Name()),
				Value: sarama.ByteEncoder(value),
			})
		}
	}

	return &Transaction{Messages: messages, Offset: msg.Offset + 1}, nil
}

func (s *KafkaSensor) Trigger(msg *sarama.ConsumerMessage, event *cloudevents.Event) (*Transaction, error) {
	messages := []*sarama.ProducerMessage{}
	offset := msg.Offset + 1

	if trigger, ok := s.triggers[string(msg.Key)]; ok {
		action, err := trigger.Update(event, msg.Partition, msg.Offset)
		if err != nil {
			return nil, err
		}

		if action != nil {
			value, err := json.Marshal(action)
			if err != nil {
				return nil, err
			}

			messages = append(messages, &sarama.ProducerMessage{
				Topic: s.topics.action,
				Key:   sarama.StringEncoder(trigger.Name()),
				Value: sarama.ByteEncoder(value),
			})
		}

		offset = trigger.Offset(msg.Partition, offset)
	}

	return &Transaction{Messages: messages, Offset: offset}, nil
}

func (s *KafkaSensor) Action(msg *sarama.ConsumerMessage, event *cloudevents.Event) (*Transaction, error) {
	if trigger, ok := s.triggers[string(msg.Key)]; ok {
		if err := trigger.Action(*event); err != nil {
			return nil, err
		}
	}

	return &Transaction{Offset: msg.Offset + 1}, nil
}

func (s *KafkaSensor) ready() bool {
	if len(s.triggers) != len(s.sensor.Spec.Triggers) {
		return false
	}

	for _, trigger := range s.triggers {
		if !trigger.Ready() {
			return false
		}
	}

	return true
}

type triggerHandlerWithDepName struct {
	KafkaTriggerHandler
	depName string
}

func (s *KafkaSensor) listTriggers(event *cloudevents.Event) []*triggerHandlerWithDepName {
	triggers := []*triggerHandlerWithDepName{}

	for _, trigger := range s.triggers {
		if depName, ok := trigger.DependsOn(event); ok {
			triggers = append(triggers, &triggerHandlerWithDepName{trigger, depName})
		}
	}

	return triggers
}
