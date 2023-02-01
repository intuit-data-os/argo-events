package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
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
	hostname string

	// kafka config
	topics        *Topics
	consumerGroup *eventbusv1alpha1.KafkaConsumerGroup

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

	var consumerGroup *eventbusv1alpha1.KafkaConsumerGroup
	switch kafkaConfig.ConsumerGroup {
	case nil:
		consumerGroup = &eventbusv1alpha1.KafkaConsumerGroup{}
	default:
		consumerGroup = kafkaConfig.ConsumerGroup
	}

	if consumerGroup.GroupName == "" {
		consumerGroup.GroupName = fmt.Sprintf("%s.%s", sensor.Namespace, sensor.Name)
	}

	return &KafkaSensor{
		Kafka:         base.NewKafka(strings.Split(kafkaConfig.URL, ","), logger),
		Mutex:         &sync.Mutex{},
		sensor:        sensor,
		topics:        topics,
		consumerGroup: consumerGroup,
		hostname:      hostname,
		triggers:      map[string]KafkaTriggerHandler{},
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

	switch s.consumerGroup.StartOldest {
	case true:
		s.config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case false:
		s.config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	switch s.consumerGroup.RebalanceStrategy {
	case "sticky":
		s.config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	case "roundrobin":
		s.config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	default:
		s.config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	}

	// producer config for exactly once
	s.config.Producer.Idempotent = true
	s.config.Producer.RequiredAcks = sarama.WaitForAll
	s.config.Producer.Transaction.ID = s.hostname
	s.config.Net.MaxOpenRequests = 1

	return nil
}

func (s *KafkaSensor) Connect(ctx context.Context, triggerName string, depExpression string, dependencies []common.Dependency, atLeastOnce bool) (common.TriggerConnection, error) {
	s.Lock()
	defer s.Unlock()

	if s.IsClosed() {
		client, err := sarama.NewClient(s.Brokers, s.config)
		if err != nil {
			return nil, err
		}

		consumer, err := sarama.NewConsumerGroupFromClient(s.consumerGroup.GroupName, client)
		if err != nil {
			return nil, err
		}

		producer, err := sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			return nil, err
		}

		// offsetManager, err := sarama.NewOffsetManagerFromClient(s.consumerGroup.GroupName, client)
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
		expr, err := govaluate.NewEvaluableExpression(strings.ReplaceAll(depExpression, "-", "\\-"))
		if err != nil {
			return nil, err
		}

		depMap := map[string]common.Dependency{}
		for _, dep := range dependencies {
			depMap[base.EventKey(dep.EventSourceName, dep.EventName)] = dep
		}

		s.triggers[triggerName] = &KafkaTriggerConnection{
			KafkaConnection: base.NewKafkaConnection(s.Logger),
			sensorName:      s.sensor.Name,
			triggerName:     triggerName,
			depExpression:   expr,
			dependencies:    depMap,
			atLeastOnce:     atLeastOnce,
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
			Mutex:     &sync.Mutex{},
			Logger:    s.Logger,
			GroupName: s.consumerGroup.GroupName,
			Producer:  s.producer,
			Handlers: map[string]func(*sarama.ConsumerMessage) *Transaction{
				s.topics.event:   s.Event,
				s.topics.trigger: s.Trigger,
				s.topics.action:  s.Action,
			},
		}

		s.Logger.Infow("Consuming", zap.Strings("topics", s.topics.List()))

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

func (s *KafkaSensor) Event(msg *sarama.ConsumerMessage) *Transaction {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		s.Logger.Errorw("Failed to deserialize cloudevent, skipping", zap.Error(err))
		return &Transaction{Offset: msg.Offset + 1}
	}

	messages := []*sarama.ProducerMessage{}
	for _, trigger := range s.listTriggers(event) {
		event, err := trigger.Transform(trigger.depName, *event)
		if err != nil {
			s.Logger.Errorw("Failed to transform cloudevent, skipping", zap.Error(err))
			continue
		}

		if trigger.Filter(trigger.depName, *event) {
			value, err := json.Marshal(event)
			if err != nil {
				s.Logger.Errorw("Failed to serialize cloudevent, skipping", zap.Error(err))
				continue
			}

			messages = append(messages, &sarama.ProducerMessage{
				Topic: s.topics.trigger,
				Key:   sarama.StringEncoder(trigger.Name()),
				Value: sarama.ByteEncoder(value),
			})
		}
	}

	return &Transaction{Messages: messages, Offset: msg.Offset + 1}
}

func (s *KafkaSensor) Trigger(msg *sarama.ConsumerMessage) *Transaction {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		s.Logger.Errorw("Failed to deserialize cloudevent, skipping", zap.Error(err))
	}

	messages := []*sarama.ProducerMessage{}
	offset := msg.Offset + 1

	// Update trigger with new event and add resulting action to
	// transaction messages.
	if trigger, ok := s.triggers[string(msg.Key)]; ok && event != nil {
		func() {
			action, err := trigger.Update(event, msg.Partition, msg.Offset)
			if err != nil {
				s.Logger.Errorw("Failed to update trigger, skipping", zap.Error(err))
				return
			}

			// no action, trigger not yet satisfied
			if action == nil {
				return
			}

			value, err := json.Marshal(action)
			if err != nil {
				s.Logger.Errorw("Failed to serialize cloudevent, skipping", zap.Error(err))
				return
			}

			messages = append(messages, &sarama.ProducerMessage{
				Topic: s.topics.action,
				Key:   sarama.StringEncoder(trigger.Name()),
				Value: sarama.ByteEncoder(value),
			})
		}()
	}

	// Need to determine smallest possible offset against all
	// triggers as other triggers may have messages that land on the
	// same partition.
	for _, trigger := range s.triggers {
		offset = trigger.Offset(msg.Partition, offset)
	}

	return &Transaction{Messages: messages, Offset: offset}
}

func (s *KafkaSensor) Action(msg *sarama.ConsumerMessage) *Transaction {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		s.Logger.Errorw("Failed to deserialize cloudevent, skipping", zap.Error(err))
		return &Transaction{Offset: msg.Offset + 1}
	}

	var after func()
	if trigger, ok := s.triggers[string(msg.Key)]; ok {
		f, err := trigger.Action(*event)
		if err != nil {
			s.Logger.Errorw("Failed to trigger action, skipping", zap.Error(err))
		} else {
			after = f
		}
	}

	return &Transaction{Offset: msg.Offset + 1, After: after}
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
