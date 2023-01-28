package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/argoproj/argo-events/eventbus/common"
	"github.com/argoproj/argo-events/eventbus/kafka/base"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
)

type KafkaTriggerConnection struct {
	*base.KafkaConnection
	driver        *KafkaSensor
	sensorName    string
	triggerName   string
	depExpression string
	dependencies  []common.Dependency
	events        []EventData

	transform func(string, cloudevents.Event) (*cloudevents.Event, error)
	filter    func(string, cloudevents.Event) bool
	action    func(map[string]cloudevents.Event)
}

type EventData struct {
	partition int32
	offset    int64
	event     *cloudevents.Event
}

func (c *KafkaTriggerConnection) String() string {
	return fmt.Sprintf("KafkaTriggerConnection{Sensor:%s,Trigger:%s}", c.sensorName, c.triggerName)
}

func (c *KafkaTriggerConnection) Close() error {
	return nil
}

func (c *KafkaTriggerConnection) IsClosed() bool {
	return false
}

func (c *KafkaTriggerConnection) Subscribe(
	ctx context.Context,
	closeCh <-chan struct{},
	resetConditionsCh <-chan struct{},
	lastResetTime time.Time,
	transform func(depName string, event cloudevents.Event) (*cloudevents.Event, error),
	filter func(string, cloudevents.Event) bool,
	action func(map[string]cloudevents.Event),
	topic *string) error {
	// register
	c.driver.Register(ctx, *topic, c)

	// todo: do this differently
	c.transform = transform
	c.filter = filter
	c.action = action

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-closeCh:
			return nil
		case <-resetConditionsCh:
			c.Reset()
		}
	}
}

func (c *KafkaTriggerConnection) Update(msg *sarama.ConsumerMessage) error {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	found := false
	eventData := EventData{
		msg.Partition,
		msg.Offset,
		event,
	}

	for i := 0; i < len(c.events); i++ {
		if c.events[i].event.Source() == event.Source() && c.events[i].event.Subject() == event.Subject() {
			c.events[i] = eventData
			found = true
			break
		}
	}

	if !found {
		c.events = append(c.events, eventData)
	}

	return nil
}

func (c *KafkaTriggerConnection) Satisfied() bool {
	return len(c.events) == len(c.dependencies)
}

func (c *KafkaTriggerConnection) Offset(partition int32, offset int64) int64 {
	for _, event := range c.events {
		if partition == event.partition && offset > event.offset {
			offset = event.offset
		}
	}

	return offset
}

func (c *KafkaTriggerConnection) Action() ([]byte, error) {
	if !c.Satisfied() {
		return nil, nil
	}

	id := ""
	events := []*cloudevents.Event{}
	for _, eventData := range c.events {
		events = append(events, eventData.event)
		id = eventData.event.ID()
	}

	action := cloudevents.NewEvent()
	action.SetID(id)
	action.SetSource(c.sensorName)
	action.SetSubject(c.triggerName)
	err := action.SetData(cloudevents.ApplicationJSON, events)
	if err != nil {
		return nil, err
	}

	return json.Marshal(action)
}

func (c *KafkaTriggerConnection) Execute(msg *sarama.ConsumerMessage) error {
	var action *cloudevents.Event
	var events []*cloudevents.Event

	if err := json.Unmarshal(msg.Value, &action); err != nil {
		return err
	}

	if err := json.Unmarshal(action.Data(), &events); err != nil {
		return err
	}

	eventMap := map[string]cloudevents.Event{}
	for _, event := range events {
		for _, dependency := range c.dependencies {
			if dependency.EventSourceName == event.Source() && dependency.EventName == event.Subject() {
				eventMap[dependency.Name] = *event
			}
		}
	}

	// todo: implement at least once / at most once

	c.action(eventMap)

	return nil
}

func (c *KafkaTriggerConnection) TransformAndFilter(depName string, event *cloudevents.Event) ([]byte, error) {
	event, err := c.transform(depName, *event)
	if err != nil {
		return nil, err
	}

	if !c.filter(depName, *event) {
		c.Logger.Debugw("Filtered out message", zap.String("dependency", depName))
		return nil, nil
	}

	return json.Marshal(event)
}

func (c *KafkaTriggerConnection) Reset() {
	c.events = []EventData{}
}
