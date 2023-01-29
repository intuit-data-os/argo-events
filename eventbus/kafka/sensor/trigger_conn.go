package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/argoproj/argo-events/eventbus/common"
	"github.com/argoproj/argo-events/eventbus/kafka/base"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type KafkaTriggerConnection struct {
	*base.KafkaConnection
	sensorName    string
	triggerName   string
	depExpression string
	dependencies  []common.Dependency
	register      func(context.Context, string, *KafkaTriggerHandler)
}

func (c *KafkaTriggerConnection) String() string {
	return fmt.Sprintf("KafkaTriggerConnection{Sensor:%s,Trigger:%s}", c.sensorName, c.triggerName)
}

// todo: implement
func (c *KafkaTriggerConnection) Close() error {
	return nil
}

// todo: implement
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
	handler := &KafkaTriggerHandler{
		sensorName:    c.sensorName,
		triggerName:   c.triggerName,
		depExpression: c.depExpression,
		dependencies:  c.dependencies,
		transform:     transform,
		filter:        filter,
		action:        action,
	}

	// register
	c.register(ctx, *topic, handler)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-closeCh:
			return nil
		case <-resetConditionsCh:
			// todo: make resilient (bump offset)
			handler.reset()
		}
	}
}

type EventWithPartitionAndOffset struct {
	*cloudevents.Event
	partition int32
	offset    int64
}

// Handler
type KafkaTriggerHandler struct {
	// trigger information
	sensorName    string
	triggerName   string
	depExpression string
	dependencies  []common.Dependency

	// trigger functions
	transform func(string, cloudevents.Event) (*cloudevents.Event, error)
	filter    func(string, cloudevents.Event) bool
	action    func(map[string]cloudevents.Event)

	// state
	events []*EventWithPartitionAndOffset
}

func (h *KafkaTriggerHandler) Update(event *EventWithPartitionAndOffset) (*cloudevents.Event, error) {
	var action cloudevents.Event
	var found bool

	for i := 0; i < len(h.events); i++ {
		if h.events[i].Source() == event.Source() && h.events[i].Subject() == event.Subject() {
			h.events[i] = event
			found = true
			break
		}
	}

	if !found {
		h.events = append(h.events, event)
	}

	if h.satisfied() {
		action = cloudevents.NewEvent()
		action.SetID(event.ID()) // use id of last event
		action.SetSource(h.sensorName)
		action.SetSubject(h.triggerName)

		if err := action.SetData(cloudevents.ApplicationJSON, h.events); err != nil {
			return nil, err
		}

		h.reset()
	}

	return &action, nil
}

func (h *KafkaTriggerHandler) Offset(partition int32, offset int64) int64 {
	for _, event := range h.events {
		if partition == event.partition && offset > event.offset {
			offset = event.offset
		}
	}

	return offset
}

func (h *KafkaTriggerHandler) Transform(depName string, event cloudevents.Event) (*cloudevents.Event, error) {
	return h.transform(depName, event)
}

func (h *KafkaTriggerHandler) Filter(depName string, event cloudevents.Event) bool {
	return h.filter(depName, event)
}

func (h *KafkaTriggerHandler) Action(event cloudevents.Event) error {
	var events []*cloudevents.Event
	if err := json.Unmarshal(event.Data(), &events); err != nil {
		return err
	}

	eventMap := map[string]cloudevents.Event{}
	for _, event := range events {
		for _, dependency := range h.dependencies {
			if dependency.EventSourceName == event.Source() && dependency.EventName == event.Subject() {
				eventMap[dependency.Name] = *event
			}
		}
	}

	// todo: implement at least once / at most once

	h.action(eventMap)

	return nil
}

// todo: implement dep expression
func (h *KafkaTriggerHandler) satisfied() bool {
	return len(h.events) == len(h.dependencies)
}

func (h *KafkaTriggerHandler) reset() {
	h.events = []*EventWithPartitionAndOffset{}
}
