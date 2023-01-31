package kafka

import (
	"encoding/json"

	"github.com/argoproj/argo-events/eventbus/common"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type KafkaTriggerHandler interface {
	common.TriggerConnection
	Name() string
	Ready() bool
	DependsOn(*cloudevents.Event) (string, bool)
	Transform(string, cloudevents.Event) (*cloudevents.Event, error)
	Filter(string, cloudevents.Event) bool
	Update(event *cloudevents.Event, partition int32, offset int64) (*cloudevents.Event, error)
	Offset(int32, int64) int64
	Action(cloudevents.Event) error
}

func (c *KafkaTriggerConnection) Name() string {
	return c.triggerName
}

func (c *KafkaTriggerConnection) Ready() bool {
	return c.transform != nil && c.filter != nil && c.action != nil
}

func (c *KafkaTriggerConnection) DependsOn(event *cloudevents.Event) (string, bool) {
	for _, dep := range c.dependencies {
		if event.Source() == dep.EventSourceName && event.Subject() == dep.EventName {
			return dep.Name, true
		}
	}

	return "", false
}

func (h *KafkaTriggerConnection) Transform(depName string, event cloudevents.Event) (*cloudevents.Event, error) {
	return h.transform(depName, event)
}

func (h *KafkaTriggerConnection) Filter(depName string, event cloudevents.Event) bool {
	return h.filter(depName, event)
}

func (h *KafkaTriggerConnection) Update(event *cloudevents.Event, partition int32, offset int64) (*cloudevents.Event, error) {
	found := false
	eventWithPartitionAndOffset := &eventWithPartitionAndOffset{
		Event:     event,
		partition: partition,
		offset:    offset,
	}

	for i := 0; i < len(h.events); i++ {
		if h.events[i].Source() == event.Source() && h.events[i].Subject() == event.Subject() {
			h.events[i] = eventWithPartitionAndOffset
			found = true
			break
		}
	}

	if !found {
		h.events = append(h.events, eventWithPartitionAndOffset)
	}

	if h.satisfied() {
		action := cloudevents.NewEvent()
		action.SetID(event.ID()) // use id of last event
		action.SetSource(h.sensorName)
		action.SetSubject(h.triggerName)

		if err := action.SetData(cloudevents.ApplicationJSON, h.events); err != nil {
			return nil, err
		}

		h.reset()
		return &action, nil
	}

	return nil, nil
}

func (h *KafkaTriggerConnection) Offset(partition int32, offset int64) int64 {
	for _, event := range h.events {
		if partition == event.partition && offset > event.offset {
			offset = event.offset
		}
	}

	return offset
}

func (h *KafkaTriggerConnection) Action(event cloudevents.Event) error {
	var events []*cloudevents.Event
	if err := json.Unmarshal(event.Data(), &events); err != nil {
		return err
	}

	eventMap := map[string]cloudevents.Event{}
	for _, event := range events {
		if depName, ok := h.DependsOn(event); ok {
			eventMap[depName] = *event
		}
	}

	// todo: implement at least once / at most once

	h.action(eventMap)

	return nil
}

// todo: implement dep expression
func (h *KafkaTriggerConnection) satisfied() bool {
	return len(h.events) == len(h.dependencies)
}

func (h *KafkaTriggerConnection) reset() {
	h.events = []*eventWithPartitionAndOffset{}
}
