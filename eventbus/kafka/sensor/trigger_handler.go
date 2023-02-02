package kafka

import (
	"encoding/json"

	"github.com/argoproj/argo-events/eventbus/common"
	"github.com/argoproj/argo-events/eventbus/kafka/base"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
)

type KafkaTriggerHandler interface {
	common.TriggerConnection
	Name() string
	Ready() bool
	DependsOn(*cloudevents.Event) (string, bool)
	Transform(string, *cloudevents.Event) (*cloudevents.Event, error)
	Filter(string, *cloudevents.Event) bool
	Update(event *cloudevents.Event, partition int32, offset int64) (*cloudevents.Event, error)
	Offset(int32, int64) int64
	Action(cloudevents.Event) (func(), error)
}

func (c *KafkaTriggerConnection) Name() string {
	return c.triggerName
}

func (c *KafkaTriggerConnection) Ready() bool {
	return c.transform != nil && c.filter != nil && c.action != nil
}

func (c *KafkaTriggerConnection) DependsOn(event *cloudevents.Event) (string, bool) {
	if dep, ok := c.dependencies[base.EventKey(event.Source(), event.Subject())]; ok {
		return dep.Name, true
	}

	return "", false
}

func (c *KafkaTriggerConnection) Transform(depName string, event *cloudevents.Event) (*cloudevents.Event, error) {
	return c.transform(depName, *event)
}

func (c *KafkaTriggerConnection) Filter(depName string, event *cloudevents.Event) bool {
	return c.filter(depName, *event)
}

func (c *KafkaTriggerConnection) Update(event *cloudevents.Event, partition int32, offset int64) (*cloudevents.Event, error) {
	found := false
	eventWithPartitionAndOffset := &eventWithPartitionAndOffset{
		Event:     event,
		partition: partition,
		offset:    offset,
	}

	for i := 0; i < len(c.events); i++ {
		if c.events[i].Source() == event.Source() && c.events[i].Subject() == event.Subject() {
			c.events[i] = eventWithPartitionAndOffset
			found = true
			break
		}
	}

	if !found {
		c.events = append(c.events, eventWithPartitionAndOffset)
	}

	satisfied, err := c.satisfied()
	if err != nil {
		return nil, err
	}

	if satisfied == true {
		action := cloudevents.NewEvent()
		action.SetID(event.ID()) // use id of last event
		action.SetSource(c.sensorName)
		action.SetSubject(c.triggerName)

		if err := action.SetData(cloudevents.ApplicationJSON, c.events); err != nil {
			return nil, err
		}

		c.reset()
		return &action, nil
	}

	return nil, nil
}

func (c *KafkaTriggerConnection) Offset(partition int32, offset int64) int64 {
	for _, event := range c.events {
		if partition == event.partition && offset > event.offset {
			offset = event.offset
		}
	}

	return offset
}

func (c *KafkaTriggerConnection) Action(event cloudevents.Event) (func(), error) {
	var events []*cloudevents.Event
	if err := json.Unmarshal(event.Data(), &events); err != nil {
		return nil, err
	}

	eventMap := map[string]cloudevents.Event{}
	for _, event := range events {
		if depName, ok := c.DependsOn(event); ok {
			eventMap[depName] = *event
		}
	}

	// If at least once is specified, we must call action before the
	// kafka transaction, otherwise action must be called after the
	// transaction. To invoke the action after the transaction, we
	// return a function
	var f func()
	if c.atLeastOnce {
		c.action(eventMap)
	} else {
		f = func() { c.action(eventMap) }
	}

	return f, nil
}

func (c *KafkaTriggerConnection) satisfied() (interface{}, error) {
	parameters := Parameters{}
	for _, event := range c.events {
		if depName, ok := c.DependsOn(event.Event); ok {
			parameters[depName] = true
		}
	}

	c.Logger.Infow("Evaluating", zap.String("expr", c.depExpression.String()), zap.Any("parameters", parameters))

	return c.depExpression.Eval(parameters)
}

func (c *KafkaTriggerConnection) reset() {
	c.events = []*eventWithPartitionAndOffset{}
}

type Parameters map[string]bool

func (p Parameters) Get(name string) (interface{}, error) {
	return p[name], nil
}
