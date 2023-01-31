package kafka

import (
	"encoding/json"
	"strings"

	"github.com/Knetic/govaluate"
	"github.com/argoproj/argo-events/eventbus/common"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
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

func (c *KafkaTriggerConnection) Transform(depName string, event cloudevents.Event) (*cloudevents.Event, error) {
	return c.transform(depName, event)
}

func (c *KafkaTriggerConnection) Filter(depName string, event cloudevents.Event) bool {
	return c.filter(depName, event)
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

func (c *KafkaTriggerConnection) Action(event cloudevents.Event) error {
	var events []*cloudevents.Event
	if err := json.Unmarshal(event.Data(), &events); err != nil {
		return err
	}

	eventMap := map[string]cloudevents.Event{}
	for _, event := range events {
		if depName, ok := c.DependsOn(event); ok {
			eventMap[depName] = *event
		}
	}

	// todo: implement at least once / at most once

	c.action(eventMap)

	return nil
}

func (c *KafkaTriggerConnection) satisfied() (interface{}, error) {
	expr, err := govaluate.NewEvaluableExpression(strings.ReplaceAll(c.depExpression, "-", "\\-"))
	if err != nil {
		return false, err
	}

	parameters := Parameters{}
	for _, event := range c.events {
		// todo: make more efficient
		if depName, ok := c.DependsOn(event.Event); ok {
			parameters[depName] = true
		}
	}

	c.Logger.Infow("Evaluating", zap.String("expr", c.depExpression), zap.Any("parameters", parameters))

	return expr.Eval(parameters)
}

func (c *KafkaTriggerConnection) reset() {
	c.events = []*eventWithPartitionAndOffset{}
}

type Parameters map[string]bool

func (p Parameters) Get(name string) (interface{}, error) {
	if parameter, ok := p[name]; ok {
		return parameter, nil
	}

	return false, nil
}
