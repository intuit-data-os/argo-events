package kafka

import (
	"context"
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
