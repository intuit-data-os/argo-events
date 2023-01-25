package installer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/argoproj/argo-events/controllers"
	"github.com/argoproj/argo-events/pkg/apis/eventbus/v1alpha1"
	"github.com/ghodss/yaml"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type exoticKafkaInstaller struct {
	eventBus *v1alpha1.EventBus
	config   *controllers.GlobalConfig
	logger   *zap.SugaredLogger
}

func NewExoticKafkaInstaller(eventBus *v1alpha1.EventBus, config *controllers.GlobalConfig, logger *zap.SugaredLogger) Installer {
	return &exoticKafkaInstaller{
		eventBus: eventBus,
		config:   config,
		logger:   logger.With("eventbus", eventBus.Name),
	}
}

func (r *exoticKafkaInstaller) Install(ctx context.Context) (*v1alpha1.BusConfig, error) {
	kafkaObj := r.eventBus.Spec.Kafka
	if kafkaObj == nil || kafkaObj.Exotic == nil {
		return nil, errors.New("invalid kafka eventbus spec")
	}

	// merge
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewBufferString(r.config.EventBus.Kafka.StreamConfig)); err != nil {
		return nil, fmt.Errorf("invalid kafka config in global configuration, %w", err)
	}
	if x := kafkaObj.Exotic.StreamConfig; x != "" {
		if err := v.MergeConfig(bytes.NewBufferString(x)); err != nil {
			return nil, fmt.Errorf("failed to merge customized config, %w", err)
		}
	}
	b, err := yaml.Marshal(v.AllSettings())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal merged buffer config, %w", err)
	}

	r.eventBus.Status.MarkDeployed("Skipped", "Skip deployment because of using exotic config.")
	r.logger.Info("using exotic config")
	busConfig := &v1alpha1.BusConfig{
		Kafka: &v1alpha1.KafkaConfig{
			URL:           kafkaObj.Exotic.URL,
			TLS:           kafkaObj.Exotic.TLS,
			SASL:          kafkaObj.Exotic.SASL,
			ConsumerGroup: kafkaObj.Exotic.ConsumerGroup,
			StreamConfig:  string(b),
		},
	}
	return busConfig, nil
}

func (r *exoticKafkaInstaller) Uninstall(ctx context.Context) error {
	r.logger.Info("nothing to uninstall")
	return nil
}
