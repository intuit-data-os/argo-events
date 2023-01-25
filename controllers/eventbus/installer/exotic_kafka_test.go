package installer

import (
	"context"
	"github.com/argoproj/argo-events/pkg/apis/eventbus/v1alpha1"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"testing"
)

var (
	testKafkaEventBus = &v1alpha1.EventBus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "EventBus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testName,
		},
		Spec: v1alpha1.EventBusSpec{
			Kafka: &v1alpha1.KafkaBus{
				Exotic: &v1alpha1.KafkaConfig{
					URL: "abcd.abcd",
				},
			},
		},
	}
)

func TestKafkaBadInstallation(t *testing.T) {
	t.Run("bad installation", func(t *testing.T) {
		badEventBus := testKafkaEventBus.DeepCopy()
		badEventBus.Spec.Kafka = nil
		installer := &exoticKafkaInstaller{
			eventBus: badEventBus,
			config:   fakeConfig,
			logger:   zaptest.NewLogger(t).Sugar(),
		}
		_, err := installer.Install(context.TODO())
		assert.Error(t, err)
	})

	t.Run("bad installation", func(t *testing.T) {
		badEventBus := testKafkaEventBus.DeepCopy()
		badEventBus.Spec.Kafka.Exotic = nil
		installer := &exoticKafkaInstaller{
			eventBus: badEventBus,
			config:   fakeConfig,
			logger:   zaptest.NewLogger(t).Sugar(),
		}
		_, err := installer.Install(context.TODO())
		assert.Error(t, err)
	})
}

func TestKafkaBadInstallationViper(t *testing.T) {
	t.Run("bad installation with Viper error", func(t *testing.T) {
		badEventBus := testKafkaEventBus.DeepCopy()
		badEventBus.Spec.Kafka.Exotic.StreamConfig = `replication: 3\nrequiredAcks: -1`
		installer := &exoticKafkaInstaller{
			eventBus: badEventBus,
			config:   fakeConfig,
			logger:   zaptest.NewLogger(t).Sugar(),
		}
		_, err := installer.Install(context.TODO())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "While parsing config: yaml: mapping values are not allowed in this context")
	})
}

func TestKafkaInstallation(t *testing.T) {
	t.Run("Kafka not Exotic", func(t *testing.T) {
		badEventBus := testKafkaEventBus.DeepCopy()
		badEventBus.Spec.Kafka.Exotic = nil
		installer := &exoticKafkaInstaller{
			eventBus: badEventBus,
			config:   fakeConfig,
			logger:   zaptest.NewLogger(t).Sugar(),
		}
		busConfig, err := installer.Install(context.TODO())
		assert.NotNil(t, err)
		assert.Nil(t, busConfig)
	})

	t.Run("exotic kafka with config", func(t *testing.T) {
		goodEventBus := testKafkaEventBus.DeepCopy()
		goodEventBus.Spec.Kafka.Exotic.StreamConfig = `replication: 3
requiredAcks: -1
configVersion: 2.5.0
`
		installer := &exoticKafkaInstaller{
			eventBus: goodEventBus,
			config:   fakeConfig,
			logger:   zaptest.NewLogger(t).Sugar(),
		}

		expectedStreamConfig := `configVersion: 2.5.0
maxRetry: 3
replication: 3
requiredAcks: -1
`
		busConfig, err := installer.Install(context.TODO())
		assert.Nil(t, err)
		assert.Equal(t, strings.ToLower(expectedStreamConfig), strings.ToLower(busConfig.Kafka.StreamConfig))
	})
}
