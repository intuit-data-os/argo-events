package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"go.uber.org/zap"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"github.com/argoproj/argo-events/common"
	"github.com/argoproj/argo-events/common/logging"
	"github.com/argoproj/argo-events/metrics"
	eventbusv1alpha1 "github.com/argoproj/argo-events/pkg/apis/eventbus/v1alpha1"
	v1alpha1 "github.com/argoproj/argo-events/pkg/apis/sensor/v1alpha1"
	"github.com/argoproj/argo-events/sensors"
	"github.com/fsnotify/fsnotify"
)

func Start() {
	logger := logging.NewArgoEventsLogger().Named("sensor")

	busConfig := &eventbusv1alpha1.BusConfig{}
	encodedBusConfigSpec := os.Getenv(common.EnvVarEventBusConfig)
	if len(encodedBusConfigSpec) > 0 {
		busConfigSpec, err := base64.StdEncoding.DecodeString(encodedBusConfigSpec)
		if err != nil {
			logger.Fatalw("failed to decode bus config string", zap.Error(err))
		}
		if err = json.Unmarshal(busConfigSpec, busConfig); err != nil {
			logger.Fatalw("failed to unmarshal bus config object", zap.Error(err))
		}
	}
	ebSubject, defined := os.LookupEnv(common.EnvVarEventBusSubject)
	if !defined {
		logger.Fatalf("required environment variable '%s' not defined", common.EnvVarEventBusSubject)
	}

	hostname, defined := os.LookupEnv("POD_NAME")
	if !defined {
		logger.Fatal("required environment variable 'POD_NAME' not defined")
	}

	kubeConfig, _ := os.LookupEnv(common.EnvVarKubeConfig)
	restConfig, err := common.GetClientConfig(kubeConfig)
	if err != nil {
		logger.Fatalw("failed to get kubeconfig", zap.Error(err))
	}
	dynamicClient := dynamic.NewForConfigOrDie(restConfig)
	kubeClient := kubernetes.NewForConfigOrDie(restConfig)

	ctx := logging.WithLogger(signals.SetupSignalHandler(), logger)

	manager := NewSensorContextManager(logger, kubeClient, dynamicClient, busConfig, ebSubject, hostname)
	if err := manager.Start(ctx); err != nil {
		logger.Fatalw("failed to start sensor context manager", zap.Error(err))
	}

	for {
		select {
		case sensorContext := <-manager.C:
			if err := sensorContext.Start(manager.Ctx); err != nil {
				logger.Fatalw("failed to listen to events", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

type SensorContextManager struct {
	C   chan *sensors.SensorContext
	Ctx context.Context

	logger          *zap.SugaredLogger
	kubeClient      kubernetes.Interface
	dynamicClient   dynamic.Interface
	eventBusConfig  *eventbusv1alpha1.BusConfig
	eventBusSubject string
	hostname        string
}

func NewSensorContextManager(logger *zap.SugaredLogger, kubeClient kubernetes.Interface, dynamicClient dynamic.Interface, eventBusConfig *eventbusv1alpha1.BusConfig, eventBusSubject, hostname string) *SensorContextManager {
	return &SensorContextManager{
		C:               make(chan *sensors.SensorContext),
		logger:          logger,
		kubeClient:      kubeClient,
		dynamicClient:   dynamicClient,
		eventBusConfig:  eventBusConfig,
		eventBusSubject: eventBusSubject,
		hostname:        hostname,
	}
}

func (scm *SensorContextManager) Start(ctx context.Context) error {
	sensorPath, defined := os.LookupEnv("SENSOR_PATH")

	// fallback to env variable
	if !defined {
		sensor, err := scm.readSensorFromEnv()
		if err != nil {
			scm.logger.Fatalw("failed to read sensor from env variable", zap.Error(err))
		}

		m := metrics.NewMetrics(sensor.Namespace)
		go m.Run(ctx, fmt.Sprintf(":%d", common.SensorMetricsPort))

		sensorCtx := sensors.NewSensorContext(scm.kubeClient, scm.dynamicClient, sensor, scm.eventBusConfig, scm.eventBusSubject, scm.hostname, m)
		scm.Ctx = ctx
		scm.C <- sensorCtx

		return nil
	}

	// watch for file changes
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	err = watcher.Add(sensorPath)
	if err != nil {
		return err
	}

	var m *metrics.Metrics

	go func() {
		defer watcher.Close()

		for {
			sensor, err := scm.readSensorFromFile(sensorPath)
			if err != nil {
				scm.logger.Fatalw("failed to read sensor from file", zap.Error(err))
			}

			// logger
			scm.logger = scm.logger.With("sensorName", sensor.Name)
			for name, value := range sensor.Spec.LoggingFields {
				scm.logger.With(name, value)
			}

			// create context
			ctx, cancel := context.WithCancel(ctx)
			scm.Ctx = ctx

			// start metrics once
			if m == nil {
				m = metrics.NewMetrics(sensor.Namespace)
				go m.Run(ctx, fmt.Sprintf(":%d", common.SensorMetricsPort))
			}

			sensorCtx := sensors.NewSensorContext(scm.kubeClient, scm.dynamicClient, sensor, scm.eventBusConfig, scm.eventBusSubject, scm.hostname, m)
			scm.C <- sensorCtx

			for {
				event, ok := <-watcher.Events
				if !ok {
					cancel()
					return
				}

				if event.Op == fsnotify.Remove {
					cancel()
					_ = watcher.Remove(sensorPath)
					if err := watcher.Add(sensorPath); err != nil {
						scm.logger.Fatalw("failed to add sensor watch", zap.Error(err))
					}

					break
				}
			}
		}
	}()

	return nil
}

func (scm *SensorContextManager) readSensorFromEnv() (*v1alpha1.Sensor, error) {
	encodedSensorSpec, defined := os.LookupEnv(common.EnvVarSensorObject)
	if !defined {
		return nil, fmt.Errorf("required environment variable '%s' not defined", common.EnvVarSensorObject)
	}
	sensorSpec, err := base64.StdEncoding.DecodeString(encodedSensorSpec)
	if err != nil {
		return nil, err
	}
	sensor := &v1alpha1.Sensor{}
	if err = json.Unmarshal(sensorSpec, sensor); err != nil {
		return nil, err
	}

	scm.verifySensor(sensor)

	return sensor, nil
}

func (scm *SensorContextManager) readSensorFromFile(path string) (*v1alpha1.Sensor, error) {
	sensorSpec, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	sensor := &v1alpha1.Sensor{}
	if err = json.Unmarshal(sensorSpec, sensor); err != nil {
		return nil, err
	}

	scm.verifySensor(sensor)

	return sensor, nil
}

func (scm *SensorContextManager) verifySensor(sensor *v1alpha1.Sensor) {
	if scm.eventBusConfig.NATS != nil {
		for _, trigger := range sensor.Spec.Triggers {
			if trigger.AtLeastOnce {
				scm.logger.Warn("ignoring atLeastOnce when using NATS")
				trigger.AtLeastOnce = false
			}
		}
	}
}
