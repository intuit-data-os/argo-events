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

	encodedSensorSpec, defined := os.LookupEnv(common.EnvVarSensorObject)
	if !defined {
		logger.Fatalf("required environment variable '%s' not defined", common.EnvVarSensorObject)
	}
	sensorSpec, err := base64.StdEncoding.DecodeString(encodedSensorSpec)
	if err != nil {
		logger.Fatalw("failed to decode sensor string", zap.Error(err))
	}
	sensor := &v1alpha1.Sensor{}
	if err = json.Unmarshal(sensorSpec, sensor); err != nil {
		logger.Fatalw("failed to unmarshal sensor object", zap.Error(err))
	}

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
	if busConfig.NATS != nil {
		for _, trigger := range sensor.Spec.Triggers {
			if trigger.AtLeastOnce {
				logger.Warn("ignoring atLeastOnce when using NATS")
				trigger.AtLeastOnce = false
			}
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

	logger = logger.With("sensorName", sensor.Name)
	for name, value := range sensor.Spec.LoggingFields {
		logger.With(name, value)
	}

	ctx := logging.WithLogger(signals.SetupSignalHandler(), logger)
	// m := metrics.NewMetrics(sensor.Namespace)
	// go m.Run(ctx, fmt.Sprintf(":%d", common.SensorMetricsPort))

	manager := NewSensorContextManager(kubeClient, dynamicClient, busConfig, ebSubject, hostname)
	if err := manager.Start(ctx); err != nil {
		logger.Fatalw("failed to start sensor context manager", zap.Error(err))
	}

	// control loop
	for {
		select {
		case sensorContext := <-manager.C:
			fmt.Println("*****************")
			fmt.Println("Starting...")
			fmt.Println("*****************")
			if err := sensorContext.Start(manager.Ctx); err != nil {
				logger.Fatalw("failed to listen to events", zap.Error(err))
			}
		case <-ctx.Done():
			fmt.Println("Done son")
			return
		}
	}
}

type SensorContextManager struct {
	C   chan *sensors.SensorContext
	Ctx context.Context

	kubeClient      kubernetes.Interface
	dynamicClient   dynamic.Interface
	eventBusConfig  *eventbusv1alpha1.BusConfig
	eventBusSubject string
	hostname        string
}

func NewSensorContextManager(kubeClient kubernetes.Interface, dynamicClient dynamic.Interface, eventBusConfig *eventbusv1alpha1.BusConfig, eventBusSubject, hostname string) *SensorContextManager {
	return &SensorContextManager{
		C:               make(chan *sensors.SensorContext),
		kubeClient:      kubeClient,
		dynamicClient:   dynamicClient,
		eventBusConfig:  eventBusConfig,
		eventBusSubject: eventBusSubject,
		hostname:        hostname,
	}
}

func (scm *SensorContextManager) Start(ctx context.Context) error {
	sensorPath, defined := os.LookupEnv("SENSOR_PATH")
	if !defined {
		sensorPath = "sensor.json"
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

	go func() {
		defer watcher.Close()

		for {
			fmt.Println("*****************")
			fmt.Println("(re)loading...")
			fmt.Println("*****************")

			// read sensor
			sensorSpec, err := os.ReadFile(sensorPath)
			if err != nil {
				// TODO
				// logger.Fatalw("failed to read sensor", zap.Error(err))
			}

			sensor := &v1alpha1.Sensor{}
			if err = json.Unmarshal(sensorSpec, sensor); err != nil {
				// TODO
				// logger.Fatalw("failed to unmarshal sensor", zap.Error(err))
			}

			// create context
			ctx, cancel := context.WithCancel(ctx)
			scm.Ctx = ctx

			m := metrics.NewMetrics(sensor.Namespace)
			// go m.Run(ctx, fmt.Sprintf(":%d", common.SensorMetricsPort))

			sensorCtx := sensors.NewSensorContext(scm.kubeClient, scm.dynamicClient, sensor, scm.eventBusConfig, scm.eventBusSubject, scm.hostname, m)
			scm.C <- sensorCtx

			for {
				event, ok := <-watcher.Events
				fmt.Println("*****************")
				fmt.Println(event, event.Name, event.Op.String())
				fmt.Println("*****************")

				if !ok {
					cancel()
					return
				}

				if event.Op == fsnotify.Remove {
					cancel()
					watcher.Remove(sensorPath)
					watcher.Add(sensorPath)
					break
				}
			}
		}
	}()

	return nil
}
