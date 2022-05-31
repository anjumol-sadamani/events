package server

import (
	"context"
	"eventprocessor/config"
	"eventprocessor/event_processor"
	"eventprocessor/model"
	"os"
	"os/signal"
	"syscall"

	"sync"

	log "github.com/sirupsen/logrus"

	"eventprocessor/data"
	kf "eventprocessor/kafka"

	"github.com/gin-gonic/gin"
)

func Start() {

	db, err := data.Connection()
	if err != nil {
		log.Fatalf("DB connection error..Exiting the process")
	}
	router := gin.Default()
	configuration := config.GetConfig()
	switch configuration.Env {
	case "dev":
		gin.SetMode(gin.DebugMode)
	case "test":
		gin.SetMode(gin.TestMode)
	default:
		gin.SetMode(gin.ReleaseMode)
	}
	InitEventProcessorRoutes(db, router)
	kafkaConfig := kf.CreateKafkaConfig()
	kf.CreateKafkaTopic(kafkaConfig)
	wg := new(sync.WaitGroup)
	stopChannel := make(chan os.Signal, 1)
	signal.Notify(stopChannel, os.Interrupt, syscall.SIGTERM)
	el := event_processor.EventListener{
		AppConfig:     configuration,
		KafkaConfig:   kafkaConfig,
		EventService:  event_processor.EventHandlerServiceCreate(db),
		SchemaService: event_processor.SchemaHandlerServiceCreate(db),
		EventChannel:  make(chan model.EventInfo, 1000),
		Ctx:           context.Background(),
		Wg:            wg,
		StopChannel:   stopChannel,
	}
	wg.Add(3)
	//Dummy event producer for testing purpose
	go kf.DevMessageProducer(context.Background())
	go el.ReadEvents()
	go el.PersistQuery()
	err = router.Run(":" + configuration.Port)
	if err != nil {
		log.Error(err)
	}
	wg.Wait()

	close(el.EventChannel)
	close(stopChannel)
}
