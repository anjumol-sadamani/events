package kafka

import (
	"context"
	"eventprocessor/config"
	"eventprocessor/model"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	//placeholder {environment}
	ConsumerTopic      = "event.processor.%s"
	ConsumerRetryTopic = "event.processor.consumer.retry.%s"
	ConsumerDlq        = "event.processor.consumer.dlq.%s"
)

type KafkaConfig struct {
	Conn        *kafka.Conn   // Conn connects to the kafka brokers
	Reader      KafkaConsumer // Reader reading from the topic defined in the Environment variable.
	RetryWriter *kafka.Writer
	DlqWriter   *kafka.Writer
}

//Split the brokers string into slice.
//Example kafkaBrokerStr = "localhost:8080,localhost:8081"
// will return ["localhost:8080", "localhost:8081"]
func brokersSlice(kafkaBrokerStr string) []string {
	return strings.Split(kafkaBrokerStr, ",")
}

func CreateKafkaConnection() *kafka.Conn {
	configuration := config.GetConfig()
	kafkaBrokers := brokersSlice(configuration.KafkaServerHost)
	var connLeader *kafka.Conn
	//todo doubt
	for _, broker := range kafkaBrokers {
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			log.Fatalf("Error while Dialing to the kafka broker: %s", err.Error())
		}
		controller, err := conn.Controller()
		if err != nil {
			log.Fatalf("error while creating the connection with the broker: %s", err.Error())
		}
		connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err != nil {
			log.Fatalf(err.Error())
		}
	}
	return connLeader
}

func CreateKafkaReader(topic string) *KafkaReader {
	configuration := config.GetConfig()
	kafkaBrokers := brokersSlice(configuration.KafkaServerHost)
	groupId := fmt.Sprintf("consumer-group-%s", topic)
	kr := &KafkaReader{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  kafkaBrokers,
			GroupID:  groupId,
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
			// In case the consumer restarted with a different
			//consumer group then it will not retry all the committed messages.
			StartOffset: kafka.FirstOffset,
		}),
		RetryTimeInterval: configuration.RetryTimeInterval,
	}

	return kr
}

func CreateKafkaConfig() *KafkaConfig {
	configuration := config.GetConfig()
	k := &KafkaConfig{
		Conn:        CreateKafkaConnection(),
		Reader:      CreateKafkaReader(configuration.KafkaTopic),
		RetryWriter: CreateWriter(configuration.KafkaRetryTopic),
		DlqWriter:   CreateWriter(configuration.KafkaDlqTopic),
	}
	return k
}

func CreateWriter(topic string) *kafka.Writer {
	kafkaBrokerStr := config.GetConfig().KafkaServerHost
	kafkaBrokers := strings.Split(kafkaBrokerStr, ",")
	wt := &kafka.Writer{
		Addr:  kafka.TCP(kafkaBrokers...),
		Topic: topic,
	}
	return wt
}

func newTopic(topic string, numPartitions, replicationFactor int) kafka.TopicConfig {
	return kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}
}

func CreateKafkaTopic(k *KafkaConfig) {
	// Creating the topic by using the topic constructor.
	configuration := config.GetConfig()
	topic := func(topic string) string {
		return fmt.Sprintf(topic, configuration.Env)
	}
	replicationFactor := configuration.ReplicationFactor

	topicConfigs := []kafka.TopicConfig{
		newTopic(topic(ConsumerTopic), 100, replicationFactor),
		newTopic(topic(ConsumerRetryTopic), 100, replicationFactor),
		newTopic(topic(ConsumerDlq), 100, replicationFactor),
	}
	// creates the topics
	if err := k.Conn.CreateTopics(topicConfigs...); err != nil {
		//the application is existed if there is returned in creating the topics
		log.Fatalf("Error while creating the topics %v", err)
	}
}

func (k *KafkaConfig) RetryEvent(ctx context.Context, ei *model.EventInfo) error {
	ei.RetryCount = ei.RetryCount + 1
	ei.ProcessAfterTimeStamp = time.Now().Add(k.Reader.GetRetryTimeInterval())
	if err := k.WriteMessageToRetryTopic(ctx, ei); err != nil {
		log.Errorf("Error while write messages to kafka %s", err.Error())
		return err
	}
	return nil
}

func (k *KafkaConfig) WriteMessageToRetryTopic(ctx context.Context, ei *model.EventInfo) error {
	if err := k.RetryWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.Itoa(rand.Int())),
		Value: []byte(ei.Data),
	}); err != nil {
		log.Errorf("RetryEvent, Not able to write the message to topic")
		return err
	}
	return nil
}

func (k *KafkaConfig) WriteMessageToDLQTopic(ctx context.Context, ei *model.EventInfo) error {
	if err := k.DlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.Itoa(rand.Int())),
		Value: []byte(ei.Data),
	}); err != nil {
		log.Errorf("DLQEvent, Not able to write the message to topic")
		return err
	}
	return nil
}
