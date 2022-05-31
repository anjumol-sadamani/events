package kafka

import (
	"context"
	"eventprocessor/config"
	"fmt"
	"github.com/segmentio/kafka-go"
	"math/rand"
	"strconv"
	"time"
)

func DevMessageProducer(ctx context.Context) {
	fmt.Println("Running message producer")
	messageKey := 0

	writer := &kafka.Writer{
		Addr:  kafka.TCP(config.GetConfig().KafkaServerHost),
		Topic: "event.processor.DEV",
	}

	// send schema as first event
	err := writer.WriteMessages(ctx, kafka.Message{
		Key: []byte(strconv.Itoa(messageKey)),
		Value: []byte(`{
				"Query":{
				"deal":"Deal",
				"user":"User"
				},
				"Deal":{
				"title":"String",
				"price":"Float",
				"user":"User"
				},
				"User":{
				"name":"String",
				"deals":"[Deal]"
				}
				}
				`),
		Headers: []kafka.Header{
			{
				Key:   "type",
				Value: []byte("Schema"),
			},
		},
	})

	checkError(err)
	messageKey++

	for i := 0; i < 10; i++ {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(messageKey)),
			Value: getMessage(),
			Headers: []kafka.Header{
				{
					Key:   "type",
					Value: []byte("Query"),
				},
			},
		})
		checkError(err)
		messageKey++
		time.Sleep(time.Second)
	}

}

func getMessage() []byte {
	messageList := [][]byte{
		[]byte(`{
				"deal":{
				"price":true
				},
				"user":{
				"id":true,
				"name":true
				}
				}`),
		[]byte(`
				{
				"deal":{
					"title":true,
				    "price":true
					}
				}`),
		[]byte(`
				{
				"deal":{
					"title":true
					}
				}`),
		[]byte(`
				{
				"deal":{
					"title":true,
					"price":true
					}
				}`),
	}

	return messageList[rand.Intn(len(messageList)-1)]
}

func checkError(err error) {
	if err != nil {
		panic("could not write message " + err.Error())
	}
}
