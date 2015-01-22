package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

func Produce(Quit chan bool, Host []string, Topic string, Data chan []byte) {
	client, err := sarama.NewClient("crontab_client", Host, sarama.NewClientConfig())
	if err != nil {
		panic(err)
	} else {
		log.Println("kafka producer connected")
	}
	defer client.Close()

	cfg := sarama.NewProducerConfig()
	cfg.Partitioner = sarama.NewRoundRobinPartitioner
	producer, err := sarama.NewProducer(client, cfg)
	if err != nil {
		panic(err)
	}
	defer producer.Close()
	log.Println("kafka producer ready")

	for {
		select {
		case pack := <-Data:
			producer.Input() <- &sarama.MessageToSend{Topic: Topic, Key: nil, Value: sarama.ByteEncoder(pack)}
		case err := <-producer.Errors():
			log.Println(err)
		case <-Quit:
			break
		}
	}
}

func main() {
	Data := make(chan []byte, 10)
	Quit := make(chan bool, 1)
	if len(os.Args) != 4 {
		fmt.Println("./kafka_sender <host> <scene_id> <key>")
		return
	}
	host := os.Args[1]
	id := os.Args[2]
	key := os.Args[3]
	go Produce(Quit, []string{host}, "timeout", Data)
	Data <- []byte(id + " " + key + " " + strconv.FormatInt(time.Now().Unix(), 10))
	Quit <- true
	time.Sleep(2 * time.Second)
}
