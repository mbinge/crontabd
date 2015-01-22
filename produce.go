package main

import (
	"errors"
	"math/rand"
	"net"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/ugorji/go/codec"
)

func BuildMsg(Key string, Time int64) ([]byte, error) {
	arr := strings.Split(Key, " ")
	if len(arr) != 2 {
		return nil, errors.New("key invalid")
	}
	scenerun := make(map[string]interface{})
	scenerun["userSceneId"] = arr[0]
	scenerun["from"] = "timeout"
	fire := make(map[string]interface{})
	fire["src"] = arr[1]
	fire["value"] = ""
	fire["time"] = Time
	scenerun["fire"] = fire
	var b []byte
	err := codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}).Encode(scenerun)
	return b, err
}

func Send(Quit chan bool, Host []string, Data chan []byte) {
	hosts := make([]net.Conn, 0)
	var err error
	var conn net.Conn
	var real_len = 0
	for _, host := range Host {
		conn, err = net.Dial("tcp", host)
		if err != nil {
			logPrintln(err)
		}
		hosts = append(hosts, conn)
		real_len = real_len + 1
	}
	for {
		select {
		case pack := <-Data:
			idx := rand.Intn(real_len)
			_, err = hosts[idx].Write(append(pack, '\n'))
			if err != nil {
				logPrintln("write to host:", err)
			}
		case <-Quit:
			break
		}
	}
}

func Produce(Quit chan bool, Host []string, Topic string, Data chan []byte) {
	client, err := sarama.NewClient("crontab_client", Host, sarama.NewClientConfig())
	if err != nil {
		panic(err)
	} else {
		logPrintln("kafka producer connected")
	}
	defer client.Close()

	cfg := sarama.NewProducerConfig()
	//cfg.RequiredAcks = sarama.WaitForAll
	//cfg.Timeout = 1000 * time.Millisecond
	cfg.Partitioner = sarama.NewRoundRobinPartitioner
	producer, err := sarama.NewProducer(client, cfg)
	if err != nil {
		panic(err)
	}
	defer producer.Close()
	logPrintln("kafka producer ready")

	for {
		select {
		case pack := <-Data:
			producer.Input() <- &sarama.MessageToSend{Topic: Topic, Key: nil, Value: sarama.ByteEncoder(pack)}
		case err := <-producer.Errors():
			logPrintln(err)
		case <-Quit:
			break
		}
	}
}
