package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
)

type Kafka struct {
	Host      []string
	Topic     string
	Partition int32
	Cursor    string
}

type Configure struct {
	KafkaIn  []Kafka
	KafkaOut []Kafka
	Log      string
	Pid      string
	Span     int64
	Port     string
	Target   []string
}

var (
	configure = flag.String("c", "config.json", "Configuration file")
)

func (cfg *Configure) ReadFrom(file string) {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(b, cfg)
	if err != nil {
		log.Fatal(err)
	}
}

func refreshCfg() *Configure {
	cfg := new(Configure)
	cfg.ReadFrom(*configure)
	return cfg
}

func LoadCfg() *Configure {
	lstat, err := os.Lstat(*configure)
	if err != nil {
		log.Fatal(err)
	} else if lstat.Mode()&os.ModeType != 0 {
		log.Fatalf(`"%s" is not a text file`, lstat.Name())
		os.Exit(-1)
	}
	return refreshCfg()
}
