package main

import (
	"log"
	"strconv"
	"time"
)

type Timer struct {
	TimeList  *vList
	Timeout   chan Fire
	KafkaData chan []byte
	DropMap   map[string]bool
}

type Items struct {
	times []int64
	key   string
}

type Fire struct {
	Time int64
	Key  string
}

func (timer *Timer) Add(times []int64, key string) {
	for _, time := range times {
		logPrintln("add:", time, key)
		timer.TimeList.Set(time, []byte(key))
	}
}

func (timer *Timer) Del(key string) {
	timer.DropMap[key] = true
}

func (timer *Timer) run() {
	var fire Fire
	for {
		select {
		case fire = <-timer.Timeout:
			timer.KafkaData <- []byte(fire.Key + " " + strconv.FormatInt(fire.Time, 10))
		}
	}
}

func (timer *Timer) ticker() {
	c := time.Tick(1 * time.Second)
	for {
		select {
		case now := <-c:
			go timer.Fire(now.Unix())
		}
	}
}

func (timer *Timer) Init() {
	timer.Timeout = make(chan Fire, 10000000)
	timer.KafkaData = make(chan []byte, 1000000)
	timer.TimeList = new(vList)
	timer.TimeList.Init()
	timer.DropMap = make(map[string]bool, 1000)
	for i := 0; i < 10; i++ {
		go timer.run()
	}
	go timer.ticker()
}

func (timer *Timer) Fire(cursor int64) {
	if g_output_start == 0 || cursor < g_output_start {
		log.Println("Please Set Begin: echo 'begin {unixtime}' | ncat 127.0.0.1:8765\n Current:", g_output_start)
		return
	}
	if g_output_end > 0 && cursor > g_output_end {
		log.Println("Ignore Timer:", g_output_end)
		return
	}
	vslice := timer.TimeList.Pop(cursor)
	if vslice == nil {
		return
	}
	cur_send := 0
	s1 := time.Now()
	sended := make(map[string]bool, vslice.total)
	for {
		if vslice.total < 1 {
			break
		}
		key := string(vslice.Cut())
		logPrintln("will Send:", cursor, key)
		if _, ok := sended[key]; ok != true {
			if _, ok1 := timer.DropMap[key]; ok1 != true {
				timer.Timeout <- Fire{Time: cursor, Key: key}
				cur_send = cur_send + 1
			}
		}
		sended[key] = true
	}
	vslice.Drop()
	vslice = nil
	sended = nil
	logPrintln(cursor, "cost:", time.Now().Sub(s1), "cur_send:", cur_send, "buf:", len(timer.Timeout))
}
