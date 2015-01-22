package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type Item struct {
	timer int64
	key   string
}

func TestTimer(t *testing.T) {
	return
	timer := new(Timer)
	timer.Init()
	start := time.Now().Unix()
	list := make([]int64, 20)
	for i := start; i < start+20; i++ {
		list = append(list, i)
	}
	timer.Add(list, "time1")
	timer.Add(list, "time2")
	timer.Add(list, "time3")
	timer.Del("time1")
	time.Sleep(20 * time.Second)
}

func TestSchedule(tt *testing.T) {
	return
	fields := strings.Split("*/4 * * * * * *", " ")
	start1 := time.Now()
	ct, err := ParseSchedule(fields[0:7])
	if err != nil {
		log.Fatal(err)
	}
	end1 := time.Now()
	fmt.Println(end1.Sub(start1))

	start1 = time.Now()
	var t int64
	var valid bool
	cur := time.Now().Unix()
	for i := 0; i < 1000000; i++ {
		t, valid = ct.Next(cur, 3600)
	}
	end1 = time.Now()
	fmt.Println(end1.Sub(start1))
	fmt.Println(t, valid)
	start1 = time.Now()
	cur = t + 1
	for i := 0; i < 1000000; i++ {
		t, valid = ct.Next(cur, 3600)
	}
	end1 = time.Now()
	fmt.Println(end1.Sub(start1))
	fmt.Println(t, valid)
}

func TestTaskimport(t *testing.T) {
	return
	db, _ := leveldb.OpenFile("task.db", nil)
	defer db.Close()
	for i := 0; i < 1000000; i++ {
		si := strconv.Itoa(i)
		db.Put([]byte("key_"+si), []byte("*/4 * * * * * *"), nil)
	}
}

func TestNext(t *testing.T) {
	timer := new(Timer)
	timer.Init()
	s := time.Now()
	task, err := parseTask("0 0 20 * * 0,1,2,3,4,5,6 *")
	task.Base = 1420372800
	ret := task.Dispatch(10, true)
	fmt.Println(ret)
	return
	for i := 0; i < 500000; i++ {
		if err == nil {
			timer.Add(ret, strconv.Itoa(i))
		}
	}
	e := time.Now()
	log.Println(e.Sub(s))
}

func TestConsum(t *testing.T) {
	return
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	quit := make(chan bool)
	go Consume(quit, []string{"10.101.11.217:9092", "10.101.11.216:9092", "10.101.11.218:9092"}, "timer", "var/cursor", 0, func(Key, Value, Type string) bool {
		fmt.Println(Key, Value, Type)
		return true
	})
	time.Sleep(1000 * time.Second)
}

func TestProduce(t *testing.T) {
	return
	quit := make(chan bool)
	data := make(chan []byte, 100)
	go Produce(quit, []string{"10.237.140.50:9092"}, "scene-run", data)
	c := time.Tick(1 * time.Second)
	for now := range c {
		b, err := BuildMsg("12345 timer", now.Unix())
		if err == nil {
			data <- b
		}
	}
}

func TestVSlice(t *testing.T) {
	return
	vs := new(vSlice)
	vs.Add([]byte("123"))
	vs.Add([]byte("456"))
	r1 := vs.Cut()
	r2 := vs.Cut()
	log.Println(string(r1))
	log.Println(string(r2))
}

func TestWalk(t *testing.T) {
	return
	db, err := leveldb.OpenFile("task.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	cur := int64(1420213798)
	WalkTasks(cur+10, 10, db)
}
