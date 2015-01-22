package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

var g_cfg *Configure
var g_output_start int64
var g_output_end int64
var g_logout = true

func logPrintln(v ...interface{}) {
	if g_logout == true {
		log.Println(v)
	}
}

func getLogAndPid() (string, string) {
	g_cfg = LoadCfg()
	bindir := filepath.Dir(os.Args[0])
	piddir := realPath(filepath.Dir(bindir + "/" + g_cfg.Pid))
	err := os.MkdirAll(piddir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
	logdir := realPath(filepath.Dir(bindir + "/" + g_cfg.Log))
	err1 := os.MkdirAll(logdir, os.ModePerm)
	if err1 != nil {
		log.Fatal(err1)
		os.Exit(-1)
	}
	return bindir + "/" + g_cfg.Pid, bindir + "/" + g_cfg.Log
}

func realPath(path string) string {
	if path[0] == '~' {
		home := os.Getenv("HOME")
		path = home + path[1:]
	}
	rpath, err := filepath.Abs(path)
	if err == nil {
		path = rpath
	}
	return strings.TrimSpace(path)
}

func parseTask(val string) (task Task, err error) {
	arr := strings.Split(val, " ")
	if len(arr) == 7 {
		task.Fields = arr[0:7]
		task.Val = val
	} else {
		err = fmt.Errorf("wrong number of fields; expected >6")
	}
	return
}

var timer *Timer

func DropTask(key, val string, span, base int64) {
	timer.DropMap[key] = true
}

func AddTask(key, val string, span, base int64) {
	task, err := parseTask(val)
	if err == nil {
		task.Key = key
		task.Base = base
		ret := task.Dispatch(span, true)
		timer.Add(ret, key)
	}
}

var inWalking = false

//从levelDB中载入列表判断是否近期执行
func WalkTasks(base, span int64, db *leveldb.DB) {
	var task Task
	var err error
	var ok bool
	timer.DropMap = nil
	timer.DropMap = make(map[string]bool)
	s := time.Now()
	iter := db.NewIterator(nil, nil)
	parseMap := make(map[string][]string, 1024)
	for iter.Next() {
		key := string(iter.Key())
		val := string(iter.Value())
		if _, ok = parseMap[val]; ok == false {
			parseMap[val] = make([]string, 0)
		}
		parseMap[val] = append(parseMap[val], key)
	}
	iter.Release()
	inWalking = true
	for val, keys := range parseMap {
		task, err = parseTask(val)
		if err == nil {
			task.Base = base
			ret := task.Dispatch(span, true)
			for _, key := range keys {
				task.Key = key
				timer.Add(ret, key)
			}
		}
	}
	inWalking = false
	parseMap = nil
	e := time.Now()
	logPrintln(e.Sub(s))
}

func writeHeap(time int64) {
	f, err := os.Create("mem." + strconv.FormatInt(time, 10))
	if err != nil {
		log.Fatal(err)
	}
	pprof.WriteHeapProfile(f)
	f.Close()
}

func _init(quit chan struct{}) {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)
	db, err := leveldb.OpenFile("task.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	timer = new(Timer)
	timer.Init()

	callback := func(Key, Value, Type string, Offset int64) bool {
		logPrintln("Consume:", Offset, Key, Value, Type)
		if Type == "add" {
			err = db.Put([]byte(Key), []byte(Value), nil)
			if err != nil {
				logPrintln(err)
			}
			if inWalking == false {
				AddTask(Key, Value, g_cfg.Span*2, time.Now().Unix())
			}
		} else if Type == "del" {
			err = db.Delete([]byte(Key), nil)
			if err != nil {
				logPrintln(err)
			}
			DropTask(Key, Value, g_cfg.Span*2, time.Now().Unix())
		}
		return true
	}

	exit := make(chan bool)
	exitLen := 1
	go Listen(exit, g_cfg.Port, callback)
	if g_cfg.KafkaIn != nil {
		exitLen = exitLen + len(g_cfg.KafkaIn)
		for _, kk := range g_cfg.KafkaIn {
			go Consume(exit, kk.Host, kk.Topic, kk.Cursor, kk.Partition, callback)
		}
	}
	if g_cfg.KafkaOut != nil {
		exitLen = exitLen + len(g_cfg.KafkaOut)
		for _, kk := range g_cfg.KafkaOut {
			go Produce(exit, kk.Host, kk.Topic, timer.KafkaData)
		}
	} else if len(g_cfg.Target) > 0 {
		exitLen = exitLen + 4
		go Send(exit, g_cfg.Target, timer.KafkaData)
		go Send(exit, g_cfg.Target, timer.KafkaData)
		go Send(exit, g_cfg.Target, timer.KafkaData)
		go Send(exit, g_cfg.Target, timer.KafkaData)
	} else {
		exitLen = exitLen + 1
		go func(Quit chan bool, Data chan []byte) {
			for {
				select {
				case pack := <-Data:
					logPrintln(string(pack))
				case <-Quit:
					break
				}
			}
		}(exit, timer.KafkaData)
	}
	c := time.Tick(time.Duration(g_cfg.Span) * time.Second)
	WalkTasks(time.Now().Unix(), g_cfg.Span*2, db)
	for {
		select {
		case now := <-c:
			cur := now.Unix()
			WalkTasks(cur+g_cfg.Span, g_cfg.Span, db)
		case <-quit:
			for i := 0; i < exitLen; i++ {
				exit <- true
			}
			break
		}
	}
}
