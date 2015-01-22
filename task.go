package main

import (
	"strings"
)

type Task struct {
	Base   int64
	Fields []string
	Key    string
	Val    string
}

var _parse_cache = make(map[string]Schedule, 10)

func (task *Task) Dispatch(offset int64, isAdd bool) []int64 {
	var ct Schedule
	var ok bool
	var err error
	key := strings.Join(task.Fields, " ")
	if ct, ok = _parse_cache[key]; ok != true {
		ct, err = ParseSchedule(task.Fields)
		if err != nil {
			logPrintln(err)
		}
		_parse_cache[key] = ct
	}
	curBase := task.Base
	end := offset + curBase
	timeline := make([]int64, 0)
	for {
		t, valid := ct.Next(curBase, end)
		if valid == true {
			timeline = append(timeline, t)
			curBase = t + 1
		} else {
			break
		}
	}
	return timeline
}
