package main

import (
	"bufio"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ungerik/go-mail"
)

type Bridge struct {
	alive  bool
	master bool
	ttl    int64
	index  int
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

func send_msg(host, body string) {
	from := "robot@xiaomi.com"
	mail := email.NewBriefMessageFrom("[ERROR][CRONTABD]"+host, body, from, g_cfg.Mail...)
	if err := mail.Send(); err != nil {
		log.Println(err)
	}
	log.Println(host, body)
}

func beMaster(host string, cur int64) bool {
	server, err := net.Dial("tcp", host)
	defer server.Close()
	if err != nil {
		send_msg(host, "To Master Connect Failed")
		return false
	}
	bio := bufio.NewReader(server)
	_, err = server.Write([]byte("begin " + strconv.FormatInt(cur, 10) + "\n"))
	if err != nil {
		send_msg("SwitchErr", host+":"+err.Error())
		return false
	}
	line, err1 := bio.ReadString('\n')
	if err1 != nil {
		send_msg("SwitchErr", host+":"+err1.Error())
		return false
	}
	send_msg("Switch", host+":"+line)
	return true
}

func keepalive(quit chan bool, host string, index, retry int, bridge chan Bridge) {
	ttl := time.NewTicker(500 * time.Millisecond)
	try := 0
	var server net.Conn
	var err error
	var last_ttl = int64(0)
	var bio *bufio.Reader
	for {
		select {
		case <-quit:
			server.Close()
			break
		case n := <-ttl.C:
			if try < 1 {
				if server != nil {
					server.Close()
				}
				server, err = net.Dial("tcp", host)
				if err != nil {
					log.Println(host, "Connect Failed")
					bridge <- Bridge{alive: false, master: false, ttl: last_ttl, index: index}
					time.Sleep(time.Second)
					continue
				}
				bio = bufio.NewReader(server)
				try = retry
			}
			_, err = server.Write([]byte("keepalive\n"))
			if err != nil {
				try--
				continue
			}
			line, err1 := bio.ReadString('\n')
			if err1 != nil {
				try--
				continue
			}
			line = strings.TrimRight(line, "\n")
			if line == "master" {
				last_ttl = n.UnixNano()
				bridge <- Bridge{alive: true, master: true, ttl: last_ttl, index: index}
			} else if line == "backup" {
				last_ttl = n.UnixNano()
				bridge <- Bridge{alive: true, master: false, ttl: last_ttl, index: index}
			} else {
				log.Println("Unexpect For:", line)
				try--
			}
		}
	}
}

var g_cfg *Configure

func _init(quit chan struct{}) {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	cn, err := net.Dial("tcp", "127.0.0.1:25")
	if err != nil {
		email.Config.Host = "10.105.10.74"
		email.Config.Port = 8025
	} else {
		cn.Close()
		email.Config.Host = "127.0.0.1"
		email.Config.Port = 25
	}
	if g_cfg == nil {
		g_cfg = LoadCfg()
	}

	exit := make(chan bool, len(g_cfg.Host))
	bridges := make(chan Bridge, 10)
	for i, host := range g_cfg.Host {
		go keepalive(exit, host, i, 3, bridges)
	}

	last_alive := make(map[int]int64, len(g_cfg.Host))
	master_index := -1
	master_ttl := int64(0)
	no_master := 0
	tk := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-quit:
			for _ = range g_cfg.Host {
				exit <- true
			}
			time.Sleep(time.Second)
		case info := <-bridges:
			last_alive[info.index] = info.ttl
			if info.master == true {
				no_master = 0
				master_ttl = info.ttl
				if master_index != info.index {
					from := "NONE"
					if master_index > -1 {
						from = g_cfg.Host[master_index]
					}
					go send_msg("master change", from+"=>"+g_cfg.Host[info.index])
					master_index = info.index
				}
			}
		case n := <-tk.C:
			limit := 1000 * int64(time.Millisecond)
			cur := n.UnixNano()
			if master_ttl == 0 {
				no_master++
				if no_master%10 == 0 {
					send_msg("NONE_MASTER", strconv.Itoa(no_master))
				}
				continue
			}
			if cur-master_ttl > limit {
				for index, ttl := range last_alive {
					if index != master_index && cur-ttl < limit {
						beMaster(g_cfg.Host[index], n.Unix())
						break
					}
				}
			}
		}
	}
}
