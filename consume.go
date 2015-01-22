package main

import (
	"bufio"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ugorji/go/codec"
)

func updateCursor(offset int64, writer *os.File) {
	writer.WriteAt([]byte(strconv.FormatInt(offset, 10)), 0)
}

func readCursor(reader *os.File) int64 {
	data := make([]byte, 32)
	n, err := reader.Read(data)
	if err == io.EOF {
		return 0
	} else if err != nil {
		panic(err)
	} else {
		cursor, err := strconv.ParseInt(strings.TrimSpace(string(data[0:n])), 10, 64)
		if err != nil {
			panic(err)
		}
		return cursor
	}
}

func Listen(quit chan bool, port string, updater func(Key, Value, Type string, Offset int64) bool) {
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	var c net.Conn
	for {
		if c, err = l.Accept(); err == nil {
			go func(client net.Conn) {
				defer client.Close()
				bio := bufio.NewReader(client)
				for {
					data, err := bio.ReadString('\n')
					if err == io.EOF {
						client.Write([]byte("end\n"))
						break
					} else if err != nil {
						client.Write([]byte(err.Error() + "\n"))
						break
					}
					if data == "\r\n" || data == "\n" {
						client.Write([]byte("<<\n"))
						break
					}
					data = strings.TrimSpace(data)
					arr := strings.Split(data, " ")
					tpe := arr[0]
					ret := true
					if tpe == "add" || tpe == "del" {
						if len(arr) != 10 {
							client.Write([]byte("invalid len\n"))
							continue
						}
						id := arr[1]
						key := arr[2]
						value := strings.Join(arr[3:], " ")
						ret = updater(id+" "+key, value, tpe, -1)
					} else if tpe == "begin" || tpe == "end" {
						unix, err := strconv.ParseInt(arr[1], 10, 64)
						if err != nil {
							client.Write([]byte(err.Error() + "\n"))
							ret = false
						}
						if tpe == "begin" {
							g_output_start = unix
						} else {
							g_output_end = unix
						}
					} else if tpe == "log" {
						if arr[1] == "on" {
							g_logout = true
						} else {
							g_logout = false
						}
					} else if tpe == "status" {
						client.Write([]byte("log:" + strconv.FormatBool(g_logout) + "\n"))
						client.Write([]byte("begin:" + strconv.FormatInt(g_output_start, 10) + "\n"))
						client.Write([]byte("end:" + strconv.FormatInt(g_output_end, 10) + "\n"))
					} else if tpe == "keepalive" {
						if g_output_start > 0 {
							n := time.Now().Unix()
							end := n
							if g_output_end > 0 {
								end = g_output_end
							}
							if n < g_output_start || n > end {
								client.Write([]byte("backup\n"))
							} else {
								client.Write([]byte("master\n"))
							}
						} else {
							client.Write([]byte("backup\n"))
						}
						continue
					} else {
						client.Write([]byte("THIS IS HELP\n--------------------------------------------------------------\n"))
						client.Write([]byte("  add\t\t<id> <key> <sec> <min> <hour> <day> <month> <week> <year>\n"))
						client.Write([]byte("  del\t\t<id> <key> <sec> <min> <hour> <day> <month> <week> <year>\n"))
						client.Write([]byte("  begin\t\t<unixtime>\n"))
						client.Write([]byte("  end\t\t<unixtime>\n"))
						client.Write([]byte("  log\t\ton | off\n"))
						client.Write([]byte("  status\n"))
						client.Write([]byte("  keepalive\n"))
					}
					if ret == true {
						client.Write([]byte("<ok\n"))
					} else {
						client.Write([]byte("<err\n"))
					}
				}
			}(c)
		}
	}
}

func Consume(quit chan bool, server []string, topic, cursor string, partition int32, updater func(Key, Value, Type string, Offset int64) bool) {
	logPrintln("Start Consume")
	cursorFile, errc := os.OpenFile(cursor, os.O_CREATE|os.O_RDWR, 0666)
	if errc != nil {
		panic(errc)
	}
	defer cursorFile.Close()

	client, err := sarama.NewClient("crontab_client", server, nil)
	if err != nil {
		panic(err)
	} else {
		logPrintln("kafka consumer connected")
	}
	defer client.Close()
	cfg := sarama.NewConsumerConfig()
	cfg.OffsetMethod = sarama.OffsetMethodManual
	cfg.OffsetValue = readCursor(cursorFile)
	consumer, err := sarama.NewConsumer(client, topic, partition, "crontab_group", cfg)
	if err != nil {
		panic(err)
	} else {
		logPrintln("kafka consumer ready")
	}
	defer consumer.Close()

consumerLoop:
	for {
		select {
		case <-quit:
			logPrintln("kafka consumer quit")
			break consumerLoop
		case event := <-consumer.Events():
			if event.Err != nil {
				logPrintln(event.Err)
				continue
			}
			var out map[string]interface{}
			err = codec.NewDecoderBytes(event.Value, &codec.MsgpackHandle{}).Decode(&out)
			if err != nil {
				logPrintln(err)
				continue
			}
			userSceneId := "0"
			switch v := out["userSceneId"].(type) {
			case uint64:
				userSceneId = strconv.FormatInt(int64(v), 10)
			case string:
				userSceneId = v
			case []byte:
				userSceneId = string(v)
			}
			year := time.Now().Year()
			timer := string(out["timer"].([]byte))
			types := string(out["type"].([]byte))
			arr := strings.Split(timer, " ")
			if len(arr) == 5 && types == "add" {
				if strings.Contains(arr[3], "*") {
					timer = strconv.Itoa(rand.Intn(10)) + " " + timer + " *"
				} else {
					timer = strconv.Itoa(rand.Intn(10)) + " " + timer + " " + strconv.Itoa(year)
				}
			}
			ret := updater(userSceneId+" "+string(out["key"].([]byte)), timer, types, event.Offset)
			if ret == true {
				updateCursor(event.Offset+1, cursorFile)
			}
		}
	}
}
