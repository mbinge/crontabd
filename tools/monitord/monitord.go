package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/sevlyar/go-daemon"
)

var (
	signal = flag.String("s", "", `send signal to the daemon
		stop — fast shutdown
		quit — graceful shutdown
		restart — restart all task
		`)
)

func main() {
	flag.Parse()
	daemon.AddCommand(daemon.StringFlag(signal, "stop"), syscall.SIGTERM, termHandler)
	daemon.AddCommand(daemon.StringFlag(signal, "quit"), syscall.SIGQUIT, termHandler)
	cli_name := filepath.Base(os.Args[0])

	pidfile, logfile := getLogAndPid()
	cntxt := &daemon.Context{
		PidFileName: pidfile,
		PidFilePerm: 0644,
		LogFileName: logfile,
		LogFilePerm: 0640,
		WorkDir:     "./",
		Umask:       027,
		Args:        []string{"[" + cli_name + "]"},
	}

	log.SetFlags(log.Lshortfile | log.LstdFlags)
	if len(daemon.ActiveFlags()) > 0 {
		d, err := cntxt.Search()
		if err != nil {
			log.Fatalln("Unable send signal to the daemon:", err)
		}
		daemon.SendCommands(d)
		return
	}

	d, err := cntxt.Reborn()
	if err != nil {
		log.Fatalln(err)
	}
	if d != nil {
		return
	}
	defer cntxt.Release()

	log.Println("- - - - - - - - - - - - - - -")
	log.Println("daemon started")

	go worker()

	err = daemon.ServeSignals()
	if err != nil {
		log.Println("Error:", err)
	}
	log.Println("daemon terminated")
}

var (
	stop = make(chan struct{})
	done = make(chan struct{})
)

func worker() {
	_init(stop)
	done <- struct{}{}
}

func termHandler(sig os.Signal) error {
	log.Println("terminating...")
	stop <- struct{}{}
	if sig == syscall.SIGQUIT {
		log.Println("send stop")
		<-done
	}
	return daemon.ErrStop
}

func reloadHandler(sig os.Signal) error {
	return nil
}
