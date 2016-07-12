package main

import (
	"fmt"
	"os"
	"config"
	"flag"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"
	"server"
	"client"
)

//go:generate go get gopkg.in/natefinch/lumberjack.v2

var configName = flag.String("c", "/opt/autosync/conf/autosync.conf", "config")

func main() {
	flag.Parse()
	conf, err := config.New(*configName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing config '%s' : %v\n", *configName, err)
		os.Exit(2)
	}

	log.SetOutput(&lumberjack.Logger{
		Filename:   conf.Log,
		MaxSize:    1, // megabytes
		MaxBackups: 9,
		MaxAge:     365, //days
		LocalTime:  true,
	})

	switch conf.Mode {
	case "server":
		server.Server(conf)
	case "client":
		client.Client(conf)
	}
}
