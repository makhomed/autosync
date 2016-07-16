package server

import (
	"crypto/tls"
	"net"
	"config"
	"strconv"
	"log"
	"encoding/gob"
	"protocol"
	"zfs"
	"io"
	"github.com/mxk/go-flowrate/flowrate"
)

func Server(conf *config.Config) {

	listener, err := tls.Listen("tcp", net.JoinHostPort(conf.Listen, strconv.Itoa(conf.Port)), conf.TlsConfig)
	if err != nil {
		log.Fatalf("tls.Listen() failed: %s", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("listener.Accept() failed: %s", err)
			continue
		}
		go handleClient(conf, conn)
	}
}

func handleClient(conf *config.Config, conn net.Conn) {
	defer conn.Close()

	enc := gob.NewEncoder(flowrate.NewWriter(conn, conf.Bwlimit * 1024)) // Will write to network.
	dec := gob.NewDecoder(flowrate.NewReader(conn, conf.Bwlimit * 1024)) // Will read from network.

	for {
		var request protocol.Request
		err := dec.Decode(&request)
		if err != nil {
			if err != io.EOF {
				log.Println("decode error:", err)
			}
			return
		}
		switch request.RequestType {
		case protocol.RequestDatasets:
			response := zfs.GetResponseDatasets(conf)
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
				return
			}
		case protocol.RequestSnapshots:
			dataset := request.DatasetName
			response := zfs.GetResponseSnapshots(conf, dataset)
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
				return
			}
		default:
			log.Println("unknown request type '%d'", request.RequestType)
			return
		}
	}
}
