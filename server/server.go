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
)

func Server(conf *config.Config) {

	listener, err := tls.Listen("tcp", net.JoinHostPort(conf.Listen, strconv.Itoa(conf.Port)), conf.TlsConfig)
	if err != nil {
		log.Printf("tls.Listen() failed: %s", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("listener.Accept() failed: %s", err)
		}
		go handleClient(conf, conn)
	}
}

func handleClient(conf *config.Config, conn net.Conn) {

	enc := gob.NewEncoder(conn) // Will write to network.
	dec := gob.NewDecoder(conn) // Will read from network.

	interaction: for {
		var request protocol.Request
		err := dec.Decode(&request)
		if err != nil {
			if err != io.EOF {
				log.Println("decode error:", err)
			}
			break interaction
		}
		switch request.RequestType {
		case protocol.RequestDatasets:
			response := zfs.GetResponseDatasets(conf)
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
				break interaction
			}
		}
	}
	conn.Close()
}
