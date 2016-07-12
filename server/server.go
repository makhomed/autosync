package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"config"
	"strconv"
	"log"
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
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	_, err := fmt.Fprintf(conn, "Hello TLS\n")
	if err != nil {
		fmt.Printf("Error on connection:%v", err)
	}
	conn.Close()

}
