package client

import (
	"fmt"
	"os"
	"io"
	"crypto/tls"
	"config"
	"net"
	"strconv"
	"time"
	"log"
)

func Client(conf *config.Config) {
	for ;; time.Sleep(conf.Delay * time.Second) {
		conn, err := tls.Dial("tcp", net.JoinHostPort(conf.Remote, strconv.Itoa(conf.Port)), conf.TlsConfig)
		if err != nil {
			log.Printf("tls.Dial() failed: %s", err)
			continue
		}
		err = conn.Handshake()
		if err != nil {
			log.Printf("Failed handshake: %v\n", err)
			continue
		}

		_, err = io.Copy(os.Stdout, conn)
		if err != nil {
			fmt.Printf("Failed receiving data:%v\n", err)
		}
		conn.Close()
	}
}
