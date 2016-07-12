package client

import (
	"fmt"
	"crypto/tls"
	"config"
	"net"
	"strconv"
	"time"
	"log"
	"encoding/gob"
	"protocol"
)

func Client(conf *config.Config) {
	for {
		session(conf)
		time.Sleep(conf.Delay * time.Second)
	}
}

func session(conf *config.Config) {
	conn, err := tls.Dial("tcp", net.JoinHostPort(conf.Remote, strconv.Itoa(conf.Port)), conf.TlsConfig)
	if err != nil {
		log.Printf("tls.Dial() failed: %s", err)
		return
	}
	defer conn.Close()

	enc := gob.NewEncoder(conn) // Will write to network.
	dec := gob.NewDecoder(conn) // Will read from network.

	request := new(protocol.Request)
	request.RequestType = protocol.RequestDatasets
	err = enc.Encode(&request)
	if err != nil {
		log.Println("encode error:", err)
		return
	}
	var response protocol.Response
	err = dec.Decode(&response)
	if err != nil {
		log.Println("decode error:", err)
		return
	}
	switch response.ResponseType {
	case protocol.ResponseDatasets:
		for _, dataset := range response.Datasets {
			fmt.Println(dataset)
		}
		fmt.Println("")
	case protocol.ResponseError:
		log.Println("remote error:", response.Error)
		return
	default:
		panic(fmt.Sprintf("unexpected response type '%d'", response.ResponseType))
	}
}