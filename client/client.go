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
	"github.com/mxk/go-flowrate/flowrate"
	"util"
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

	enc := gob.NewEncoder(flowrate.NewWriter(conn, conf.Bwlimit * 1024)) // Will write to network.
	dec := gob.NewDecoder(flowrate.NewReader(conn, conf.Bwlimit * 1024)) // Will read from network.

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
		processDatasetsResponse1(conf, enc, dec, &response)
	case protocol.ResponseError:
		log.Println("remote error:", response.Error)
		return
	default:
		log.Println("unexpected response type '%d'", response.ResponseType)
		return
	}
}

func processDatasetsResponse1(conf *config.Config, enc *gob.Encoder, dec *gob.Decoder, response *protocol.Response) {
	datasets := util.FilterDatasets(conf, response.Datasets)
	for _, dataset := range datasets {
		fmt.Println(dataset)
	}
	fmt.Println("")
}

func processDatasetsResponse(conf *config.Config, enc *gob.Encoder, dec *gob.Decoder, response *protocol.Response) {
	datasets := util.FilterDatasets(conf, response.Datasets)
	for _, dataset := range datasets {
		request := new(protocol.Request)
		request.RequestType = protocol.RequestSnapshots
		request.DatasetName = dataset
		err := enc.Encode(&request)
		if err != nil {
			log.Println("snapshots encode error:", err)
			continue
		}
		var response protocol.Response
		err = dec.Decode(&response)
		if err != nil {
			log.Println("snapshots decode error:", err)
			continue
		}
		switch response.ResponseType {
		case protocol.ResponseSnapshots:
			processSnapshotsResponse(conf, enc, dec, &response)
		case protocol.ResponseError:
			log.Println("snapshots remote error:", response.Error)
			continue
		default:
			panic(fmt.Sprintf("unexpected response type '%d'", response.ResponseType))
		}
	}
}

func processSnapshotsResponse(conf *config.Config, enc *gob.Encoder, dec *gob.Decoder, response *protocol.Response) {
	snapshots := response.Snapshots
	for _, snapshot := range snapshots {
		fmt.Println(snapshot)
	}
	fmt.Println("")
}
