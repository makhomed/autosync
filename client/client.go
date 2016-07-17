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
	"zfs"
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

	var request protocol.Request
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
		processDatasetsResponse(conf, enc, dec, &response)
	case protocol.ResponseError:
		log.Println("remote error:", response.Error)
		return
	default:
		log.Println("unexpected response type '%d'", response.ResponseType)
		return
	}
}

func processDatasetsResponse(conf *config.Config, enc *gob.Encoder, dec *gob.Decoder, response *protocol.Response) {
	datasets := util.FilterDatasets(conf, response.Datasets)
	for _, dataset := range datasets {
		var request protocol.Request
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
			processSnapshotsResponse(conf, enc, dec, &response, dataset)
		case protocol.ResponseError:
			log.Println("snapshots remote error:", response.Error)
			continue
		default:
			log.Printf("unexpected response type '%d'", response.ResponseType)
			continue
		}
	}
}

func processSnapshotsResponse(conf *config.Config, enc *gob.Encoder, dec *gob.Decoder, response *protocol.Response, sourceDataset string) {
	sourceSnapshots := response.Snapshots
	if len(sourceSnapshots) == 0 {
		log.Println("source snapshots list empty, can't replicate")
		return
	}
	destinationDataset := util.DestinationDataset(conf.Storage, sourceDataset)
	destinationDatasets, err := zfs.GetDestinationDatasets(conf.Storage)
	if err != nil {
		log.Println("can't get list of destination datasets", err)
		return
	}
	if _, ok := destinationDatasets[destinationDataset]; !ok {
		// destination dataset not exists, process full zfs send
	}
	destinationSnapshots, err := zfs.GetSnapshots(destinationDataset)
	if err != nil {
		log.Println("can't get list of destination snapshots", err)
		return
	}
	intersection := util.IntersectionOfSnapshots(sourceSnapshots, destinationSnapshots)
	if len(intersection) == 0 {
		// no intersection, process full zfs send
	} else {
		// intersection, process incremental zfs send
	}

	fmt.Println("")
	fmt.Println(sourceDataset, destinationDataset)
	for _, sourceSnapshot := range sourceSnapshots {
		fmt.Println(sourceSnapshot)
	}
	fmt.Println(destinationSnapshots)
}

