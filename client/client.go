package client

import (
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
	"os/exec"
	"bufio"
)

func Client(conf *config.Config) {
	for {
		if util.Verbose() {
			log.Println("Sync started...")
		}
		t0 := time.Now()
		session(conf)
		t1 := time.Now()
		if util.Verbose() {
			log.Printf("Sync finished, duration %v", t1.Sub(t0))
		}
		if util.Verbose() {
			log.Println("Delay started...")
		}
		d0 := time.Now()
		time.Sleep(conf.Delay * time.Second)
		d1 := time.Now()
		if util.Verbose() {
			log.Printf("Delay finished, duration %v", d1.Sub(d0))
		}
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
		processDatasetsResponse(conf, &response)
	case protocol.ResponseError:
		log.Println("remote error:", response.Error)
		return
	default:
		log.Println("unexpected response type:", response.ResponseType)
		return
	}
}

func processDatasetsResponse(conf *config.Config, response *protocol.Response) {
	conn, err := tls.Dial("tcp", net.JoinHostPort(conf.Remote, strconv.Itoa(conf.Port)), conf.TlsConfig)
	if err != nil {
		log.Printf("tls.Dial() failed: %s", err)
		return
	}
	defer conn.Close()

	enc := gob.NewEncoder(flowrate.NewWriter(conn, conf.Bwlimit * 1024)) // Will write to network.
	dec := gob.NewDecoder(flowrate.NewReader(conn, conf.Bwlimit * 1024)) // Will read from network.

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
			processSnapshotsResponse(conf, &response, dataset)
		case protocol.ResponseError:
			log.Println("snapshots remote error:", response.Error)
			continue
		default:
			log.Println("unexpected response type:", response.ResponseType)
			continue
		}
	}
}

func processSnapshotsResponse(conf *config.Config, response *protocol.Response, sourceDataset string) {
	sourceSnapshots := response.Snapshots
	if len(sourceSnapshots) == 0 {
		log.Printf("source snapshots list empty, can't replicate %s", sourceDataset)
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
		sourceSnapshot := util.SourceSnapshotForFullZfsSend(sourceSnapshots)
		processFullZfsSend(conf, sourceDataset, sourceSnapshot)
		return
	}
	destinationSnapshots, err := zfs.GetSnapshots(destinationDataset)
	if err != nil {
		log.Println("can't get list of destination snapshots", err)
		return
	}
	intersection := util.IntersectionOfSnapshots(sourceSnapshots, destinationSnapshots)
	if len(intersection) == 0 {
		// no intersection, process full zfs send
		sourceSnapshot := util.SourceSnapshotForFullZfsSend(sourceSnapshots)
		processFullZfsSend(conf, sourceDataset, sourceSnapshot)
		return
	} else {
		// intersection, process incremental zfs send
		snapshot1 := intersection[len(intersection) - 1]
		snapshot2 := sourceSnapshots[len(sourceSnapshots) - 1]
		if snapshot1 != snapshot2 {
			processIncrementalZfsSend(conf, sourceDataset, snapshot1, snapshot2)
			return
		}
	}
}

func processFullZfsSend(conf *config.Config, sourceDataset string, snapshot1 string) {

	destinationDataset := util.DestinationDataset(conf.Storage, sourceDataset)
	destinationSnapshots, err := zfs.GetSnapshots(destinationDataset)
	if err != nil {
		log.Println("can't get list of destination snapshots", err)
		return
	}
	for destinationSnapshot := range destinationSnapshots {
		zfs.Destroy(destinationDataset + "@" + destinationSnapshot)
	}

	// destinationDataset := util.DestinationDataset(conf.Storage, sourceDataset)
	// fmt.Println("full", sourceDataset, destinationDataset, snapshot1)
	conn, err := tls.Dial("tcp", net.JoinHostPort(conf.Remote, strconv.Itoa(conf.Port)), conf.TlsConfig)
	if err != nil {
		log.Printf("tls.Dial() failed: %s", err)
		return
	}
	defer conn.Close()

	enc := gob.NewEncoder(flowrate.NewWriter(conn, conf.Bwlimit * 1024)) // Will write to network.
	dec := gob.NewDecoder(flowrate.NewReader(conn, conf.Bwlimit * 1024)) // Will read from network.

	var request protocol.Request
	request.RequestType = protocol.RequestFullSnapshot
	request.DatasetName = sourceDataset
	request.Snapshot1Name = snapshot1
	err = enc.Encode(&request)
	if err != nil {
		log.Println("encode error:", err)
		return
	}

	//zfs recv -F -d tank/backup
	cmd := exec.Command("zfs", "recv", "-F", "-d", conf.Storage)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Println("can't run zfs recv command:", err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Println("can't run zfs recv command:", err)
		return
	}
	if err := cmd.Start(); err != nil {
		log.Println("can't run zfs recv command:", err)
		return
	}
	defer stdin.Close()

	var response protocol.Response
	for {
		err = dec.Decode(&response)
		if err != nil {
			log.Println("decode error:", err)
			return
		}
		switch response.ResponseType {
		case protocol.ResponseZfsStream:
			data := response.DataChunk
			_, err := stdin.Write(data)
			if err != nil {
				log.Println("zfs recv write error:", err)
			}
		case protocol.ResponseDataEOF:
			stdin.Close()
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				line := scanner.Text()
				log.Println("stderr:", line)
			}
			if err := cmd.Wait(); err != nil {
				log.Println("zfs recv failed", err)
			}
			return
		case protocol.ResponseError:
			log.Println("remote error:", response.Error)
			return
		default:
			log.Println("unexpected response type:", response.ResponseType)
			return
		}
	}
}

func processIncrementalZfsSend(conf *config.Config, sourceDataset string, snapshot1 string, snapshot2 string) {
	//destinationDataset := util.DestinationDataset(conf.Storage, sourceDataset)
	//fmt.Println("incr", sourceDataset, destinationDataset, snapshot1, snapshot2)
	conn, err := tls.Dial("tcp", net.JoinHostPort(conf.Remote, strconv.Itoa(conf.Port)), conf.TlsConfig)
	if err != nil {
		log.Printf("tls.Dial() failed: %s", err)
		return
	}
	defer conn.Close()

	enc := gob.NewEncoder(flowrate.NewWriter(conn, conf.Bwlimit * 1024)) // Will write to network.
	dec := gob.NewDecoder(flowrate.NewReader(conn, conf.Bwlimit * 1024)) // Will read from network.

	var request protocol.Request
	request.RequestType = protocol.RequestIncrementalSnapshot
	request.DatasetName = sourceDataset
	request.Snapshot1Name = snapshot1
	request.Snapshot2Name = snapshot2
	err = enc.Encode(&request)
	if err != nil {
		log.Println("encode error:", err)
		return
	}

	//zfs recv -F -d tank/backup
	cmd := exec.Command("zfs", "recv", "-F", "-d", conf.Storage)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Println("can't run zfs recv command:", err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Println("can't run zfs recv command:", err)
		return
	}
	if err := cmd.Start(); err != nil {
		log.Println("can't run zfs recv command:", err)
		return
	}
	defer stdin.Close()

	var response protocol.Response
	for {
		err = dec.Decode(&response)
		if err != nil {
			log.Println("decode error:", err)
			return
		}
		switch response.ResponseType {
		case protocol.ResponseZfsStream:
			data := response.DataChunk
			_, err := stdin.Write(data)
			if err != nil {
				log.Println("zfs recv write error:", err)
			}
		case protocol.ResponseDataEOF:
			stdin.Close()
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				line := scanner.Text()
				log.Println("stderr:", line)
			}
			if err := cmd.Wait(); err != nil {
				log.Println("zfs recv failed", err)
			}
			return
		case protocol.ResponseError:
			log.Println("remote error:", response.Error)
			return
		default:
			log.Println("unexpected response type:", response.ResponseType)
			return
		}
	}
}
