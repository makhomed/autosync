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
	"os/exec"
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
		case protocol.RequestFullSnapshot:
			processRequestFullSnapshot(conf, enc, dec, request)

		case protocol.RequestIncrementalSnapshot:
			processRequestIncrementalSnapshot(conf, enc, dec, request)
		default:
			log.Println("unknown request type '%d'", request.RequestType)
			return
		}
	}
}

const BufferLen = 2 * 1024 * 1024 // 2 MiB

func processRequestFullSnapshot(conf *config.Config, enc *gob.Encoder, dec *gob.Decoder, request protocol.Request) {
	dataset := request.DatasetName
	snapshot := request.Snapshot1Name
	fullSnapshotName := dataset + "@" + snapshot

	//zfs send tank/101@autosnap.2016-07-16.17:24:14.daily
	cmd := exec.Command("zfs", "send", "-p", fullSnapshotName)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("can't run zfs send command:", err)
		return
	}
	if err := cmd.Start(); err != nil {
		log.Println("can't run zfs send command:", err)
		return
	}
	defer stdout.Close()

	buffer := make([]byte, BufferLen)
	var response protocol.Response
	for {
		n, err := stdout.Read(buffer)
		if n == 0 && err == io.EOF {
			response.ResponseType = protocol.ResponseDataEOF
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
			}
			stdout.Close()
			if err := cmd.Wait(); err != nil {
				log.Println("zfs send failed", err)
			}
			return
		}
		if n == BufferLen {
			response.ResponseType = protocol.ResponseZfsStream
			response.DataChunk = buffer
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
				return
			}
		}
		if n > 0 && n < BufferLen {
			response.ResponseType = protocol.ResponseZfsStream
			response.DataChunk = buffer[:n]
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
				return
			}
		}
		if err != nil && err != io.EOF {
			log.Println("read error:", err)
		}
	}
}

func processRequestIncrementalSnapshot(conf *config.Config, enc *gob.Encoder, dec *gob.Decoder, request protocol.Request) {
	dataset := request.DatasetName
	snapshot1 := request.Snapshot1Name
	snapshot2 := request.Snapshot2Name
	fullSnapshot1Name := "@" + snapshot1
	fullSnapshot2Name := dataset + "@" + snapshot2

	//zfs send tank/101@autosnap.2016-07-16.17:24:14.daily
	cmd := exec.Command("zfs", "send", "-p", "-I", fullSnapshot1Name, fullSnapshot2Name)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("can't run zfs send command:", err)
		return
	}
	if err := cmd.Start(); err != nil {
		log.Println("can't run zfs send command:", err)
		return
	}
	defer stdout.Close()

	buffer := make([]byte, BufferLen)
	var response protocol.Response
	for {
		n, err := stdout.Read(buffer)
		if n == 0 && err == io.EOF {
			response.ResponseType = protocol.ResponseDataEOF
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
			}
			stdout.Close()
			if err := cmd.Wait(); err != nil {
				log.Println("zfs send failed", err)
			}
			return
		}
		if n == BufferLen {
			response.ResponseType = protocol.ResponseZfsStream
			response.DataChunk = buffer
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
				return
			}
		}
		if n > 0 && n < BufferLen {
			response.ResponseType = protocol.ResponseZfsStream
			response.DataChunk = buffer[:n]
			err = enc.Encode(&response)
			if err != nil {
				log.Println("encode error:", err)
				return
			}
		}
		if err != nil && err != io.EOF {
			log.Println("read error:", err)
		}
	}
}