package zfs

import (
	"strings"
	"bufio"
	"bytes"
	"os/exec"
	"config"
	"protocol"
)

func getDatasets() (map[string]bool, error) {
	datasets := make(map[string]bool)
	cmd := exec.Command("zfs", "list", "-H", "-o", "name")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(output)
	scanner := bufio.NewScanner(buffer)
	for scanner.Scan() {
		dataset := scanner.Text()
		dataset = strings.TrimSpace(dataset)
		if dataset == "" {
			continue
		}
		datasets[dataset] = true
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return datasets, nil
}

func GetResponseDatasets(conf *config.Config) protocol.Response {
	response := protocol.Response{}
	datasets, err := getDatasets()
	if err != nil {
		response.ResponseType = protocol.ResponseError
		response.Error = err.Error()
		return response
	}
	result := make([]string, 0)
	for dataset := range datasets {
		if conf.Included(dataset) {
			result = append(result, dataset)
		}
	}
	response.ResponseType = protocol.ResponseDatasets
	response.Datasets = result
	return response
}