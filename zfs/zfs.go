package zfs

import (
	"strings"
	"bufio"
	"bytes"
	"os/exec"
	"config"
	"protocol"
	"sort"
)

func getAllDatasets() (map[string]bool, error) {
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
	datasets, err := getAllDatasets()
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
	sort.Strings(result)
	response.ResponseType = protocol.ResponseDatasets
	response.Datasets = result
	return response
}

func GetDestinationDatasets(prefix string) (map[string]bool, error) {
	datasets, err := getAllDatasets()
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool)
	for dataset := range datasets {
		if strings.HasPrefix(dataset, prefix) {
			result[dataset] = true
		}
	}
	return result, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetSnapshots(forDataset string) (map[string]bool, error) {
	snapshots := make(map[string]bool)
	// zfs list -H -p -o name -t snap
	cmd := exec.Command("zfs", "list", "-H", "-p", "-o", "name", "-t", "snap")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(output)
	scanner := bufio.NewScanner(buffer)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		split := strings.SplitN(line, "@", 2)
		dataset, snapshot := split[0], split[1]
		if !strings.HasPrefix(snapshot, "autosnap") {
			continue
		}
		if dataset != forDataset {
			continue
		}
		snapshots[snapshot] = true
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func GetResponseSnapshots(conf *config.Config, forDataset string) protocol.Response {
	response := protocol.Response{}
	snapshots, err := GetSnapshots(forDataset)
	if err != nil {
		response.ResponseType = protocol.ResponseError
		response.Error = err.Error()
		return response
	}
	result := make([]string, 0)
	for snapshot := range snapshots {
		result = append(result, snapshot)
	}
	sort.Strings(result)
	response.ResponseType = protocol.ResponseSnapshots
	response.Snapshots = result
	return response
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

