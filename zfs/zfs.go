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
	sort.Strings(result)
	response.ResponseType = protocol.ResponseDatasets
	response.Datasets = result
	return response
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func getSnapshots(forDataset string) ([]string, error) {
	snapshots := make([]string, 0)
	// zfs list -H -p -o name -t snap
	cmd := exec.Command("zfs", "list", "-H", "-p", "-o", "name", "-t", "snap")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(output)
	scanner := bufio.NewScanner(buffer)
	for scanner.Scan() {
		snapshot := scanner.Text()
		snapshot = strings.TrimSpace(snapshot)
		split := strings.SplitN(snapshot, "@", 2)
		dataset, rest := split[0], split[1]
		if !strings.HasPrefix(rest, "autosnap") {
			continue
		}
		if dataset != forDataset {
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	sort.Strings(snapshots)
	return snapshots, nil
}

func GetResponseSnapshots(conf *config.Config, forDataset string) protocol.Response {
	response := protocol.Response{}
	snapshots, err := getSnapshots(forDataset)
	if err != nil {
		response.ResponseType = protocol.ResponseError
		response.Error = err.Error()
		return response
	}
	response.ResponseType = protocol.ResponseSnapshots
	response.Snapshots = snapshots
	return response
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

