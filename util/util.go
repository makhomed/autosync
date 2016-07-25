package util

import (
	"config"
	"sort"
	"strings"
	"fmt"
	"path"
	"os"
)

func FilterDatasets(conf *config.Config, datasets []string) []string {
	result := make([]string, 0)
	for _, dataset := range datasets {
		if conf.Included(dataset) {
			result = append(result, dataset)
		}
	}
	sort.Strings(result)
	return result
}

func DestinationDataset(storage string, sourceDataset string) string {
	pos := strings.Index(sourceDataset, "/")
	if pos == -1 {
		panic(fmt.Sprintf("unexpected dataset name: '%s'", sourceDataset))
	}
	return path.Join(storage, sourceDataset[pos+1:])
}

func IntersectionOfSnapshots(sourceSnapshots []string, destinationSnapshots map[string]bool) []string {
	result := make([]string, 0)
	for _, sourceSnapshot := range sourceSnapshots {
		if _, ok := destinationSnapshots[sourceSnapshot]; ok {
			result = append(result, sourceSnapshot)
		}
	}
	return result
}

func Suffix(snapshot string) string {
	pos := strings.LastIndex(snapshot, ".")
	if pos == -1 {
		panic(fmt.Sprintf("unexpected snapshot name: '%s'", snapshot))
	}
	return snapshot[pos+1:]
}

func SourceSnapshotForFullZfsSend(sourceSnapshots []string) string {
	sort.Strings(sourceSnapshots)
	result := sourceSnapshots[0]
	suffix := Suffix(result)
	for _, line := range sourceSnapshots {
		if Suffix(line) == suffix {
			result = line
		}
	}
	return result
}

func Verbose() bool {
	if _, err := os.Stat("/opt/autosync/log/v"); os.IsNotExist(err) {
		return false
	}
	return true
}
