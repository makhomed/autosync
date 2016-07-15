package util

import (
	"config"
	"sort"
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
