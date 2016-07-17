package util

import "testing"

func TestDestinationDataset(t *testing.T) {
	if DestinationDataset("tank/storage","tank/101") != "tank/storage/101" {
		t.Error(1)
	}
	if DestinationDataset("tank/storage/","tank/102/raw") != "tank/storage/102/raw" {
		t.Error(2)
	}
}

func TestSuffix(t *testing.T) {
	if Suffix("autosnap.2016-07-16.17:24:14.daily") != "daily" {
		t.Error(3)
	}
}