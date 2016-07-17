package util

import "testing"

func TestDestinationDataset(t *testing.T) {
	if DestinationDataset("tank/storage","tank/101") != "tank/storage/101" {
		t.Error("1")
	}
	if DestinationDataset("tank/storage/","tank/102/raw") != "tank/storage/102/raw" {
		t.Error("2")
	}
}
