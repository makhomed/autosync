package protocol

/*

request: get datasets list
reply: list of all datasets

request: get snapshots list for dataset
reply: list of all snapshots for dataset

request: get full snapshot
reply: send binary stream

request: get incremental snapshot
reply: send binary stream

*/

const (
	RequestDatasets = 1
	ResponseDatasets = 2

	RequestSnapshots = 3
	ResponseSnapshots = 4

	RequestFullSnapshot = 5
	RequestIncrementalSnapshot = 7
	ResponseZfsStream = 9

	ResponseDataEOF = 254
	ResponseError = 255
)

type Request struct {
	RequestType   byte
	DatasetName   string
	Snapshot1Name string
	Snapshot2Name string
}

type Response struct {
	ResponseType byte
	Datasets     []string
	Snapshots    []string
	DataChunk    []byte
	Error        string
}
