package healthchecker

import (
	"bytes"
	_ "embed"
)

//go:embed test.wav
var testWav []byte

func readTestFile() *bytes.Buffer {
	return bytes.NewBuffer(testWav)
}
