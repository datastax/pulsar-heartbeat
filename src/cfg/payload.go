package cfg

import (
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kafkaesque-io/pulsar-monitor/src/util"
)

// manage the payload size of Pulsar message

const (
	// PrefixDelimiter for message prefix
	PrefixDelimiter = "-"
)

// Payload defines the payload size
type Payload struct {
	Ceiling        int
	Floor          int
	DefaultPayload []byte // to save time for large payload size generation
}

// GenDefaultPayload generates default payload size
func (p *Payload) GenDefaultPayload() []byte {
	p.DefaultPayload = p.createPayload()
	return p.DefaultPayload
}

func (p Payload) createPayload() []byte {
	size := randRange(p.Ceiling, p.Floor)
	//use random to make compress impossible
	return util.RandStringBytes(size)
}

// PrefixPayload creates string prefix in the payload
func (p Payload) PrefixPayload(prefix string) []byte {
	return append([]byte(prefix), p.createPayload()...)
}

// PrefixDefaultPayload creates string prefix in the payload
func (p Payload) PrefixDefaultPayload(prefix string) []byte {
	return append([]byte(prefix), p.DefaultPayload...)
}

func randRange(ceiling, floor int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(ceiling-floor+1) + floor
}

// GenPayload generates an array of bytes with prefix string
// and payload size. If the specified payload size is less than
// the prefix size, the payload will just be the prefix.
func GenPayload(prefix, size string) ([]byte, int) {
	numOfBytes := NumOfBytes(size)
	if len(prefix) > numOfBytes {
		return []byte(prefix), numOfBytes
	}

	numOfBytes = numOfBytes - len(prefix)
	p := NewPayload(numOfBytes)

	return p.PrefixDefaultPayload(prefix), numOfBytes
}

// NewPayload returns a new Payload object with a fixed payload size
func NewPayload(size int) Payload {
	p := Payload{
		Ceiling: size,
		Floor:   size,
	}
	p.GenDefaultPayload()
	return p
}

// NumOfBytes returns a number of bytes with specified size in MB or KB
func NumOfBytes(size string) int {
	unitRegex, _ := regexp.Compile("[a-zA-Z]+")
	numRegex, _ := regexp.Compile("[0-9]+")

	num := unitRegex.ReplaceAllString(size, "")
	unit := numRegex.ReplaceAllString(size, "")

	bytes, err := strconv.Atoi(num)
	if err != nil {
		return 0
	}

	switch strings.ToLower(unit) {
	case "mb", "megabytes", "megabyte", "megab":
		return bytes * 1024 * 1024
	case "kb", "kilobytes", "kilobyte", "kilob":
		return bytes * 1024
	default:
		return bytes
	}
}

// AllMsgPayloads generates a series of payloads based on
// specified payload sizes or the number of messages
func AllMsgPayloads(prefix string, payloadSizes []string, numOfMsg int) ([][]byte, int) {
	maxPayloadSize := len(prefix)
	actualNumOfMsg := 1 //default minimun one message
	specifiedSizes := len(payloadSizes)
	if specifiedSizes > numOfMsg {
		actualNumOfMsg = specifiedSizes
	} else if numOfMsg > actualNumOfMsg {
		actualNumOfMsg = numOfMsg
	}

	if specifiedSizes == 0 {
		payloadSizes = append(payloadSizes, "0")
		specifiedSizes = 1
	}

	// for large number of messages we keep a map of payload to speed up random payload generation
	// payloadTemplates := make(map[string]Payload)

	payloads := make([][]byte, actualNumOfMsg)

	for i := 0; i < actualNumOfMsg; i++ {
		specifiedIndex := specifiedSizes - 1
		if i < specifiedSizes {
			specifiedIndex = i
		}

		pre := fmt.Sprintf("%s-%d-", prefix, i)
		size := 0
		payloads[i], size = GenPayload(pre, payloadSizes[specifiedIndex])
		maxPayloadSize = int(math.Max(float64(maxPayloadSize), float64(size)))

	}

	return payloads, maxPayloadSize
}

// GetMessageID returns the message index by parsing the template payload string with a prefix.
func GetMessageID(prefix, str string) int {
	parts := strings.Split(string(str), PrefixDelimiter)
	if prefix != parts[0] {
		return -1
	}
	index, err := strconv.Atoi(parts[1])
	if err != nil {
		return -2
	}
	return index
}
