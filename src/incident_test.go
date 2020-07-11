package main

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kafkaesque-io/pulsar-monitor/src/util"
)

func TestUnmarshConfigFile(t *testing.T) {
	ReadConfigFile("../config/runtime-template.json")
	assert(t, ":8081" == GetConfig().PrometheusConfig.Port, "load json config")
	ReadConfigFile("../config/runtime-template.yml")
	assert(t, ":8080" == GetConfig().PrometheusConfig.Port, "load yaml config")

}

func TestRandBytes(t *testing.T) {
	p := Payload{
		Ceiling: 8,
		Floor:   8,
	}
	assert(t, 8 == len(p.createPayload()), "fixed size payload")
	assert(t, string(p.createPayload()) != string(p.createPayload()), "payloads are random")

	p = Payload{
		Ceiling: 8000,
		Floor:   7000,
	}
	size := len(p.createPayload())
	assert(t, p.Ceiling > size && size > p.Floor, "variable size payload")

	payload := p.PrefixPayload("messageid:1290-")
	assert(t, "messageid:1290" == strings.Split(string(payload), "-")[0], "verify prefix in generated payload")

	p = Payload{
		Ceiling: 50 * 1000 * 1000,
		Floor:   44 * 1000 * 1000,
	}
	size = len(p.createPayload())
	fmt.Println(size)
	assert(t, 40*1000*1000 < size && size < p.Ceiling, "large size payload")

	prefix := "prefix"
	p.GenDefaultPayload()
	for i := 0; i < 10; i++ {
		assert(t, string(p.PrefixDefaultPayload(prefix)) == string(p.PrefixDefaultPayload(prefix)), "use default payload")
	}
}

func TestGenMultipleSamePayloadSize(t *testing.T) {

	// with single payload size specified with 3 messages
	msgs, _ := AllMsgPayloads("messageid", []string{"20B"}, 13)
	assert(t, 13 == len(msgs), "total messages")
	for i := 0; i < len(msgs); i++ {
		assert(t, 20 == len(msgs[i]), "individual message size")
		assert(t, i == GetMessageID("messageid", string(msgs[i])), "check message index")
	}
}

func TestGenMulitplDifferentPayloads(t *testing.T) {

	// with more payload size specified than the number of messages
	msgs, _ := AllMsgPayloads("aid", []string{"20B", "400B", "25B", "2B", "1KB"}, 3)
	assert(t, 5 == len(msgs), "total messages")
	for i := 0; i < len(msgs); i++ {
		assert(t, i == GetMessageID("aid", string(msgs[i])), "check message index")
	}
}

func TestGenSinglePayloads(t *testing.T) {

	// with single payload size specified with 3 messages
	msgs, _ := AllMsgPayloads("messageid", []string{"2KB"}, 0)
	assert(t, 1 == len(msgs), "total messages")
	assert(t, 2*1024 == len(msgs[0]), "individual message size")
	messageArray := strings.Split(string(msgs[0]), PrefixDelimiter)
	assert(t, "messageid" == messageArray[0], "check prefix")
	index, err := strconv.Atoi(messageArray[1])
	errNil(t, err)
	assert(t, index == 0, "check message index")
}

func TestGenDefaultSinglePayload(t *testing.T) {

	// no single payload size nor the number of message
	msgs, _ := AllMsgPayloads("yours", []string{}, 0)
	assert(t, 1 == len(msgs), "total messages")
	assert(t, len("yours-0-") == len(msgs[0]), "individual message size")
	messageArray := strings.Split(string(msgs[0]), PrefixDelimiter)
	assert(t, "yours" == messageArray[0], "check prefix")
	index, err := strconv.Atoi(messageArray[1])
	errNil(t, err)
	assert(t, index == 0, "check message index")
}

func TestGenMultipleDefaultPayloadSize(t *testing.T) {

	// no single payload size nor the number of message
	msgs, _ := AllMsgPayloads("your", []string{}, 1002)
	assert(t, 1002 == len(msgs), "total messages")
	for i := 0; i < len(msgs); i++ {
		assert(t, 12 > len(msgs[i]), "individual message size")
		messageArray := strings.Split(string(msgs[i]), PrefixDelimiter)
		assert(t, "your" == messageArray[0], "check prefix")
		index, err := strconv.Atoi(messageArray[1])
		errNil(t, err)
		assert(t, index == i, "check message index")
	}
}

func TestIncidentAlertPolicy(t *testing.T) {

	assert(t, util.StrContains([]string{"test", "foo"}, "foo"), "fail to eval container string")

	policy := AlertPolicyCfg{
		Ceiling:               20,
		MovingWindowSeconds:   30,
		CeilingInMovingWindow: 40,
	}

	// clear has no effect on no alert component
	ClearIncident("component1")
	assert(t, 0 == len(incidentTrackers), "")
	for i := 0; i < 19; i++ {
		assert(t, !trackIncident("component1", "time out message", "save me description", &policy), "")
		assert(t, !trackIncident("component2", "time out message", "save me description", &policy), "")
		assert(t, 2 == len(incidentTrackers), "")
		// clear will reset the counter
		ClearIncident("component2")
		assert(t, 1 == len(incidentTrackers), "")
	}
	assert(t, trackIncident("component1", "time out message", "save me description", &policy), "")
	assert(t, !trackIncident("component1", "time out message", "save me description", &policy), "")

	assert(t, 1 == len(incidentTrackers), "")
	for i := 0; i < 19; i++ {
		assert(t, !trackIncident("component2", "time out message", "save me description", &policy), "")
	}
	assert(t, trackIncident("component2", "time out message", "save me description", &policy), "")
	assert(t, !trackIncident("component2", "time out message", "save me description", &policy), "")
}

func TestIncidentAlertMovingWindowPolicy(t *testing.T) {

	policy := AlertPolicyCfg{
		Ceiling:               300,
		MovingWindowSeconds:   1,
		CeilingInMovingWindow: 3,
	}
	for i := 0; i < 5; i++ {
		assert(t, !trackIncident("component3", "time out message", "save me description", &policy), "")
		assert(t, !trackIncident("component3", "time out message", "save me description", &policy), "")
		// clear won't reset counter in moving window
		ClearIncident("component3")
		assert(t, trackIncident("component3", "time out message", "save me description", &policy), "")
	}

	assert(t, !trackIncident("component3", "time out message", "save me description", &policy), "")
	assert(t, !trackIncident("component3", "time out message", "save me description", &policy), "")
	time.Sleep(1 * time.Second)
	assert(t, !trackIncident("component3", "time out message", "save me description", &policy), "")
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// test if an err is not nil.
func errNil(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}
