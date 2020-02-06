package main

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestIncidentAlertPolicy(t *testing.T) {

	assert(t, StrContains([]string{"test", "foo"}, "foo"), "fail to eval container string")

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
