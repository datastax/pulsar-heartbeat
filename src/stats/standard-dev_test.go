package stats

import (
	"fmt"
	"testing"
)

func TestStandardDev(t *testing.T) {
	std := NewStandardDeviation("Test")
	std.Push(3)
	std.Push(5)
	std.Push(9)
	std.Push(1)
	std.Push(8)
	std.Push(6)
	std.Push(58)
	std.Push(9)
	std.Push(4)
	sd, _, within2Std := std.Push(10)

	fmt.Printf("standard dev is %f, expected standard devu %s, %f", sd, fmt.Sprintf("%.4f", sd), std.Mean)
	if fmt.Sprintf("%.4f", sd) != "15.8117" {
		t.FailNow()
	}

	if !within2Std {
		t.FailNow()
	}
}
