 //
 //  Copyright (c) 2020-2021 Datastax, Inc.
 //  
 //  Licensed to the Apache Software Foundation (ASF) under one
 //  or more contributor license agreements.  See the NOTICE file
 //  distributed with this work for additional information
 //  regarding copyright ownership.  The ASF licenses this file
 //  to you under the Apache License, Version 2.0 (the
 //  "License"); you may not use this file except in compliance
 //  with the License.  You may obtain a copy of the License at
 //  
 //     http://www.apache.org/licenses/LICENSE-2.0
 //  
 //  Unless required by applicable law or agreed to in writing,
 //  software distributed under the License is distributed on an
 //  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 //  KIND, either express or implied.  See the License for the
 //  specific language governing permissions and limitations
 //  under the License.
 //

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
	sd, _, within3Std := std.Push(10)

	fmt.Printf("standard dev is %f, expected standard devu %s, %f", sd, fmt.Sprintf("%.4f", sd), std.Mean)
	if fmt.Sprintf("%.4f", sd) != "15.8117" {
		t.FailNow()
	}

	if !within3Std {
		t.FailNow()
	}
}

func Test0StandardDev(t *testing.T) {
	std := NewStandardDeviation("Test")
	std.Push(2)
	for i := 0; i < 20; i++ {
		std.Push(2)
	}
	stdev, _, within3Std := std.Push(2)

	if stdev != 0 {
		t.FailNow()
	}

	if within3Std {
		t.FailNow()
	}
}
