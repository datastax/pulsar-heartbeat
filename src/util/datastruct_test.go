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

package util

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestSyncMap(t *testing.T) {
	syncMap := NewSycMap()

	if !syncMap.IsEmpty() {
		t.FailNow()
	}

	syncMap.Put("test", "value0")
	if syncMap.Size() != 1 {
		t.FailNow()
	}
	syncMap.Put("test", "value1")
	syncMap.Put("test2", "value2")
	syncMap.Put("test3", "value3")

	if syncMap.Get("test3") != "value3" {
		t.Fatal("expect key test3 value ")
	}
	if syncMap.Get("test") != "value1" {
		t.Fatal("expect key test value is value1")
	}
	if syncMap.Get("test4") != nil {
		t.Fatal("expect no test4 key")
	}

	type finish struct{}
	it := 1000
	finishSig := make(chan *finish, it)
	go func() {
		for k := range [1000]int{1} {
			go func(i int) {
				syncMap.Put("test"+strconv.Itoa(i), "value")
				finishSig <- &finish{}
			}(k)
		}
	}()
	if syncMap.Size() > it {
		t.Fatal("finish too quick")
	}

	ticker := time.NewTicker(4 * time.Second)
	finalCount := 0
	for finalCount != it {
		select {
		case <-finishSig:
			finalCount++
			fmt.Printf("%d\n", finalCount)
		case <-ticker.C:
			t.Fatal("time out wait for put() loop to complete")
		}
	}
	if syncMap.Size() != it+1 {
		t.Fatalf("expect size to be %d", it+1)
	}

	if syncMap.Get("test345") != "value" {
		t.Fatal("expect key test3 value ")
	}
	if syncMap.Get("test") != "value1" {
		t.Fatal("expect key test value is value1")
	}

	if syncMap.Replace("test", "value2") != "value1" {
		t.Fatal("Replace expect key test previous value is value1")
	}
	if syncMap.Get("test") != "value2" {
		t.Fatal("Replace expect key test new value is value2")
	}
}
