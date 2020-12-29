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

import "sync"

// SyncMap is a generic map data structure protected with syc.RWMutex
type SyncMap struct {
	sync.RWMutex
	data map[interface{}]interface{}
}

// NewSycMap creates a new SyncMap
func NewSycMap() *SyncMap {
	return &SyncMap{
		data: make(map[interface{}]interface{}),
	}
}

// Put associates the specified value with the specified key in this map
func (sm *SyncMap) Put(key interface{}, value interface{}) interface{} {
	sm.Lock()
	defer sm.Unlock()

	sm.data[key] = value
	return sm.data[key]
}

// Replace puts a key and value pair and returns a previous value if exists,
func (sm *SyncMap) Replace(key interface{}, value interface{}) interface{} {
	sm.Lock()
	defer sm.Unlock()
	previous := sm.data[key]
	sm.data[key] = value
	return previous
}

// Get returns the value to which the specified key is mapped, or nil if this map contains no mapping for the key.
func (sm *SyncMap) Get(key interface{}) interface{} {
	sm.RLock()
	defer sm.RUnlock()

	return sm.data[key]
}

// GetOrDefault returns the value to which the specified key is mapped, or nil if this map contains no mapping for the key.
func (sm *SyncMap) GetOrDefault(key interface{}, defaultValue interface{}) interface{} {
	sm.RLock()
	defer sm.RUnlock()

	if value := sm.data[key]; value != nil {
		return value
	}
	return defaultValue
}

// Size returns the size of the map
func (sm *SyncMap) Size() int {
	sm.RLock()
	defer sm.RUnlock()
	return len(sm.data)
}

// IsEmpty returns whether the map is empty
func (sm *SyncMap) IsEmpty() bool {
	if sm.Size() == 0 {
		return true
	}
	return false
}

// Remove removes the specified key and associsated value
func (sm *SyncMap) Remove(key interface{}) {
	sm.Lock()
	defer sm.Unlock()

	delete(sm.data, key)
}

// TODO: add these functions
// Clear()
