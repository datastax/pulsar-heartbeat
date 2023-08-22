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
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/datastax/pulsar-heartbeat/src/stats"
)

var (
	// used to generate random payload size
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	// key is the cluster name
	standardDeviationStore = make(map[string]*stats.StandardDeviation)
)

// ResponseErr - Error struct for Http response
type ResponseErr struct {
	Error string `json:"error"`
}

// JoinString joins multiple strings
func JoinString(strs ...string) string {
	var sb strings.Builder
	for _, str := range strs {
		sb.WriteString(str)
	}
	return sb.String()
}

// ResponseErrorJSON builds a Http response.
func ResponseErrorJSON(e error, w http.ResponseWriter, statusCode int) {
	response := ResponseErr{e.Error()}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(jsonResponse)
}

// ReceiverHeader parses headers for Pulsar required configuration
func ReceiverHeader(h *http.Header) (token, topicFN, pulsarURL string, err bool) {
	token = strings.TrimSpace(strings.Replace(h.Get("Authorization"), "Bearer", "", 1))
	topicFN = h.Get("TopicFn")
	pulsarURL = h.Get("PulsarUrl")
	return token, topicFN, pulsarURL, token == "" || topicFN == ""

}

// FirstNonEmptyString returns the first non-empty string
// It is equivalent the following in Javascript
// var value = val0 || val1 || val2 || default
func FirstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func TokenSupplierWithOverride(token string, supplier func() (string, error)) func() (string, error) {
	if token != "" {
		return func() (string, error) {
			return token, nil
		}
	}
	return supplier
}

// ReportError logs error
func ReportError(err error) error {
	log.Printf("error %v", err)
	return err
}

// ReadFile reads /etc/lsd_release
// Similar to Read(), but takes the name of a file to load instead
func ReadFile(filename string) (osrelease map[string]string, err error) {
	osrelease = make(map[string]string)

	lines, err := parseFile(filename)
	if err != nil {
		return
	}

	for _, v := range lines {
		key, value, err := parseLine(v)
		if err == nil {
			osrelease[key] = value
		}
	}
	return
}

func parseFile(filename string) (lines []string, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func parseLine(line string) (key string, value string, err error) {
	err = nil

	// skip empty lines
	if len(line) == 0 {
		return "", "", errors.New("Skipping: zero-length")
	}

	// skip comments
	if line[0] == '#' {
		err = errors.New("Skipping: comment")
		return
	}

	// try to split string at the first '='
	splitString := strings.SplitN(line, "=", 2)
	if len(splitString) != 2 {
		err = errors.New("Can not extract key=value")
		return
	}

	// trim white space from key and value
	key = splitString[0]
	key = strings.Trim(key, " ")
	value = splitString[1]
	value = strings.Trim(value, " ")

	// Handle double quotes
	if strings.ContainsAny(value, `"`) {
		first := string(value[0:1])
		last := string(value[len(value)-1:])

		if first == last && strings.ContainsAny(first, `"'`) {
			value = strings.TrimPrefix(value, `'`)
			value = strings.TrimPrefix(value, `"`)
			value = strings.TrimSuffix(value, `'`)
			value = strings.TrimSuffix(value, `"`)
		}
	}

	// expand anything else that could be escaped
	value = strings.Replace(value, `\"`, `"`, -1)
	value = strings.Replace(value, `\$`, `$`, -1)
	value = strings.Replace(value, `\\`, `\`, -1)
	value = strings.Replace(value, "\\`", "`", -1)
	return
}

// TimeDuration evaluate the run interval with the guard default value
func TimeDuration(configV, defaultV int, timeUnit time.Duration) time.Duration {
	if configV == 0 {
		return time.Duration(defaultV) * timeUnit
	}
	return time.Duration(configV) * timeUnit

}

// StrContains check if a string is contained in an array of string
func StrContains(strs []string, str string) bool {
	for _, v := range strs {
		if v == str {
			return true
		}
	}
	return false
}

// Trim trims leading/prefix and suffix spaces, tab, \n in a string
func Trim(str string) string {
	return strings.TrimSpace(str)
}

// RandStringBytes generates n length byte array with each byte is randomly picked
// from lower or upper case letters
func RandStringBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {

		q := rand.Intn(len(letters))
		b[i] = letters[q]
	}
	return b
}

// SingleSlashJoin joins two parts of url path with no double slash
func SingleSlashJoin(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// StrToInt converts string to an integer format with a default if inproper value retrieved
func StrToInt(str string, defaultNum int) int {
	if i, err := strconv.Atoi(str); err == nil {
		return i
	}
	return defaultNum
}

// GetStdBucket gets the standard deviation bucket
func GetStdBucket(key string) *stats.StandardDeviation {
	stdVerdict, ok := standardDeviationStore[key]
	if !ok {
		std := stats.NewStandardDeviation(key)
		standardDeviationStore[key] = &std
		return &std
	}
	return stdVerdict
}

// TokenizeTopicFullName tokenizes a topic full name into persistent, tenant, namespace, and topic name.
func TokenizeTopicFullName(topicFn string) (isPersistent bool, tenant, namespace, topic string, err error) {
	var topicRoute string
	if strings.HasPrefix(topicFn, "persistent://") {
		topicRoute = strings.Replace(topicFn, "persistent://", "", 1)
		isPersistent = true
	} else if strings.HasPrefix(topicFn, "non-persistent://") {
		topicRoute = strings.Replace(topicFn, "non-persistent://", "", 1)
	} else {
		return false, "", "", "", fmt.Errorf("invalid persistent or non-persistent part")
	}

	parts := strings.Split(topicRoute, "/")
	if len(parts) == 3 {
		return isPersistent, parts[0], parts[1], parts[2], nil
	} else if len(parts) == 2 {
		return isPersistent, parts[0], parts[1], "", nil
	} else {
		return false, "", "", "", fmt.Errorf("missing tenant, namespace, or topic name")
	}
}

// PreserveHeaderForRedirect preserves HTTP headers during HTTP redirect
func PreserveHeaderForRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 50 {
		return fmt.Errorf("too many redirects in GET CachedProxy")
	}
	if len(via) == 0 {
		return nil
	}
	for attr, val := range via[0].Header {
		if _, ok := req.Header[attr]; !ok {
			req.Header[attr] = val
		}
	}
	return nil
}

// MinInt returns a smaller integer of two integers
func MinInt(a, b int) int {
	if a >= b {
		return b
	}
	return a
}

// TopicFnToURL converts fully qualified topic name to url route
func TopicFnToURL(topicFn string) (string, error) {
	// non-persistent://tenant/namesapce/topic
	persistentParts := strings.Split(topicFn, "://")
	if len(persistentParts) != 2 {
		return "", fmt.Errorf("invalid topic full name pattern")
	}

	parts := strings.Split(persistentParts[1], "/")
	if len(parts) != 3 {
		return "", fmt.Errorf("missing tenant or namespace or topic")
	}

	return strings.ReplaceAll(topicFn, "://", "/"), nil
}

// ComputeDelta computes positive delta between last and current integer value, returns a default if the delta is negative
func ComputeDelta(last, current, defaultValue uint64) uint64 {
	if last >= current {
		return defaultValue
	}
	return current - last
}
