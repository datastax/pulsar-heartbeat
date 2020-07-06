package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kafkaesque-io/pulsar-monitor/src/stats"
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

// AssignString returns the first non-empty string
// It is equivalent the following in Javascript
// var value = val0 || val1 || val2 || default
func AssignString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
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
