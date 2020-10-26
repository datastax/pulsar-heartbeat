package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/apex/log"
)

// SlackMessage is the message struct to be posted for Slack
type SlackMessage struct {
	Channel   string `json:"channel"`
	Text      string `json:"text"`
	Username  string `json:"username"`
	IconEmogi string `json:"icon_emogi"`
}

// Alert alerts to slack, email, text.
func Alert(msg string) {
	log.Errorf("Alert %s", msg)
	if GetConfig().SlackConfig.AlertURL == "" {
		return
	}
	err := SendSlackNotification(GetConfig().SlackConfig.AlertURL, SlackMessage{
		Text: msg,
	})
	if err != nil {
		log.Errorf("slack error %v", err)
	}
}

// SendSlackNotification will post to an 'Incoming Webook' url setup in Slack Apps. It accepts
// some text and the slack channel is saved within Slack.
func SendSlackNotification(webhookURL string, msg SlackMessage) error {
	slackBody, _ := json.Marshal(msg)
	req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewBuffer(slackBody))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if buf.String() != "ok" {
		return fmt.Errorf("Non-ok response returned from Slack, message %s", buf.String())
	}
	return nil
}
