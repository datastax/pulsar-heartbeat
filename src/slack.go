package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"
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
	log.Println("error ", msg)
	err := SendSlackNotification(GetConfig().SlackConfig.AlertURL, SlackMessage{
		Text: msg,
	})
	if err != nil {
		log.Println("slack error ", err)
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
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if buf.String() != "ok" {
		log.Println(buf.String())
		return errors.New("Non-ok response returned from Slack")
	}
	return nil
}
