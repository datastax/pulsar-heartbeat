//
//  Copyright (c) 2021 Datastax, Inc.
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

package cfg

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	log "github.com/apex/log"
	"github.com/hashicorp/go-retryablehttp"
)

const (
	ocmTrigger     = "trigger"
	ocmAcknowledge = "acknowledge"
	ocmResolve     = "resolve"
)

// IBMOCMEvent represents the event payload for IBM OCM webhook
type IBMOCMEvent struct {
	Action      string    `json:"action"` // trigger, acknowledge, or resolve
	Summary     string    `json:"summary"`
	Source      string    `json:"source"`
	Severity    string    `json:"severity"`
	Component   string    `json:"component"`
	Description string    `json:"description"`
	Timestamp   time.Time `json:"timestamp"`
	DedupKey    string    `json:"dedup_key"`
}

// IBMOCMWebhookResponse represents the response from IBM OCM webhook
type IBMOCMWebhookResponse struct {
	DeduplicationKey string `json:"deduplicationKey"` // IBM OCM's generated deduplication key
	EventID          string `json:"eventid"`          // IBM OCM's event ID
	Status           string `json:"status,omitempty"` // Optional status field
	Message          string `json:"message,omitempty"` // Optional message field
	Error            string `json:"error,omitempty"`  // error message if any
}

// IBMOCMEventDetails represents the event details from Events API
type IBMOCMEventDetails struct {
	DeduplicationKey string `json:"deduplicationKey"`
	InstanceUUID     string `json:"instanceUuid"`
	EventID          string `json:"eventid"`
	IncidentUUID     string `json:"incidentUuid"` // This is what we need for resolution
	EventState       string `json:"eventState"`
	Summary          string `json:"summary"`
	Severity         int    `json:"severity"`
}

// IBMOCMIncidentUpdate represents the payload for updating an incident
type IBMOCMIncidentUpdate struct {
	State string `json:"state"` // "resolved", "acknowledged", etc.
}

// validateWebhookURL validates the webhook URL format and security
func validateWebhookURL(webhookURL string) error {
	if webhookURL == "" {
		return nil // empty is valid (feature disabled)
	}

	// Validate URL format first
	parsedURL, err := url.Parse(webhookURL)
	if err != nil {
		return fmt.Errorf("invalid webhook URL format: %v", err)
	}

	// Ensure host is present
	if parsedURL.Host == "" {
		return fmt.Errorf("webhook URL must include a valid host")
	}

	// Enforce HTTPS for security (except localhost/127.0.0.1 for testing)
	isLocalhost := strings.HasPrefix(parsedURL.Host, "localhost:") ||
		strings.HasPrefix(parsedURL.Host, "127.0.0.1:")

	if !strings.HasPrefix(webhookURL, "https://") && !isLocalhost {
		return fmt.Errorf("webhook URL must use HTTPS, got: %s", webhookURL)
	}

	return nil
}

// CreateIBMOCMIncident creates an IBM OCM incident via webhook
// Returns error if creation fails
func CreateIBMOCMIncident(component, alias, msg, webhookURL, apiBaseURL, apiUser, apiPassword string) error {
	if webhookURL == "" {
		return nil
	}

	// Validate webhook URL
	if err := validateWebhookURL(webhookURL); err != nil {
		log.Errorf("IBM OCM webhook URL validation failed: %v", err)
		return err
	}

	event := IBMOCMEvent{
		Action:      ocmTrigger,
		Summary:     component + ": " + msg,
		Source:      "pulsar-heartbeat",
		Severity:    "critical",
		Component:   component,
		Description: msg,
		Timestamp:   time.Now(),
		DedupKey:    alias,
	}

	resp, err := sendIBMOCMWebhookEvent(webhookURL, &event)
	if err != nil {
		return err
	}

	if resp == nil {
		return errors.New("empty IBM OCM webhook response")
	}

	// Store the deduplicationKey and eventID for later resolution
	// We'll need these to query the Events API and get the incidentUuid
	incident := incidentRecord{
		requestID: resp.DeduplicationKey, // IBM OCM's dedup key
		alertID:   resp.EventID,          // IBM OCM's event ID (needed for Events API query)
		createdAt: time.Now(),
	}

	incidentsLock.Lock()
	defer incidentsLock.Unlock()
	incidents[component] = incident

	log.Infof("IBM OCM incident created for %s with deduplicationKey: %s, eventID: %s",
		component, resp.DeduplicationKey, resp.EventID)
	return nil
}

// ResolveIBMOCMIncident resolves an IBM OCM incident using the 3-step process:
// 1. We have deduplicationKey and eventID from incident creation
// 2. Query Events API to get incidentUuid
// 3. Call Incident Management API to resolve the incident
func ResolveIBMOCMIncident(component, dedupKey, eventID, apiBaseURL, apiUser, apiPassword string) error {
	if apiBaseURL == "" || apiUser == "" || apiPassword == "" {
		log.Warnf("IBM OCM API credentials not configured, skipping resolution for %s", component)
		return nil
	}

	// Step 1: Query Events API to get incidentUuid
	incidentUUID, err := getIBMOCMIncidentUUID(dedupKey, eventID, apiBaseURL, apiUser, apiPassword)
	if err != nil {
		log.Errorf("Failed to get incident UUID for %s: %v", component, err)
		return err
	}

	if incidentUUID == "" {
		return fmt.Errorf("no incident UUID found for component %s", component)
	}

	// Step 2: Resolve the incident using Incident Management API
	err = resolveIBMOCMIncidentByUUID(incidentUUID, apiBaseURL, apiUser, apiPassword)
	if err != nil {
		log.Errorf("Failed to resolve incident %s for %s: %v", incidentUUID, component, err)
		return err
	}

	log.Infof("IBM OCM incident resolved for %s (incidentUuid: %s)", component, incidentUUID)
	return nil
}

// getIBMOCMIncidentUUID queries the Events API to get the incident UUID
func getIBMOCMIncidentUUID(dedupKey, eventID, apiBaseURL, apiUser, apiPassword string) (string, error) {
	// Build the Events API URL with query parameters
	eventsURL := fmt.Sprintf("%s/api/events/v1?deduplicationKey=%s&eventid=%s",
		strings.TrimSuffix(apiBaseURL, "/"), dedupKey, eventID)

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(10) * time.Second
	client.RetryWaitMin = 2 * time.Second
	client.RetryWaitMax = 30 * time.Second
	client.RetryMax = 3

	req, err := retryablehttp.NewRequest(http.MethodGet, eventsURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create Events API request: %v", err)
	}

	// Set Basic Auth header
	auth := base64.StdEncoding.EncodeToString([]byte(apiUser + ":" + apiPassword))
	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to query Events API: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("Events API returned status code %d", resp.StatusCode)
	}

	var eventDetails IBMOCMEventDetails
	if err := json.NewDecoder(resp.Body).Decode(&eventDetails); err != nil {
		return "", fmt.Errorf("failed to decode Events API response: %v", err)
	}

	if eventDetails.IncidentUUID == "" {
		return "", fmt.Errorf("no incidentUuid in Events API response")
	}

	log.Infof("Retrieved incident UUID: %s for deduplicationKey: %s", eventDetails.IncidentUUID, dedupKey)
	return eventDetails.IncidentUUID, nil
}

// resolveIBMOCMIncidentByUUID resolves an incident using the Incident Management API
func resolveIBMOCMIncidentByUUID(incidentUUID, apiBaseURL, apiUser, apiPassword string) error {
	// Build the Incident Management API URL
	incidentURL := fmt.Sprintf("%s/api/incimgmt/v1/%s",
		strings.TrimSuffix(apiBaseURL, "/"), incidentUUID)

	updatePayload := IBMOCMIncidentUpdate{
		State: "resolved",
	}

	payload, err := json.Marshal(updatePayload)
	if err != nil {
		return fmt.Errorf("failed to marshal incident update: %v", err)
	}

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(10) * time.Second
	client.RetryWaitMin = 2 * time.Second
	client.RetryWaitMax = 30 * time.Second
	client.RetryMax = 3

	req, err := retryablehttp.NewRequest(http.MethodPost, incidentURL, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create Incident Management API request: %v", err)
	}

	// Set Basic Auth header
	auth := base64.StdEncoding.EncodeToString([]byte(apiUser + ":" + apiPassword))
	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call Incident Management API: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Incident Management API returned status code %d", resp.StatusCode)
	}

	log.Infof("Successfully resolved incident %s via Incident Management API", incidentUUID)
	return nil
}

// sendIBMOCMWebhookEvent sends an event to IBM OCM webhook
func sendIBMOCMWebhookEvent(webhookURL string, event *IBMOCMEvent) (*IBMOCMWebhookResponse, error) {
	if webhookURL == "" {
		return nil, nil
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Errorf("failed to marshal IBM OCM event: %v", err)
		return nil, err
	}

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(10) * time.Second
	client.RetryWaitMin = 2 * time.Second
	client.RetryWaitMax = 30 * time.Second
	client.RetryMax = 3

	req, err := retryablehttp.NewRequest(http.MethodPost, webhookURL, bytes.NewBuffer(payload))
	if err != nil {
		log.Errorf("failed to create IBM OCM request: %v", err)
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("failed to send event to IBM OCM: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("IBM OCM webhook returned status code %d", resp.StatusCode)
	}

	var ocmResp IBMOCMWebhookResponse
	if err := json.NewDecoder(resp.Body).Decode(&ocmResp); err != nil {
		// Some webhooks may not return JSON, so we'll just log success
		log.Infof("IBM OCM event sent successfully to %s", webhookURL)
		return &IBMOCMWebhookResponse{
			DeduplicationKey: "unknown",
			EventID:          "unknown",
		}, nil
	}

	log.Infof("IBM OCM event sent - deduplicationKey: %s, eventID: %s",
		ocmResp.DeduplicationKey, ocmResp.EventID)
	return &ocmResp, nil
}
