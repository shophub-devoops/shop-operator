/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// discordAPIBase is the Discord REST API base URL (v10 is the current stable).
const discordAPIBase = "https://discord.com/api/v10"

// channelTypeText is Discord's enum value for GUILD_TEXT channels.
const channelTypeText = 0

// discordClient is a minimal HTTP client for the few Discord API endpoints we
// need: create/delete channel, create webhook. Token is sent in the
// Authorization header as `Bot <token>`.
type discordClient struct {
	token string
	http  *http.Client
}

func newDiscordClient(token string) *discordClient {
	return &discordClient{
		token: token,
		http:  &http.Client{Timeout: 15 * time.Second},
	}
}

// discordChannel is the subset of Discord's channel response we care about.
type discordChannel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type int    `json:"type"`
}

// discordWebhook is the subset of Discord's webhook response we care about.
type discordWebhook struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	Token string `json:"token"`
}

// createChannel creates a GUILD_TEXT channel in the given guild.
func (c *discordClient) createChannel(ctx context.Context, guildID, name string) (*discordChannel, error) {
	body := map[string]any{"name": name, "type": channelTypeText}
	var out discordChannel
	if err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/guilds/%s/channels", guildID), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// deleteChannel deletes a channel by ID. Returns nil if the channel is already
// gone (404) — that's the desired post-condition either way.
func (c *discordClient) deleteChannel(ctx context.Context, channelID string) error {
	err := c.do(ctx, http.MethodDelete,
		fmt.Sprintf("/channels/%s", channelID), nil, nil)
	if apiErr, ok := err.(*discordAPIError); ok && apiErr.Status == http.StatusNotFound {
		return nil
	}
	return err
}

// createWebhook creates a webhook on the channel and returns its URL.
func (c *discordClient) createWebhook(ctx context.Context, channelID, name string) (*discordWebhook, error) {
	body := map[string]any{"name": name}
	var out discordWebhook
	if err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/channels/%s/webhooks", channelID), body, &out); err != nil {
		return nil, err
	}
	// Some payloads omit `url` and provide id+token instead. Reconstruct if needed.
	if out.URL == "" && out.ID != "" && out.Token != "" {
		out.URL = fmt.Sprintf("%s/webhooks/%s/%s", discordAPIBase, out.ID, out.Token)
	}
	return &out, nil
}

// discordAPIError is returned for any non-2xx Discord response.
type discordAPIError struct {
	Status int
	Body   string
}

func (e *discordAPIError) Error() string {
	return fmt.Sprintf("discord API %d: %s", e.Status, e.Body)
}

// do issues a Discord API request with bot auth, JSON-encoded reqBody, and
// JSON-decodes the response into respOut (both can be nil).
func (c *discordClient) do(ctx context.Context, method, path string, reqBody, respOut any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		buf, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, discordAPIBase+path, bodyReader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bot "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "shop-operator (shophub-devoops, 0.1.0)")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &discordAPIError{Status: resp.StatusCode, Body: string(respBytes)}
	}

	if respOut != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, respOut); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
