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

const discordAPIBase = "https://discord.com/api/v10"

const channelTypeText = 0 // discordov enum za txt channel

type discordClient struct {
	token string // bot token
	http  *http.Client
}

func newDiscordClient(token string) *discordClient {
	return &discordClient{
		token: token,
		http:  &http.Client{Timeout: 15 * time.Second},
	} // timeout je bitan, bez njega bi zaglavljeni discord poziv zauvek blokirao reconcile
}

type discordChannel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type int    `json:"type"`
}

type discordWebhook struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	Token string `json:"token"`
}

func (c *discordClient) createChannel(ctx context.Context, guildID, name string) (*discordChannel, error) {
	body := map[string]any{"name": name, "type": channelTypeText}
	var out discordChannel
	if err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/guilds/%s/channels", guildID), body, &out); err != nil {
		return nil, err
	}
	return &out, nil // vraca kanal sa id-jem
}

func (c *discordClient) deleteChannel(ctx context.Context, channelID string) error {
	err := c.do(ctx, http.MethodDelete,
		fmt.Sprintf("/channels/%s", channelID), nil, nil)
	if apiErr, ok := err.(*discordAPIError); ok && apiErr.Status == http.StatusNotFound {
		return nil // ako je vec obrisan vrati nil, to je zeljeno stanje, idempotentnost
	}
	return err
}

func (c *discordClient) createWebhook(ctx context.Context, channelID, name string) (*discordWebhook, error) {
	body := map[string]any{"name": name}
	var out discordWebhook
	if err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/channels/%s/webhooks", channelID), body, &out); err != nil {
		return nil, err
	}
	// neki discord odgovori ne vrate gotov url nego id + token pa onda mi mora da rucno da pravimo
	if out.URL == "" && out.ID != "" && out.Token != "" {
		out.URL = fmt.Sprintf("%s/webhooks/%s/%s", discordAPIBase, out.ID, out.Token)
	}
	return &out, nil
}

type discordAPIError struct {
	Status int
	Body   string
}

func (e *discordAPIError) Error() string {
	return fmt.Sprintf("discord API %d: %s", e.Status, e.Body)
}

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
	defer func() { _ = resp.Body.Close() }()

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
