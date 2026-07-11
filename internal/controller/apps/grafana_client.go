package apps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	promDatasourceUID = "prometheus"
	lokiDatasourceUID = "P8E80F9AEF21F6940"
	grafanaAdminUser  = "admin"
)

type grafanaClient struct {
	baseURL string
	user    string
	pass    string
	promURL string
	lokiURL string
	http    *http.Client
	// mora da koristimo organizacije jer je to jedini nacin da se u free grafani izoluju tenanti, folderi nisu dovoljni
	// grafana admin ima trenutnu aktivnu organizaciju, pisanja idu na tu organizaciju
	// ako 2 reconcile rade isotovremeno jedan bi mogoao da pise u tudju organizaciju, zato mutex
	mu sync.Mutex
}

func newGrafanaClientFromEnv() *grafanaClient {
	base := os.Getenv("GRAFANA_URL")
	if base == "" { // ako grafana nije podesena vrati nil, ne radimo nista
		return nil
	}
	return &grafanaClient{
		baseURL: base,
		user:    envOr("GRAFANA_ADMIN_USER", grafanaAdminUser),
		pass:    os.Getenv("GRAFANA_ADMIN_PASSWORD"),
		promURL: envOr("GRAFANA_PROMETHEUS_URL", "http://kube-prometheus-stack-prometheus.monitoring:9090"),
		lokiURL: envOr("GRAFANA_LOKI_URL", "http://loki:3100"),
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (g *grafanaClient) syncTenantDashboard(ctx context.Context, namespace string, dashboard map[string]any) error {
	orgID, err := g.ensureOrg(ctx, namespace) // napravi organizaciju ako ne postoji
	if err != nil {
		return fmt.Errorf("ensure org: %w", err)
	}
	if err := g.ensureDatasources(ctx, orgID); err != nil { // dodaj prometheus i loki u nju
		return fmt.Errorf("ensure datasources: %w", err)
	}
	if err := g.upsertDashboard(ctx, orgID, dashboard); err != nil { // ubaci dashboard
		return fmt.Errorf("upsert dashboard: %w", err)
	}
	return nil
}

type orgRef struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// vracamo ime organizacije
func (g *grafanaClient) lookupOrg(ctx context.Context, name string) (id int64, found bool, err error) {
	var org orgRef
	err = g.do(ctx, http.MethodGet, "/api/orgs/name/"+name, 0, nil, &org)
	if err == nil {
		return org.ID, true, nil
	}
	if apiErr, ok := err.(*grafanaAPIError); ok && apiErr.Status == http.StatusNotFound {
		return 0, false, nil // ako nema organizacije no-operation
	}
	return 0, false, err
}

func (g *grafanaClient) ensureOrg(ctx context.Context, name string) (int64, error) {
	id, found, err := g.lookupOrg(ctx, name)
	if err != nil {
		return 0, err
	}
	if found {
		return id, nil
	}

	var created struct {
		OrgID int64 `json:"orgId"`
	}
	if err := g.do(ctx, http.MethodPost, "/api/orgs", 0, map[string]any{"name": name}, &created); err != nil {
		if id, found, err2 := g.lookupOrg(ctx, name); err2 == nil && found {
			return id, nil
		}
		return 0, err
	}
	return created.OrgID, nil
}

func (g *grafanaClient) deleteTenantDashboard(ctx context.Context, namespace, uid string) error {
	orgID, found, err := g.lookupOrg(ctx, namespace)
	if err != nil {
		return fmt.Errorf("lookup org: %w", err)
	}
	if !found { // ako je nije nasao to je uspeh, no-op
		return nil
	}
	err = g.do(ctx, http.MethodDelete, "/api/dashboards/uid/"+uid, orgID, nil, nil)
	if apiErr, ok := err.(*grafanaAPIError); ok && apiErr.Status == http.StatusNotFound {
		return nil
	}
	return err
}

type grafanaDatasource struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	UID       string `json:"uid"`
	URL       string `json:"url"`
	Access    string `json:"access"`
	IsDefault bool   `json:"isDefault"`
}

func (g *grafanaClient) ensureDatasources(ctx context.Context, orgID int64) error {
	datasources := []grafanaDatasource{
		{Name: "Prometheus", Type: "prometheus", UID: promDatasourceUID, URL: g.promURL, Access: "proxy", IsDefault: true},
		{Name: "Loki", Type: "loki", UID: lokiDatasourceUID, URL: g.lokiURL, Access: "proxy"},
	}
	for _, ds := range datasources {
		err := g.do(ctx, http.MethodGet, "/api/datasources/uid/"+ds.UID, orgID, nil, nil)
		if err == nil {
			continue // vec postoji
		}
		if apiErr, ok := err.(*grafanaAPIError); !ok || apiErr.Status != http.StatusNotFound {
			return err
		}
		if err := g.do(ctx, http.MethodPost, "/api/datasources", orgID, ds, nil); err != nil {
			// reconcile ga je mozda napravio izmedju get i post, to je ok, samo nastavi
			if apiErr, ok := err.(*grafanaAPIError); ok && apiErr.Status == http.StatusConflict {
				continue
			}
			return err
		}
	}
	return nil
}

func (g *grafanaClient) upsertDashboard(ctx context.Context, orgID int64, dashboard map[string]any) error {
	dash := make(map[string]any, len(dashboard))
	maps.Copy(dash, dashboard)
	delete(dash, "id")
	body := map[string]any{
		"dashboard": dash,
		"overwrite": true,
		"folderId":  0,
	}
	return g.do(ctx, http.MethodPost, "/api/dashboards/db", orgID, body, nil)
}

type grafanaAPIError struct {
	Status int
	Body   string
}

func (e *grafanaAPIError) Error() string {
	return fmt.Sprintf("grafana API %d: %s", e.Status, e.Body)
}

func (g *grafanaClient) do(ctx context.Context, method, path string, orgID int64, reqBody, respOut any) error {
	if orgID > 0 {
		g.mu.Lock()
		defer g.mu.Unlock()
		if err := g.request(ctx, http.MethodPost, fmt.Sprintf("/api/user/using/%d", orgID), nil, nil); err != nil {
			return fmt.Errorf("switch to org %d: %w", orgID, err)
		}
	}
	return g.request(ctx, method, path, reqBody, respOut)
}

func (g *grafanaClient) request(ctx context.Context, method, path string, reqBody, respOut any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		buf, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, g.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.SetBasicAuth(g.user, g.pass)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := g.http.Do(req)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &grafanaAPIError{Status: resp.StatusCode, Body: string(respBytes)}
	}
	if respOut != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, respOut); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
