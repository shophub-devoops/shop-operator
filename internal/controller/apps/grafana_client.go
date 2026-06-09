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

// Datasource UIDs the embedded dashboard.json references. A freshly-created
// Grafana org has no datasources, so we recreate them here with these exact
// UIDs — otherwise the imported dashboard's panels point at non-existent
// datasources and render "Datasource not found".
const (
	promDatasourceUID = "prometheus"
	lokiDatasourceUID = "P8E80F9AEF21F6940"
	grafanaAdminUser  = "admin"
)

// grafanaClient is a minimal HTTP client for the Grafana org-provisioning we
// need (spec 4.1 optional: each ShopHub tenant sees only their own dashboards).
// It authenticates as the Grafana admin (basic auth) and provisions, per tenant
// namespace, a dedicated Grafana Organization holding that tenant's datasources
// and Shop dashboards. ShopHub creates the per-tenant user that logs into the
// org (see the shophub backend's grafana provisioning).
//
// True per-tenant isolation requires Organizations: in OSS Grafana the basic
// Viewer role is org-wide, so folder permissions alone cannot scope a user to a
// single folder. A separate org per tenant is the only OSS-native way to keep
// one tenant from seeing another's dashboards.
type grafanaClient struct {
	baseURL string
	user    string
	pass    string
	promURL string
	lokiURL string
	http    *http.Client

	// mu serializes the "switch admin's active org, then operate" sequence.
	// Grafana scopes basic-auth writes to the admin user's current org (set via
	// /api/user/using/:id), which is shared mutable state; the lock keeps two
	// concurrent reconciles from writing into each other's org.
	mu sync.Mutex
}

// newGrafanaClientFromEnv builds a grafanaClient from GRAFANA_* env, or returns
// nil when GRAFANA_URL is unset so the controller treats org provisioning as a
// best-effort no-op (same pattern as the other optional integrations).
func newGrafanaClientFromEnv() *grafanaClient {
	base := os.Getenv("GRAFANA_URL")
	if base == "" {
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

// syncTenantDashboard provisions everything a tenant org needs to render one
// Shop dashboard: the org itself, its datasources, and the dashboard. Idempotent
// — safe to call on every reconcile.
func (g *grafanaClient) syncTenantDashboard(ctx context.Context, namespace string, dashboard map[string]any) error {
	orgID, err := g.ensureOrg(ctx, namespace)
	if err != nil {
		return fmt.Errorf("ensure org: %w", err)
	}
	if err := g.ensureDatasources(ctx, orgID); err != nil {
		return fmt.Errorf("ensure datasources: %w", err)
	}
	if err := g.upsertDashboard(ctx, orgID, dashboard); err != nil {
		return fmt.Errorf("upsert dashboard: %w", err)
	}
	return nil
}

// orgRef is the subset of Grafana's org response we read.
type orgRef struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// ensureOrg returns the id of the org named name, creating it if absent.
func (g *grafanaClient) ensureOrg(ctx context.Context, name string) (int64, error) {
	var org orgRef
	err := g.do(ctx, http.MethodGet, "/api/orgs/name/"+name, 0, nil, &org)
	if err == nil {
		return org.ID, nil
	}
	if apiErr, ok := err.(*grafanaAPIError); !ok || apiErr.Status != http.StatusNotFound {
		return 0, err
	}

	var created struct {
		OrgID int64 `json:"orgId"`
	}
	if err := g.do(ctx, http.MethodPost, "/api/orgs", 0, map[string]any{"name": name}, &created); err != nil {
		// Lost a create race (another reconcile won) — re-read by name.
		if err2 := g.do(ctx, http.MethodGet, "/api/orgs/name/"+name, 0, nil, &org); err2 == nil {
			return org.ID, nil
		}
		return 0, err
	}
	return created.OrgID, nil
}

// grafanaDatasource is the create payload for POST /api/datasources.
type grafanaDatasource struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	UID       string `json:"uid"`
	URL       string `json:"url"`
	Access    string `json:"access"`
	IsDefault bool   `json:"isDefault"`
}

// ensureDatasources creates the Prometheus and Loki datasources (with the UIDs
// the dashboard references) in the given org if they are not already present.
func (g *grafanaClient) ensureDatasources(ctx context.Context, orgID int64) error {
	datasources := []grafanaDatasource{
		{Name: "Prometheus", Type: "prometheus", UID: promDatasourceUID, URL: g.promURL, Access: "proxy", IsDefault: true},
		{Name: "Loki", Type: "loki", UID: lokiDatasourceUID, URL: g.lokiURL, Access: "proxy"},
	}
	for _, ds := range datasources {
		err := g.do(ctx, http.MethodGet, "/api/datasources/uid/"+ds.UID, orgID, nil, nil)
		if err == nil {
			continue // already exists
		}
		if apiErr, ok := err.(*grafanaAPIError); !ok || apiErr.Status != http.StatusNotFound {
			return err
		}
		if err := g.do(ctx, http.MethodPost, "/api/datasources", orgID, ds, nil); err != nil {
			// A concurrent reconcile may have created it between our GET and POST.
			if apiErr, ok := err.(*grafanaAPIError); ok && apiErr.Status == http.StatusConflict {
				continue
			}
			return err
		}
	}
	return nil
}

// upsertDashboard imports dashboard into orgID, overwriting any existing version
// with the same uid. The dashboard's `id` must be absent so Grafana treats it as
// an org-local create/update rather than an id collision.
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

// grafanaAPIError is returned for any non-2xx Grafana response.
type grafanaAPIError struct {
	Status int
	Body   string
}

func (e *grafanaAPIError) Error() string {
	return fmt.Sprintf("grafana API %d: %s", e.Status, e.Body)
}

// do issues a Grafana API request as the admin user. When orgID > 0 the admin's
// active org is switched to it first, under the client lock, so the request
// operates inside the target tenant org.
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

// request performs a single authenticated Grafana API call.
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
