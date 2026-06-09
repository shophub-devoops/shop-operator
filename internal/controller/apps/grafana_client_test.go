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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeGrafana is a minimal in-memory Grafana that records what the client does,
// so we can assert org creation, the active-org switch, datasource and
// dashboard writes happen as expected.
type fakeGrafana struct {
	mu             sync.Mutex
	orgsByName     map[string]int64
	nextOrgID      int64
	activeOrg      int64
	datasourceUIDs map[int64]map[string]bool // orgID -> set of created uids
	dashboards     map[int64][]string        // orgID -> dashboard uids
}

func newFakeGrafana() *fakeGrafana {
	return &fakeGrafana{
		orgsByName:     map[string]int64{},
		nextOrgID:      1,
		datasourceUIDs: map[int64]map[string]bool{},
		dashboards:     map[int64][]string{},
	}
}

func (f *fakeGrafana) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/orgs/name/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		name := strings.TrimPrefix(r.URL.Path, "/api/orgs/name/")
		id, ok := f.orgsByName[name]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "name": name})
	})

	mux.HandleFunc("/api/orgs", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var body struct {
			Name string `json:"name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		id := f.nextOrgID
		f.nextOrgID++
		f.orgsByName[body.Name] = id
		_ = json.NewEncoder(w).Encode(map[string]any{"orgId": id})
	})

	mux.HandleFunc("/api/user/using/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id, _ := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/api/user/using/"), 10, 64)
		f.activeOrg = id
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/api/datasources/uid/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		uid := strings.TrimPrefix(r.URL.Path, "/api/datasources/uid/")
		if f.datasourceUIDs[f.activeOrg][uid] {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	})

	mux.HandleFunc("/api/datasources", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var body struct {
			UID string `json:"uid"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if f.datasourceUIDs[f.activeOrg] == nil {
			f.datasourceUIDs[f.activeOrg] = map[string]bool{}
		}
		f.datasourceUIDs[f.activeOrg][body.UID] = true
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/api/dashboards/db", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var body struct {
			Dashboard map[string]any `json:"dashboard"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if _, hasID := body.Dashboard["id"]; hasID {
			http.Error(w, "dashboard must not carry an id", http.StatusBadRequest)
			return
		}
		uid, _ := body.Dashboard["uid"].(string)
		f.dashboards[f.activeOrg] = append(f.dashboards[f.activeOrg], uid)
		w.WriteHeader(http.StatusOK)
	})

	return mux
}

func TestSyncTenantDashboard(t *testing.T) {
	fake := newFakeGrafana()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	g := &grafanaClient{
		baseURL: srv.URL,
		user:    grafanaAdminUser,
		pass:    "pw",
		promURL: "http://prom",
		lokiURL: "http://loki",
		http:    srv.Client(),
	}

	dash := map[string]any{"uid": "shop-acme", "title": "Shop — acme", "id": float64(7)}
	if err := g.syncTenantDashboard(context.Background(), "tenant-acme", dash); err != nil {
		t.Fatalf("syncTenantDashboard: %v", err)
	}

	orgID := fake.orgsByName["tenant-acme"]
	if orgID == 0 {
		t.Fatal("tenant org was not created")
	}
	// Datasources must exist in the tenant org with the UIDs the dashboard refs.
	for _, uid := range []string{promDatasourceUID, lokiDatasourceUID} {
		if !fake.datasourceUIDs[orgID][uid] {
			t.Errorf("datasource %q missing in org %d", uid, orgID)
		}
	}
	// Dashboard imported into the tenant org (not the default org).
	if got := fake.dashboards[orgID]; len(got) != 1 || got[0] != "shop-acme" {
		t.Errorf("tenant org dashboards = %v, want [shop-acme]", got)
	}

	// The original dashboard map must be left untouched (id preserved for the
	// ConfigMap path), while the uploaded copy dropped the id.
	if _, ok := dash["id"]; !ok {
		t.Error("upsertDashboard mutated the caller's dashboard map (removed id)")
	}
}

func TestEnsureOrgReusesExisting(t *testing.T) {
	fake := newFakeGrafana()
	fake.orgsByName["tenant-x"] = 42
	fake.nextOrgID = 43
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	g := &grafanaClient{baseURL: srv.URL, user: grafanaAdminUser, pass: "pw", http: srv.Client()}
	id, err := g.ensureOrg(context.Background(), "tenant-x")
	if err != nil {
		t.Fatalf("ensureOrg: %v", err)
	}
	if id != 42 {
		t.Errorf("ensureOrg returned %d, want existing 42", id)
	}
	if len(fake.orgsByName) != 1 {
		t.Errorf("ensureOrg created a duplicate org: %v", fake.orgsByName)
	}
}
