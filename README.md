# shop-operator

The Kubernetes operator that backs the **ShopHub** platform (spec section 3.1).
Built with [Kubebuilder](https://book.kubebuilder.io/) (multi-group layout), it
reconciles three Custom Resources into the running infrastructure of a Shop
tenant.

## CRDs

| Kind | Group | What the controller does |
|------|-------|--------------------------|
| **Shop** | `apps.shophub.local/v1` | Provisions a full storefront: a database via its operator (CNPG `Cluster` for `postgres`, `MongoDBCommunity` for `mongodb`), a Deployment (2 replicas for `standard`, 3 for `high`), Service, Ingress, a Prometheus `ServiceMonitor`, a per-tenant Grafana dashboard (ConfigMap), and — when a Discord webhook is referenced — an `AlertmanagerConfig` that routes the shop's alerts to its channel. Tracks state in `.status` (Available/Progressing/Degraded). |
| **DiscordChannel** | `notify.shophub.local/v1` | Talks to the real Discord API: creates a channel on the guild and a webhook, stores the webhook URL in a Secret (`<name>-webhook`, key `webhook-url`). A finalizer deletes the channel on the guild before the CR is removed. |
| **Wallet** | `payments.shophub.local/v1` | Records an externally supplied address, or generates a fresh secp256k1 keypair and stores the private key in a Secret (`<name>-key`). |

See [`config/samples/`](config/samples/) for a working example of each.

## Architecture notes

- **Database is operator-managed.** The Shop controller creates the database CR
  and waits for the operator-published connection Secret (`<shop>-app`), then
  injects it into the storefront Deployment as `DATABASE_URL`. CNPG publishes the
  URI under the `uri` key; the MongoDB Community operator under
  `connectionString.standard`.
- **MongoDB per-namespace RBAC.** The MongoDB Community operator's chart only
  installs the `mongodb-database` ServiceAccount (+ Role/RoleBinding) in its own
  namespace, so the controller materializes them in every tenant namespace.
- **Observability wiring.** Each Shop gets its own dashboard (the embedded
  [`dashboard.json`](internal/controller/apps/dashboard.json) is stamped with the
  tenant name) and the backend is pointed at the in-cluster Tempo OTLP endpoint
  for tracing.
- **Garbage collection.** Every child object carries an OwnerReference to its
  Shop/Wallet/DiscordChannel, so deleting a CR cleans up everything it created.

## Layout

```
api/{apps,notify,payments}/v1/   # CRD Go types (+ generated deepcopy)
internal/controller/{apps,notify,payments}/  # the three reconcilers + tests
config/                          # kustomize: CRDs, RBAC, manager, samples
cmd/main.go                      # manager entrypoint
Dockerfile                       # distroless controller image
```

The chart that deploys this operator (Deployment + RBAC + CRDs + PrometheusRule)
lives in the [`helm-charts`](https://github.com/shophub-devoops/helm-charts) repo
under `charts/shop-operator`.

## Development

Requires Go (see `go.mod`), `make`, and Docker (for envtest / image builds).

```bash
make manifests generate   # regenerate CRDs + deepcopy after editing api/ types
make test                 # unit + envtest controller tests (downloads envtest bins)
make build                # compile the manager binary
make docker-build         # build the controller image
```

Run against a cluster from your kubeconfig:

```bash
make install              # apply CRDs
make run                  # run the manager locally
```

## CI

- **test** — build, vet and run the unit + envtest controller tests on every PR.
- **test-e2e** — Kind-based end-to-end test.
- **lint** — `golangci-lint`.
- **docker-build** — builds the controller image on PRs (no push).
- **docker-publish** — pushes the image to DockerHub (`shop-operator-controller`)
  with SemVer tags on `main` / tags.
- **commit-lint** — enforces Conventional Commits.
