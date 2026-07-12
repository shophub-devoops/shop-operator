# shop-operator

Kubernetes operator koji pokreće **ShopHub** platformu (specifikacija 3.1).
Napravljen sa [Kubebuilder](https://book.kubebuilder.io/)-om (multi-group), a
zadatak mu je da tri Custom Resource-a pretvori u stvarnu infrastrukturu
prodavnice.

## CRD-ovi

| Kind | Grupa | Šta kontroler radi |
|------|-------|--------------------|
| **Shop** | `apps.shophub.local/v1` | Pravi celu prodavnicu: bazu preko njenog operatora (CNPG `Cluster` za `postgres`, `MongoDBCommunity` za `mongodb`), Deployment (2 replike za `standard`, 3 za `high`), Service, Ingress, Prometheus `ServiceMonitor`, Grafana dashboard po tenantu (ConfigMap), i — kad je referenciran Discord webhook — `AlertmanagerConfig` koji alarme prodavnice šalje u njen kanal. Stanje piše u `.status` (Available/Progressing/Degraded). |
| **DiscordChannel** | `notify.shophub.local/v1` | Priča sa pravim Discord API-jem: pravi kanal na guildu i webhook, čuva webhook URL u Secret-u (`<ime>-webhook`, ključ `webhook-url`). Finalizer briše kanal na guildu pre nego što se CR obriše. |
| **Wallet** | `payments.shophub.local/v1` | Zapiše spolja datu adresu, ili sam generiše novi secp256k1 par ključeva i privatni ključ čuva u Secret-u (`<ime>-key`). |

Radni primer svakog vidi u [`config/samples/`](config/samples/).

## Kako radi (bitne stvari)

- **Bazu pravi tuđi operator.** Shop kontroler napravi CR baze i čeka da taj
  operator objavi konekcioni Secret (`<shop>-app`), pa ga ubaci u Deployment
  storefront-a kao `DATABASE_URL`. CNPG objavi URI pod ključem `uri`, MongoDB
  operator pod `connectionString.standard`.
- **Reconcile je idempotentan.** Svaka `ensure` funkcija prvo pročita postojeće
  stanje; ako je već kako treba, ništa ne dira. Checkpoint je uvek `.status`.
- **Garbage collection.** Svaki napravljeni objekat nosi OwnerReference ka svom
  Shop/Wallet/DiscordChannel CR-u, pa brisanje CR-a počisti sve što je napravio.
- **Watch sa predikatom.** Operator ne prati sve Secret-e u klasteru (memory
  leak), nego filtrira po sufiksu (`-app`, `-webhook`).

> ⚠️ **Ne brišite ručno `<shop>-app` Secret.** Objavljuje ga operator baze
> (CNPG/MongoDB) i sadrži konekcioni string ka bazi prodavnice — bez njega
> storefront ne može na bazu. Operator ga posmatra i vraća se u čekanje ako
> nestane, ali elegantnog automatskog oporavka za ručno brisanje nema.

## Struktura

```
api/{apps,notify,payments}/v1/               # Go tipovi CRD-ova (+ generisani deepcopy)
internal/controller/{apps,notify,payments}/  # tri reconciler-a + testovi
config/                                       # kustomize: CRD-ovi, RBAC, manager, samples
cmd/main.go                                   # ulaz (manager)
Dockerfile                                    # slika kontrolera
```

Helm chart koji deploy-uje ovaj operator (Deployment + RBAC + CRD-ovi +
PrometheusRule) je u [`helm-charts`](https://github.com/shophub-devoops/helm-charts)
repou, pod `charts/shop-operator`.

## Razvoj

Treba Go (vidi `go.mod`), `make` i Docker (za envtest / build slike).

```bash
make manifests generate   # regeneriši CRD-ove + deepcopy posle izmene api/ tipova
make test                 # unit + envtest testovi kontrolera
make build                # kompajliraj manager binarni fajl
make docker-build         # napravi sliku kontrolera
```

Pokretanje protiv klastera iz kubeconfig-a:

```bash
make install              # apply CRD-ova
make run                  # pokreni manager lokalno
```

## CI

- **test** — build, vet i unit + envtest testovi na svakom PR-u.
- **test-e2e** — end-to-end test na Kind klasteru.
- **lint** — `golangci-lint`.
- **docker-build** — build slike na PR-u (bez push-a) + hadolint.
- **docker-publish** — push slike na DockerHub (`shop-operator-controller`) sa
  SemVer tagovima na `main` / tagovima.
- **commit-lint** — Conventional Commits.
