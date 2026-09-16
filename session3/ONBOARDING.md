# #ExampleFrame# Onboarding Guide

This guide walks a new team through the two steps it takes to get #ExampleFrame# producing
useful RCAs for your services:

1. **#ExampleFrame# 1.0** - instrument your JVM apps so OpenTelemetry traces flow into Azure Application Insights.
2. **#ExampleFrame# 2.0** - register a *service mapping* so the orchestrator + child agents know which K8s workload, Oracle schema, dbt model, etc. belongs to each of your services.

You do (1) once per app, and (2) once per service you want covered.

---

## Which URL to use - your domain's cell, not the meta

Every domain runs its own #ExampleFrame# cell with its own URL, plus there is one meta
orchestrator that fronts all of them:

| URL | What it is | Use it for |
|---|---|---|
| `https://order.tb.com` | The **order** domain's cell | Anything about order-domain services |
| `https://payment.tb.com` | The **payment** domain's cell | Anything about payment-domain services |
| `https://claims.tb.com` | The **claims** domain's cell | Anything about claims-domain services |
| `https://meta.tb.com` | The **meta** orchestrator | Cross-domain questions, or when you don't know which domain owns a service |

**Default to your own domain's URL.** The meta forwards an in-domain question to your cell
verbatim and your cell's own router runs it, so the answer is the same either way - but going
direct is strictly better for a team that knows its services:

- **Richer prompting.** Your cell can prompt you interactively for infrastructure scope it
  can't infer (namespace, schema, tables) and then run the playbook. The meta only collects
  user-level params (time range, service) - for anything unmapped it falls back to a
  free-form investigation instead of a playbook run.
- **One less hop, one less LLM call.** Every question through the meta pays a routing hop
  plus a parameter-extraction LLM call before your cell even starts.
- **Live agent panels.** The cell streams each agent's raw findings as they land. Through
  the meta you get the cell's finished answer as a single block.
- **Access scoping.** Cell URLs line up with per-domain access groups - your team's
  credentials work on your cell without granting anything cross-domain.

**When the meta is the right door:** an incident that spans domains ("checkout is slow AND
claims batch is backed up"), a service you can't place, or on-call/SRE work across domains.
Meta routing works off the `domain` field the owning cell stamps on each service mapping -
which is one more reason mappings are registered on the cell, never on the meta (see Part 2).

Approvals for write operations (restart, scale, delete) work identically on both doors: the
same authorization panel appears wherever you asked, and nothing executes until you approve.

---

## Part 1 - #ExampleFrame# 1.0 (instrumentation)

#ExampleFrame# 1.0 is a ByteBuddy-based Java agent (`-javaagent:#exampleframe#.jar`) that auto-instruments your JVM applications at the **application package level** and ships OpenTelemetry traces, metrics, and logs to Azure Application Insights with **zero code changes**.

> **Onboarding for #ExampleFrame# 1.0 - follow the PRR:**
> **`<INSERT #EXAMPLEFRAME# 1.0 PRR LINK HERE>`**
>
> The PRR covers: how to attach the agent, configure the App Insights connection string, set business-context attributes (`tenantId`, `orderId`, etc.), and validate traces are arriving.

Come back here once your service is emitting traces.

---

## Part 2 - #ExampleFrame# 2.0 (service mapping)

### Why service mapping matters

The orchestrator routes natural-language questions to specialist agents (Kubernetes, Oracle, Azure Insight, dbt, JVM). Without a mapping, when a user asks *"why is checkout slow?"*, the orchestrator falls back to a **ReAct loop** - it has to ask the K8s agent *"which namespaces have a 'checkout'?"*, then ask the Oracle agent *"which schema owns the relevant tables?"*, etc. Each wrong guess costs an LLM round-trip and seconds of latency.

With a service mapping in place, that same question routes **deterministically**: namespace, workload, schema, dbt model, and package name are looked up in one hop and injected into every agent call. RCA happens in one conversation instead of three.

### What a service mapping looks like

**The mapping form is generated from your cell's agents.** Each domain's Settings modal
asks only for the fields its own agents can use - the form is built from
`GET /api/settings/service-context/schema`, which reflects the cell's configured agent
list. An order cell running Kubernetes + Oracle shows K8s and Oracle fields; a payment
cell running Kubernetes + PostgreSQL shows K8s and Postgres fields instead. The meta shows
no infrastructure fields at all - it only routes by domain. So don't expect the exact
table below on every cell; it's the field set for a cell running K8s + Oracle + Azure
Monitor + dbt.

Each mapping ties together everything #ExampleFrame# needs to know about one service:

| Field | What it is | Example |
|---|---|---|
| **Service Name** | Canonical name; primary key for the mapping | `checkout-service` |
| **Aliases** | Other names users / alerts / traces might use for the same service | `checkout, checkout-api, CheckoutService` |
| **K8s Namespace** | Namespace where the service runs | `production` |
| **K8s Workload** | **Deployment name** (or StatefulSet / DaemonSet) - NOT the pod name | `checkout-service` |
| **Oracle Schema** | DB schema the service owns | `CHECKOUT_PROD` |
| **Oracle Tables** | Tables the service reads/writes (comma-separated) | `ORDERS,ORDER_ITEMS,PAYMENTS` |
| **Azure Monitor Package Name** | Java package prefix used to filter App Insights / KQL queries | `com.yourcompany.checkout` |
| **dbt Definition ID** | dbt model/definition identifier | `def_checkout_refresh` |

> **`workload` = Deployment name.** Pod names are dynamic (`checkout-service-7b8d9c-xyz` changes every restart). Use the controller that owns the pods. Verify with:
> ```bash
> kubectl get deployment <workload-name> -n <namespace>
> ```

### How to add a service mapping (UI)

1. Open **your domain's** #ExampleFrame# terminal (e.g. `https://order.tb.com/`). Register
   mappings on the cell that owns the service, never on the meta - the cell stamps its
   `domain` on the mapping, and that stamp is what the meta uses to route questions to you.
2. Click **`[SETTINGS]`** in the header.
3. Scroll to **"Service Context Mapping"** at the top of the modal.
4. Fill in the fields above and click **`SAVE MAPPING`**.
5. The mapping is persisted in Cassandra and is live immediately - no restart needed.

To edit or delete an existing mapping, scroll to **"Current Service Mappings"** below the form.

### How to add a service mapping (YAML - for bulk / GitOps)

For bulk loads or environments you want under version control, add entries to `#exampleframe#-orchestrator/src/main/resources/application.yaml` (or your environment-specific profile yaml such as `application-acme.yaml`):

```yaml
#exampleframe#:
  orchestrator:
    app-context:
      services:
        checkout-service:
          aliases: "checkout,checkout-api,CheckoutService"
          azure-monitor.packageName: "com.yourcompany.checkout"
          kubernetes.namespace: "production"
          kubernetes.workload: "checkout-service"
          oracle.schema: "CHECKOUT_PROD"
          oracle.tables: "ORDERS,ORDER_ITEMS,PAYMENTS"
          dbt.definitionId: "def_checkout_refresh"
```

YAML-defined mappings load at startup; UI-defined mappings reload dynamically. Both sources coexist - UI changes do not overwrite YAML.

### Verifying the mapping works

After saving, ask the orchestrator a question that should hit the mapping:

```
Why is checkout-service slow in the last hour?
```

Confirm in the agent logs that the K8s agent receives the correct namespace + workload:

```bash
kubectl logs -n #exampleframe# deploy/kubernetes-agent --tail=200 | \
  grep -E "namespace|workload|checkout"
```

You should see lines querying `namespace=production` and `workload=checkout-service`. If you instead see the agent guessing or scanning all namespaces, the mapping isn't being picked up - see Troubleshooting below.

### Aliases - what to put there

The orchestrator matches aliases **case-insensitive, word-boundary**, longest-first. Add every reasonable variant your users, alerts, dashboards, or trace span names use:

| You should include | Example |
|---|---|
| Short name | `checkout` |
| API name | `checkout-api` |
| Class/Camel name | `CheckoutService` |
| Internal nickname | `chk-svc` |
| Owning team's term | `order-flow` (if your team calls it that) |

The more aliases, the fewer "I don't know what service you mean" failures.

---

## Part 3 - VCS Package Mapping (code-level RCA)

### Why it matters

#ExampleFrame# 1.0 captures spans at the **method level** (e.g., `com.yourcompany.checkout.OrderValidator.checkInventory`). When the Azure Insight agent surfaces *"`OrderValidator.checkInventory` is the hot frame"*, the orchestrator can do something powerful: **pull the actual source code for that class straight from your repo** and let the LLM reason over it.

That turns a trace finding into a code-level diagnosis - *"this method does a sync DB call inside a stream pipeline, that's why it's slow at scale"* - instead of the user having to open the repo themselves.

For that to work, #ExampleFrame# needs to know **which Java package lives in which repo**. That's what VCS Package Mappings do.

### What a VCS mapping looks like

| Field | What it is | Example |
|---|---|---|
| **Package Name** | Java package prefix that should resolve to this repo (longest match wins) | `com.yourcompany.checkout` |
| **Provider** | `github` or `azure-devops` | `github` |

#### GitHub-specific fields
| Field | Example |
|---|---|
| **Owner** | `yourcompany` |
| **Repo** | `checkout-service` |
| **Branch** | `main` (default) |

#### Azure DevOps-specific fields
| Field | Example |
|---|---|
| **Organization** | `yourcompany` |
| **Project** | `my-project` |
| **Repo** | `checkout-service` |
| **Branch** | `main` (default) |
| **Base URL** *(optional)* | `https://dev.azure.com/yourcompany/my-project/_apis/git/repositories/checkout-service` |

### How to add a VCS mapping (UI)

1. Open **your domain's** #ExampleFrame# terminal (e.g. `https://order.tb.com/`).
2. Click **`[SETTINGS]`** in the header.
3. Scroll past *"Service Context Mapping"* and *"Current Service Mappings"* to **"VCS Package Mappings"**.
4. In the **Add Mapping** form:
   - Type the **Package Name** (e.g., `com.yourcompany.checkout`).
   - Pick the **Provider** (GitHub or Azure DevOps).
   - Fill in the provider-specific fields below (the form swaps based on provider).
5. Click **`ADD MAPPING`**.
6. Changes take effect **immediately - no restart needed** (see the note above the form).

The mappings table at the bottom shows what's currently registered. Use the delete button to remove an entry.

### Mapping strategy - what packages to register

Register the **broadest stable prefix** per repo. Examples:

| Your package layout | What to register |
|---|---|
| `com.yourcompany.checkout.*` lives in one repo | `com.yourcompany.checkout` (single mapping) |
| `com.yourcompany.checkout.api.*` and `com.yourcompany.checkout.batch.*` live in different repos | Two mappings: `com.yourcompany.checkout.api` + `com.yourcompany.checkout.batch` |
| All of `com.yourcompany.*` is in a monorepo | `com.yourcompany` (one mapping covers everything) |

**Longest prefix wins**, so you can register a broad mapping and then override specific sub-packages where needed.

### Verifying the mapping works

After adding a mapping, ask #ExampleFrame# a question that should reach the code:

```
Get the source code for com.yourcompany.checkout.OrderValidator.checkInventory
```

The orchestrator should return the actual method body from GitHub/Azure DevOps. If you see *"package not mapped"* or *"file not found in any repo"*, the mapping is missing or wrong.

For an end-to-end test, ask a question that requires both Azure Insight + VCS:

```
What's the slowest method in checkout-service in the last hour, and show me its source
```

You should see the agent identify the hot method from App Insights, then fetch its code from your repo.

### Tying it together with Service Mapping

Service Mapping connects a **service** to its infrastructure (namespace, schema, etc.). VCS Mapping connects a **package** to its repo. They overlap on the *Azure Monitor Package Name* field in the service mapping - that's the entry point: service mapping tells #ExampleFrame# *"checkout's traces filter on `com.yourcompany.checkout`"*, then VCS mapping tells #ExampleFrame# *"that package lives in `yourcompany/checkout-service` on GitHub."*

Set both for every service you care about. One without the other only gets you halfway:

| What you have | What you get |
|---|---|
| Service mapping only | Right namespace, right schema - but no code-level RCA |
| VCS mapping only | #ExampleFrame# can fetch code if asked, but won't know which package to look at without service context |
| **Both** | **Full chain: trace -> method -> source -> RCA** |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Orchestrator says *"no agents available"* intermittently | Nacos heartbeat or child agent restarted briefly | Check `kubectl get pods -n #exampleframe#` for restarts; loosen Nacos heartbeat windows |
| K8s agent queries the wrong namespace | Mapping missing, or alias doesn't match the user's wording | Add the user's exact wording to the **Aliases** field |
| Meta asks *"which domain owns this service?"* for a service you mapped | Mapping was registered on the meta (no `domain` stamp) or on the wrong cell | Re-register on the **owning** cell's URL; the cell stamps `domain`, which the meta routes on |
| Mapping saved in UI doesn't take effect | Browser cache | Hard-refresh (Cmd-Shift-R), then re-prompt |
| K8s agent says "Deployment not found" | `workload` field has the pod name (dynamic) instead of the deployment name | Replace with the Deployment name; verify with `kubectl get deployment <name> -n <ns>` |
| Prod responses are terser than non-prod | Different LLM (gpt-4o vs Claude) and/or `temperature` default of 1.0 | Match `temperature: 0.1` and use gpt-5 in prod |
| *"Package not mapped"* / *"file not found in repo"* when asking for code | No VCS mapping registered for that Java package | Add a VCS Package Mapping under `[SETTINGS]` for the broadest prefix (e.g., `com.yourcompany.checkout`) |
| Agent fetches code from wrong branch | VCS mapping is pointing at `main` but your code is on a release branch | Edit the VCS mapping - change the **Branch** field |

---

## Reference

- **#ExampleFrame# 1.0 PRR**: `<INSERT #EXAMPLEFRAME# 1.0 PRR LINK HERE>`
- #ExampleFrame# 2.0 architecture: [#ExampleFrame#-Architecture-README.md](#ExampleFrame#-Architecture-README.md)
- Cassandra data model: [cassandra-data-model.md](cassandra-data-model.md)
- Orchestrator service-mapping code: [#exampleframe#-orchestrator/src/main/java/com/#exampleframe#/orchestrator/service/context/](#exampleframe#-orchestrator/src/main/java/com/#exampleframe#/orchestrator/service/context/)

---

## Need help?

- Questions about #ExampleFrame# 1.0 instrumentation -> see the PRR link above.
- Questions about service mapping or RCA quality -> ping the #ExampleFrame# team in your usual Slack channel.
