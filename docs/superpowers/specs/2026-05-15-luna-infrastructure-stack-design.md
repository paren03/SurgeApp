# Luna Infrastructure Stack Design

Generated: 2026-05-15

## Goal

Integrate an enterprise-grade local infrastructure stack into Luna without
corrupting the truth model. The stack must strengthen transport, runtime
survival, observability, durable execution, memory, inference routing, and
self-audit while preserving the hard rule that only strict attestors can
advance the rebuild frontier.

## Verified Current State

### Truth and attestation

- `memory/tier_review/tier_rebuild_frontier.json` is the current honest
  rebuild source:
  - `current_rebuild_tier = 131`
  - `highest_honestly_verified_tier = 130`
  - blocker: proof valid, adoption/use missing
- `memory/tier_truth/tier_proof_registry.json` still carries the older
  historical `current_operating_tier = 160` / `current_effective_tier = 500`
  framing. This must be treated as historical, not frontier truth.
- Strict non-synthetic signers already exist and are the right boundary:
  - `luna_tier_adoption_attestor.py`
  - `luna_tier_use_attestor.py`
  - `tier_runtime_proof.py`
- `repair_task_executor.py` already refuses to write synthetic adoption/use
  placeholders and routes failed attestations to `manual_followup`.

### Runtime and operator surface

- `LaunchLunaDashboard.pyw` is the live boot owner.
- `worker.py` owns the live brain loop.
- `repair_task_executor.py` owns repair/adopt/use execution.
- `luna_runtime_watchdog.py` + scheduled watchdog already provide bounded
  runtime supervision.
- `/api/terminal-truth` is the primary operator truth route.
- `luna_agent_bus.py` is the current message bus, backed by append-only JSONL.

### Advisory partition

- Council modules exist and are already advisory-only:
  - `luna_llm_council_bridge.py`
  - `luna_llm_council_config.py`
  - `luna_llm_council_policy.py`
  - `luna_llm_council_storage.py`
  - `luna_llm_council_reporter.py`
  - `luna_council_advisor.py`
- Authoritative modules must remain council-free.

## Verified Tier Audit

Strict file audit across Tiers 1..131 shows the current rebuild frontier is not
the only place with legacy gaps.

### Canonical file audit summary

- `proof_missing = 4`
- `adoption_missing = 6`
- `use_missing = 7`
- `synthetic proofs/adoption/use in 1..131 = 0` in the canonical JSON files
- first incomplete canonical tier by strict file presence = `15`

### Known incomplete canonical tiers

- Tier 15: use missing
- Tier 17: adoption missing, use missing
- Tier 19: proof missing, adoption missing, use missing
- Tier 31: proof missing, adoption missing, use missing
- Tier 38: proof missing, adoption missing, use missing
- Tier 41: proof missing, adoption missing, use missing
- Tier 131: adoption missing, use missing

### Meaning

There are two truths at once:

1. The active rebuild campaign is currently blocked at Tier 131.
2. The canonical tier-truth folders still contain earlier legacy gaps that a
   fully strict audit would need to rebuild as well.

This means the honest reset cannot merely "finish 131 and continue." It needs a
rebuild campaign mode that can:

- preserve the current active frontier,
- record a legacy-gap backlog,
- and refuse to let historical missing adoption/use be mistaken for complete
  capability.

## Target Infrastructure Inventory

### Present in repo or environment

- LiteLLM is installed in `.aider_venv`, but not wired into Luna runtime.
- `.semgrep.yml` exists, so Semgrep policy scaffolding exists, but it is not a
  strict attestor gate yet.
- `install_ollama_aider_stack.bat` exists, and council bridge has an Ollama
  placeholder, so local inference ideas exist, but not a real fleet.

### Not currently wired in Luna

- NATS
- WinSW
- OpenTelemetry
- Qdrant
- Temporal
- LocalAI
- vLLM
- llama.cpp
- SonarQube

These should be treated as fresh integrations, not already-working subsystems.

## Final Ecosystem Blueprint

### 1. Message transport spine

`luna_agent_bus.py` should remain the policy and evidence validator, but stop
being the only transport.

Final shape:

- `luna_agent_bus.py` becomes the canonical message contract + evidence gate.
- NATS JetStream becomes the live transport and replay backbone.
- JSONL remains the append-only audit mirror for operator visibility and local
  forensics.

Proposed subject layout:

- `luna.agentbus.publish`
- `luna.agentbus.verified`
- `luna.agentbus.blocked`
- `luna.review.tasks`
- `luna.review.results`
- `luna.truth.events`
- `luna.watchdog.heartbeat`
- `luna.runtime.alerts`

Rule:
NATS carries messages, but `luna_agent_bus.publish()` still decides whether a
message is a visible verified fact, a hidden needs-review event, or a blocked
claim.

### 2. Runtime survival shell

WinSW should become the Windows service layer above the existing boot chain.

Final shape:

- WinSW service wraps the dashboard runtime root.
- Child responsibilities remain split:
  - dashboard/http process
  - worker loop
  - repair executor
  - watchdog
- The current scheduled watchdog remains useful during transition, but final
  ownership should move to service-managed survival.

Rule:
WinSW proves survival and restart continuity. It does not sign truth, and it
does not replace the attestors.

### 3. Durable execution engine

Temporal should take over fragile polling and file-claim loops where durability
 matters.

Final shape:

- `repair_task_executor.py` becomes a Temporal worker host plus activities.
- `terminal_manager_tier_review.py` becomes a workflow driver for:
  - inspect
  - repair
  - adopt
  - prove
  - verify
  - use
  - pass
- retries, heartbeats, and resumability move to Temporal state, not ad hoc
  file polling.

Rule:
Temporal owns orchestration durability, but the final proof/adoption/use write
 still happens through the strict attestor modules.

### 4. Truth and observability mesh

OpenTelemetry should observe every truth-critical path without becoming truth.

Spans and events must cover:

- `/api/chat/send`
- `luna_agent_bus.publish()`
- `repair_task_executor.process_task()`
- `terminal_manager_tier_review.step_one_tier()`
- attestor runtime probes
- watchdog health passes/failures
- NATS publish/consume latency
- Temporal workflow state transitions

Rule:
OTel provides evidence-of-use, latency, and failure visibility. It never
 substitutes for a proof/adoption/use record.

### 5. Memory expansion

Qdrant should become the long-term semantic memory backend behind Luna’s memory
router layer, not a direct signer.

Final shape:

- current memory artifacts remain canonical truth
- Qdrant stores embeddings and recall candidates
- memory router chooses recall
- attestors only care whether a tier module genuinely uses that memory path

Rule:
Qdrant can support capability tiers, but use must be proven by runtime
invocation and by non-empty results coming back through a tier module.

### 6. Local inference fleet

LiteLLM should sit in front of LocalAI, vLLM, and llama.cpp and become the
single routing surface for non-authoritative model use.

Final shape:

- LiteLLM router controls:
  - model selection
  - fallback
  - load balancing
  - local-first policy
- model servers:
  - LocalAI for OpenAI-compatible tool/chat APIs
  - vLLM for higher-throughput hosted local models
  - llama.cpp for lightweight offline fallback
- consumers:
  - `luna_council_advisor`
  - helper routers
  - planner/synthesizer/factory advisory paths

Rule:
No authoritative signer may depend on LiteLLM or the local model fleet to
declare a tier complete.

### 7. Self-audit gate

Semgrep and SonarQube should become pre-attestor safety gates for generated or
modified code.

Final shape:

- Semgrep runs locally and fast on every candidate code path.
- SonarQube provides deeper code-quality/security trend analysis.
- their outputs become evidence refs in:
  - repair candidates
  - author/factory outputs
  - tier module proposals

Rule:
Static-analysis pass is necessary but never sufficient. Attestors still require
real runtime use.

## Phased Integration Mapping

### Phase 1: Nervous System and Survival

Scope:

- WinSW
- NATS
- minimal OpenTelemetry bootstrap

Why first:

- Luna needs stable process survival and a durable message spine before adding
  heavier orchestration or new cognition layers.

Attestor criteria:

- WinSW: Luna runtime starts under service control; watchdog heartbeats continue
  across restart; service restart produces no false "already serving" state.
- NATS: `luna_agent_bus.publish()` writes to NATS and replayed events are
  consumed by at least one internal loop; JSONL mirror still matches.
- OTel bootstrap: message bus, watchdog, and chat path emit spans/events into a
  local collector or file exporter; evidence refs point to trace IDs.

### Phase 2: Durable Execution

Scope:

- Temporal
- expanded OpenTelemetry around workflows

Why second:

- Once process survival and transport are stable, the most fragile remaining
  piece is task durability and tier-review orchestration.

Attestor criteria:

- repair/adopt/use jobs are driven by Temporal workflows, not only poll loops
- interrupted jobs resume without duplicate completion
- tier review loop survives restart and continues from persisted workflow state
- attestors are still the sole writers of non-synthetic adoption/use artifacts

### Phase 3: Brain Expansion

Scope:

- LiteLLM
- LocalAI
- vLLM
- llama.cpp
- Qdrant

Why third:

- cognition scaling only makes sense after transport and execution are durable

Attestor criteria:

- a tier module uses LiteLLM-routed local inference in a real loop
- a tier module uses Qdrant-backed recall and returns non-empty evidence
- fallback between local providers is observable and bounded
- no authoritative tier signer depends on advisory model output

### Phase 4: Autonomous Self-Audit

Scope:

- Semgrep
- SonarQube

Why fourth:

- once Luna is generating more of her own code and routing through a broader
  local stack, autonomous self-audit must tighten before higher-tier adoption

Attestor criteria:

- candidate code paths produce Semgrep/Sonar evidence refs
- failing static-analysis blocks adoption/use
- audit outputs are surfaced in operator truth and tier review results

### Phase 5: Full-stack bounded autonomy

Scope:

- all prior systems working together in one bounded-autonomous loop

Attestor criteria:

- Luna survives service restarts
- messages persist and replay through NATS
- workflows resume through Temporal
- local inference routing is used live through LiteLLM
- Qdrant recall is exercised in a real tier module
- static audit runs before attestation
- proof/adoption/use remain non-synthetic

## Rebuild Campaign Plan

### Track A: active frontier

- unblock Tier 131 by creating a real tier module/spec candidate discoverable by
  the strict attestors
- produce `adoption_records/131.json`
- produce `use_attestations/131.json`

### Track B: legacy canonical gap backlog

Rebuild these tiers with real artifacts, in ascending order:

- 15
- 17
- 19
- 31
- 38
- 41

For each tier:

1. verify whether a real tier module exists
2. if not, author a grounded tier module/spec
3. rerun strict runtime proof
4. rerun adoption attestor
5. rerun use attestor
6. only then mark the tier complete

### Track C: registry reconciliation

- demote `tier_proof_registry.json` from any current-frontier authority role
- keep it as historical lifecycle evidence
- make `tier_rebuild_frontier.json` the rebuild truth
- only restore unified truth when canonical tier folders and frontier agree

## First Critical Integration Recommendation

The first infrastructure integration should be **Phase 1: WinSW + NATS
bootstrap**, with OTel bootstrap included for proof.

Why this first:

- WinSW stabilizes unattended runtime survival.
- NATS replaces the weakest current enterprise gap: file-backed message
  continuity.
- OTel gives us the evidence needed to prove the integration is genuinely live.

Why not Temporal first:

- Temporal on top of a still-fragile service shell and file bus would add
  complexity before survival and transport are stable.

Why not LiteLLM/Qdrant first:

- those expand cognition, but do not solve the current runtime durability spine.

## Step 4 Entry Condition

Do not implement Step 4 until approved.

When approved, Step 4 should wire:

1. WinSW service wrapper for the Luna runtime root
2. NATS sidecar + minimal subject contract
3. `luna_agent_bus` bridge adapter that mirrors validated messages to NATS
4. OTel bootstrap spans for bus + watchdog + chat path

That is the smallest honest slice that materially upgrades Luna’s unattended
runtime without touching the strict attestor boundary.
