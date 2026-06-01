# Phase 1 Infrastructure Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Phase 1 honest and live by wiring real NATS publish under `luna_agent_bus`, explicit OpenTelemetry traces across bus/watchdog/chat, and a Tier 131 infrastructure-use module that strict attestors can actually exercise.

**Architecture:** Preserve the existing Luna truth boundary. `luna_agent_bus.py` remains the policy gate, `luna_nats_adapter.py` becomes the transport bridge, WinSW remains the service shell, and a new Tier 131 module proves active utilization by checking live NATS, telemetry, and service-hosted runtime status together. No synthetic tier truth writes.

**Tech Stack:** Python 3.11, WinSW service XML/install scripts, `nats-py`, local `nats-server.exe`, existing `luna_otel.py`, strict attestors, pytest.

---

### Task 1: Add failing tests for the real missing Phase 1 behavior

**Files:**
- Modify: `D:\SurgeApp\tests\test_luna_full_stack_install_static.py`
- Modify: `D:\SurgeApp\tests\test_luna_infrastructure_upgrade_static.py`
- Create: `D:\SurgeApp\tests\test_phase1_runtime_spine_static.py`

- [ ] **Step 1: Write failing tests for real NATS publish + bus wiring**
- [ ] **Step 2: Write failing tests for OTel instrumentation on agent-bus publish**
- [ ] **Step 3: Write failing tests for Tier 131 infrastructure runtime module**
- [ ] **Step 4: Run targeted pytest and confirm failure reasons are the intended missing wires**

### Task 2: Implement the transport and telemetry wires

**Files:**
- Modify: `D:\SurgeApp\luna_modules\luna_nats_adapter.py`
- Modify: `D:\SurgeApp\luna_modules\luna_agent_bus.py`
- Modify: `D:\SurgeApp\luna_modules\luna_otel.py`

- [ ] **Step 1: Implement a real best-effort NATS publish path while keeping JSONL fallback**
- [ ] **Step 2: Add transport status helpers that can prove server reachability and successful publish attempts**
- [ ] **Step 3: Emit explicit OTel spans/events/counters from the agent-bus publish path**
- [ ] **Step 4: Re-run targeted pytest until the new transport/telemetry tests pass**

### Task 3: Implement the Tier 131 active-utilization module

**Files:**
- Create: `D:\SurgeApp\luna_modules\luna_tier131_phase1_runtime_spine.py`
- Modify: `D:\SurgeApp\luna_modules\luna_runtime_ownership.py`
- Modify: `D:\SurgeApp\luna_modules\luna_terminal_truth.py` (only if needed for surface/reporting)

- [ ] **Step 1: Add a Tier 131 module with `adoption_probe()` and `use_probe()`**
- [ ] **Step 2: Make the probes verify all three active-utilization conditions**
- [ ] **Step 3: Surface Phase 1 runtime state in ownership/status output if needed**
- [ ] **Step 4: Re-run targeted pytest until the Tier 131 module tests pass**

### Task 4: Activate the live runtime dependencies and prove them

**Files:**
- Modify if needed: `D:\SurgeApp\tools\Install_External_Servers.ps1`
- Modify if needed: `D:\SurgeApp\services\Install_Luna_Services.ps1`

- [ ] **Step 1: Install `nats-py` and fetch `nats-server.exe` if missing**
- [ ] **Step 2: Start a real local NATS server and verify port `4222`**
- [ ] **Step 3: Attempt WinSW activation if the environment permits it; if not, preserve the service shell and report the exact privilege blocker**
- [ ] **Step 4: Run live checks proving NATS events, OTel spans, and service/runtime status**

### Task 5: Invoke strict attestors only after proof

**Files:**
- Runtime artifacts only under `D:\SurgeApp\memory\tier_truth\`

- [ ] **Step 1: Run the Tier 131 module probes manually and capture evidence**
- [ ] **Step 2: Invoke `luna_tier_adoption_attestor` for Tier 131**
- [ ] **Step 3: Invoke `luna_tier_use_attestor` for Tier 131**
- [ ] **Step 4: Verify the written records are non-synthetic and tied to live Phase 1 evidence**

### Task 6: Final verification and reporting

**Files:**
- Modify: `D:\SurgeApp\memory\core_brain\luna_full_stack_install_and_integration_report.md`
- Modify: `D:\SurgeApp\memory\core_brain\luna_infrastructure_upgrade_report.md` (only if needed)

- [ ] **Step 1: Run the focused pytest suites for infrastructure, watchdog, chat bridge, and new Phase 1 tests**
- [ ] **Step 2: Run live runtime checks for NATS events, OTel spans, and current Tier 131 truth**
- [ ] **Step 3: Update the report with what became truly live versus what remains staged**
- [ ] **Step 4: Return a concise Phase 1 completion report and stop for Phase 2 authorization**
