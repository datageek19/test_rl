# Gemini CLI + ADK Playbook: Building a CX Agent from Zero to Production

**For:** Koodo Mobile GECX Agent (or any telecom CX agent)
**Stack:** Gemini CLI, ADK (Python), Vertex AI, Cloud Run, CX Agent Studio
**Grounding:** Every command verified against ADK docs, adk-python GitHub, Google Cloud tutorials, and Gemini CLI blog posts as of April 2026.

---

## Phase 0: Environment Setup

### 0.1 Install toolchain

```bash
# Python 3.11+ required
python --version  # must be >= 3.11

# Install ADK
pip install google-adk

# Install Gemini CLI (requires Node.js 18+)
npm install -g @anthropic-ai/gemini-cli
# Or via: npx @anthropic-ai/gemini-cli

# Install gcloud CLI (if not already)
# https://cloud.google.com/sdk/docs/install

# Verify ADK
adk --version
```

### 0.2 Authenticate

```bash
# Option A: Vertex AI (recommended for production)
gcloud auth application-default login
export GOOGLE_CLOUD_PROJECT="your-project-id"
export GOOGLE_CLOUD_LOCATION="us-central1"

# Option B: Google AI Studio (quick prototyping, no GCP project needed)
export GOOGLE_GENAI_API_KEY="your-api-key"
```

### 0.3 Enable GCP APIs (for Vertex AI path)

```bash
gcloud services enable \
  aiplatform.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  --project=$GOOGLE_CLOUD_PROJECT
```

---

## Phase 1: Scaffold & Develop (Gemini CLI as pair programmer)

### 1.1 Bootstrap project with `adk create`

```bash
mkdir koodo-gecx-agent && cd koodo-gecx-agent

# ADK scaffolds the package structure for you
adk create koodo_agent
# Choose: gemini-2.5-flash
# Choose: Vertex AI
# Accept project ID and region defaults
```

This creates:
```
koodo_agent/
├── __init__.py    # must export root_agent
├── agent.py       # agent definition
└── .env           # credentials
```

### 1.2 Use Gemini CLI to write agent code

Open Gemini CLI in your project directory. The ADK's `llms-full.txt`
file gives Gemini CLI deep framework knowledge.

```bash
# Start Gemini CLI
gemini

# Then prompt it:
```

**Prompt 1 — Agent definition:**
```
Read the koodo_agent/ directory structure. I need a Koodo Mobile customer
support agent with these tools: get_plans, get_device, get_devices_catalog,
get_billing, get_promotions, check_network_status, create_support_ticket,
get_account. Each tool should be a Python function with type hints and
docstrings (ADK auto-wraps them as FunctionTools). Use mock data reflecting
real Koodo pricing: 5G 20GB at $40/mo, 80GB at $50/mo, Galaxy A57 at $0
upfront with $15/mo Tab. Write agent.py and tools.py.
```

**Prompt 2 — Iterate on instructions:**
```
The agent instruction needs to capture Koodo's brand voice: friendly,
casual, uses words like "awesome", "no sweat", "happy". Add rules about
the Koodo Tab (no fixed contracts), Pick Your Perk (4 options), $80
connection fee waiver for online orders, and $25 referral credits.
Update the instruction in agent.py.
```

**Prompt 3 — Add design pipeline:**
```
Add a SequentialAgent called design_pipeline with three sub-agents:
planner, extractor, designer. Each should use output_key to pass state
to the next agent. The planner outputs task_plan, the extractor reads
{task_plan} and outputs extraction, the designer reads {extraction}
and outputs cuj_designs. This is for design-time CUJ generation only.
```

### 1.3 Test locally in CLI mode

```bash
# Run in terminal — interactive REPL
adk run koodo_agent

# Try these conversations:
# > What plans do you have?
# > Show me the Samsung Galaxy A57
# > Why did my bill go up? My account is ACC-001
# > I have no signal on my phone
# > Any deals right now?
```

### 1.4 Test in web UI

```bash
# Launch ADK Dev UI at http://localhost:8000
adk web koodo_agent

# Or with auto-reload during development
adk web koodo_agent --reload_agents
```

The Dev UI lets you:
- Select agents from a dropdown (koodo_agent, design_pipeline)
- View event traces (tool calls, state changes, LLM reasoning)
- Inspect session state
- Debug tool execution step by step

---

## Phase 2: Evaluate

### 2.1 Create evaluation dataset

Create a file `koodo_agent/eval/koodo_eval.evalset.json`:

```json
[
  {
    "name": "plan_browsing_basic",
    "data": [
      {
        "query": "What plans do you have?",
        "expected_tool_use": [
          {
            "tool_name": "get_plans",
            "tool_input": {}
          }
        ],
        "reference": "The agent should list available plans with pricing"
      }
    ]
  },
  {
    "name": "device_lookup",
    "data": [
      {
        "query": "How much is the Samsung Galaxy A57?",
        "expected_tool_use": [
          {
            "tool_name": "get_device",
            "tool_input": {
              "device_id": "samsung-galaxy-a57"
            }
          }
        ],
        "reference": "$0 upfront with $15/mo Happy Tab"
      }
    ]
  },
  {
    "name": "billing_requires_auth",
    "data": [
      {
        "query": "Show me my bill",
        "reference": "The agent should ask for account verification before showing billing"
      }
    ]
  },
  {
    "name": "promotions_check",
    "data": [
      {
        "query": "Any deals right now?",
        "expected_tool_use": [
          {
            "tool_name": "get_promotions",
            "tool_input": {}
          }
        ],
        "reference": "Should mention referral bonus, connection fee waiver, Stream+ bundle"
      }
    ]
  },
  {
    "name": "troubleshooting_flow",
    "data": [
      {
        "query": "I have no signal on my phone",
        "expected_tool_use": [
          {
            "tool_name": "check_network_status",
            "tool_input": {}
          }
        ],
        "reference": "Should check network status and provide troubleshooting steps"
      }
    ]
  },
  {
    "name": "multi_turn_purchase",
    "data": [
      {
        "query": "I want to buy a new phone",
        "expected_intermediate_agent_responses": [
          "The agent should ask about preferences or show catalog"
        ]
      },
      {
        "query": "Show me Samsung phones under $20/month",
        "expected_tool_use": [
          {
            "tool_name": "get_devices_catalog",
            "tool_input": {
              "brand": "Samsung",
              "max_monthly": 20.0
            }
          }
        ],
        "reference": "Should show Galaxy A57 ($15/mo) and Galaxy A16 ($4/mo)"
      }
    ]
  },
  {
    "name": "out_of_scope_guardrail",
    "data": [
      {
        "query": "What is the capital of France?",
        "reference": "Agent should redirect to Koodo-related help, not answer general knowledge"
      }
    ]
  },
  {
    "name": "escalation_to_human",
    "data": [
      {
        "query": "I want to cancel my service",
        "reference": "Agent should acknowledge and offer to schedule callback with retention team"
      }
    ]
  }
]
```

### 2.2 Run evaluation

```bash
# Basic eval
adk eval koodo_agent koodo_agent/eval/koodo_eval.evalset.json

# With detailed output
adk eval koodo_agent koodo_agent/eval/koodo_eval.evalset.json \
  --print_detailed_results

# With a scoring config (optional)
adk eval koodo_agent koodo_agent/eval/koodo_eval.evalset.json \
  --config_file koodo_agent/eval/eval_config.json
```

### 2.3 Eval config (optional)

Create `koodo_agent/eval/eval_config.json`:

```json
{
  "pass_threshold": 0.7,
  "criteria": [
    {
      "name": "tool_trajectory_avg_score",
      "weight": 1.0
    },
    {
      "name": "response_match_score",
      "weight": 0.5
    }
  ]
}
```

### 2.4 Use Gemini CLI to iterate on failures

```bash
gemini

# Prompt:
# "The eval for 'billing_requires_auth' failed — the agent called
# get_billing without asking for verification first. Update the
# instruction in agent.py to require phone number confirmation
# before any billing or account tool calls."
```

Iterate: edit instruction → `adk eval` → fix → repeat until pass rate ≥ threshold.

---

## Phase 3: Deploy

### 3.1 Deploy to Cloud Run (one command)

```bash
# ADK built-in deploy — packages agent, builds container, deploys
adk deploy cloud_run koodo_agent \
  --project=$GOOGLE_CLOUD_PROJECT \
  --region=us-central1

# With web UI bundled (for internal testing)
adk deploy cloud_run koodo_agent \
  --project=$GOOGLE_CLOUD_PROJECT \
  --region=us-central1 \
  --with_ui
```

This command:
1. Creates a Dockerfile from the agent package
2. Builds the container via Cloud Build
3. Pushes to Artifact Registry
4. Deploys to Cloud Run
5. Prints the service URL

### 3.2 Deploy webhook separately (for GECX path)

If connecting to CX Agent Studio via OpenAPI tools, deploy the
webhook Flask service:

```bash
cd webhook/
gcloud run deploy koodo-webhook \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --project=$GOOGLE_CLOUD_PROJECT

# Note the service URL from output — you'll need it for GECX
```

### 3.3 Test deployed agent

```bash
# Health check
curl https://koodo-agent-HASH-uc.a.run.app/health

# If deployed with --with_ui, open in browser:
# https://koodo-agent-HASH-uc.a.run.app
```

---

## Phase 4: Connect to CX Agent Studio (GECX)

### Path A: OpenAPI tools (webhook-based)

1. Open **console.cloud.google.com** → Customer Engagement AI → CX Agent Studio
2. **Create agent application** → name it "Koodo Virtual Agent"
3. **Paste agent instruction** from `agent.py` (the `KOODO_INSTRUCTION` string)
4. **Add tools** — for each OpenAPI spec in `koodo_agent/openapi/`:
   - Tools panel → Create Tool → OpenAPI
   - Paste the YAML spec
   - Set server URL to your Cloud Run webhook URL
   - Save
5. **Add guardrails** — Guardrails panel → Create:
   - "no_competitor_pricing": Don't quote specific competitor prices
   - "no_unauthorized_account_access": Require verification before account data
   - "scope_boundary": Redirect non-Koodo topics
6. **Test** in Simulator panel
7. **Deploy** via Web Widget or Telephony channel

### Path B: ADK agent on Vertex AI Agent Engine (bring your own agent)

```bash
# Deploy to Agent Engine
adk deploy agent_engine koodo_agent \
  --project=$GOOGLE_CLOUD_PROJECT \
  --region=us-central1

# Then register in Gemini Enterprise:
# Console → Gemini Enterprise → Agents → Add agent → Custom agent via Agent Engine
```

### Path C: CX Agent Studio MCP server (programmatic)

For CI/CD or Gemini CLI-driven setup, use the GECX MCP server:

```bash
# Enable MCP endpoint
gcloud beta services mcp enable ces.googleapis.com \
  --project=$GOOGLE_CLOUD_PROJECT

# Then from Gemini CLI or any MCP client, you can:
# - Create/update agent apps
# - Add/modify tools
# - Export/import configurations
# - Run evaluations
```

---

## Phase 5: CI/CD Pipeline

### 5.1 Automated eval in CI

```yaml
# .github/workflows/agent-ci.yml (or Cloud Build equivalent)
name: Agent CI
on: [push]
jobs:
  eval:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install google-adk
      - run: |
          export GOOGLE_GENAI_API_KEY=${{ secrets.GEMINI_API_KEY }}
          adk eval koodo_agent koodo_agent/eval/koodo_eval.evalset.json
```

### 5.2 Cloud Build deploy pipeline

```yaml
# cloudbuild.yaml
steps:
  # Run evals
  - name: 'python:3.12'
    entrypoint: bash
    args:
      - -c
      - |
        pip install google-adk
        adk eval koodo_agent koodo_agent/eval/koodo_eval.evalset.json

  # Deploy if evals pass
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: bash
    args:
      - -c
      - |
        pip install google-adk
        adk deploy cloud_run koodo_agent \
          --project=$PROJECT_ID \
          --region=us-central1
```

---

## Command Reference (Verified ADK CLI)

| Command | What it does |
|---------|-------------|
| `adk create <name>` | Scaffold new agent project with prompts for model/backend |
| `adk run <agent_dir>` | Interactive CLI REPL for testing |
| `adk web <agent_dir>` | Dev UI at localhost:8000 (agent selector, event trace, state inspector) |
| `adk web <agent_dir> --reload_agents` | Dev UI with hot-reload on code changes |
| `adk api_server <agent_dir>` | Headless FastAPI server (REST API, no UI) |
| `adk eval <agent_dir> <evalset.json>` | Run evaluation against test cases |
| `adk eval <agent_dir> <evalset.json> --print_detailed_results` | Eval with verbose output |
| `adk deploy cloud_run <agent_dir> --project=X --region=Y` | One-command Cloud Run deploy |
| `adk deploy cloud_run <agent_dir> --with_ui` | Deploy with bundled Dev UI |
| `adk deploy agent_engine <agent_dir>` | Deploy to Vertex AI Agent Engine |

---

## Gemini CLI Workflow Summary

```
┌─────────────────────────────────────────────────────────────────┐
│ DEVELOP                                                          │
│                                                                  │
│  gemini "write agent.py with Koodo tools"                       │
│  gemini "add pick-your-perk logic to the instruction"           │
│  gemini "create eval test for billing auth guardrail"           │
│       │                                                          │
│       ▼                                                          │
│  adk run koodo_agent          ← quick CLI test                  │
│  adk web koodo_agent          ← visual debugging                │
│       │                                                          │
│       ▼                                                          │
├─────────────────────────────────────────────────────────────────┤
│ EVALUATE                                                         │
│                                                                  │
│  adk eval koodo_agent eval/koodo_eval.evalset.json              │
│       │                                                          │
│       ├── PASS → proceed to deploy                              │
│       └── FAIL → gemini "fix the auth guardrail, eval says..."  │
│                  └── loop back to develop                        │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│ DEPLOY                                                           │
│                                                                  │
│  adk deploy cloud_run koodo_agent --project=X --region=Y        │
│       │                                                          │
│       ├── Direct use: Cloud Run URL + adk web UI                │
│       ├── GECX path: register OpenAPI tools in CX Agent Studio  │
│       └── Enterprise: deploy to Agent Engine → Gemini Enterprise│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Gotchas (Learned from Grounding Audit)

1. **`root_agent` must be exported** — ADK discovery looks for this exact name in `__init__.py`
2. **FunctionTools via type hints** — ADK auto-wraps plain Python functions; no manual `FunctionTool()` needed
3. **`output_key` for SequentialAgent** — this is how agents pass data between pipeline steps
4. **`{variable_name}` in instructions** — ADK template substitution reads from session state
5. **GECX has no single-file import** — setup is via console, REST API, or MCP server
6. **OpenAPI tools ≠ Dialogflow CX webhooks** — GECX OpenAPI tools make standard REST calls, not protobuf
7. **`adk deploy cloud_run` exists** — you don't need to write your own Dockerfile for the agent
8. **Eval before deploy** — `adk eval` checks both response quality and tool trajectory accuracy
