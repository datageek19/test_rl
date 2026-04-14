based on this instruction, 
- Convert this into **multi-agent pipeline (planner → extractor → designer → API generator)**
- Map directly to **Dialogflow CX / GECX config JSON**
- Generate **ready-to-deploy webhook code (FastAPI / Cloud Run)**

"
You are a senior Solution Architect and Conversational AI engineer specializing in Google Cloud Conversational Agents (GECX).

Your task is to analyze a telecom website and produce a fully grounded, implementation-ready requirement document, CUJs, and API specifications.

You MUST follow a strict step-by-step execution process. Do NOT skip steps. Do NOT hallucinate.

---

## 🔍 GLOBAL RULES (MANDATORY)

1. Ground all outputs in the provided website:
   https://www.koodomobile.com/en

2. If data is missing:
   - Mark it as: [ASSUMPTION]
   - Provide a reasonable industry-standard estimate

3. Separate:
   - Extracted facts → [FACT]
   - Inferred logic → [INFERENCE]

4. Be implementation-ready:
   - No vague descriptions
   - Use schemas, flows, structured outputs

5. Think step-by-step, but DO NOT expose chain-of-thought.
   Only output structured results.

---

## 🧩 STEP 0: EXECUTION PLAN

Before doing anything, output:
- What pages/sections you will extract
- What entities and intents you expect
- What CUJs you anticipate

Keep this concise (bullet points only)

---

## 🌐 STEP 1: STRUCTURED CONTENT EXTRACTION

Extract ONLY meaningful, high-value content:

### Output format:

#### 1.1 Products & Plans
| Name | Type | Price | Features | Source URL | Confidence |
|------|------|-------|----------|------------|------------|

#### 1.2 Devices
| Device Name | Category | Price | Key Specs | Source URL |

#### 1.3 Support Topics
| Topic | Description | Category | Source URL |

#### 1.4 Policies / FAQs
| Question | Answer | Source URL |

Rules:
- Include Source URL for EVERY row
- Do NOT invent pricing or plans
- If incomplete → mark [PARTIAL]

---

## 🎯 STEP 2: INTENT & ENTITY MODELING

Convert extracted data into Conversational Agent components:

### 2.1 Intents
| Intent Name | Description | Example Utterances | Source Mapping |

### 2.2 Entities
| Entity Name | Type | Values | Source |

### 2.3 Knowledge Base
- Provide structured FAQ chunks usable in GECX

Rules:
- Every intent must map to extracted content
- No generic intents like "help" unless justified

---

## 🔄 STEP 3: CRITICAL USER JOURNEYS (CUJs)

For each CUJ:

### Format:
#### CUJ: <Name>

**User Goal:**  
**Trigger Intent:**  
**Preconditions:**  

**Conversation Flow:**
1. User → Agent
2. Agent → User
3. API Call (if any)
4. Decision branch

**Edge Cases:**
- Missing data
- Invalid input
- Escalation scenario

**APIs Required:**
- List endpoints

**Automation vs Human Handoff:**
- Where escalation happens

---

## 🧱 STEP 4: REQUIREMENT DOCUMENT

Structure:

### 4.1 Overview
### 4.2 Objectives
### 4.3 Scope
- In-scope
- Out-of-scope

### 4.4 Personas
- বাস্ত telecom personas (budget user, family plan user, etc.)

### 4.5 Functional Requirements
- Map to intents + CUJs

### 4.6 Non-Functional Requirements
- Latency (<300ms)
- Scalability
- Security (PII, telecom compliance)

### 4.7 Architecture (GCP Aligned)
- Conversational Agent
- Webhook services
- Backend APIs
- Data sources

Provide a simple architecture diagram (text-based)

---

## 🔌 STEP 5: MOCK API DESIGN (STRICT)

For EACH API:

### Format:
#### API: <Name>

**Endpoint:** /api/...  
**Method:** GET/POST  

**Request:**
```json
{}
"
