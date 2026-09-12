# CMA Middleware & Enterprise Model Context Protocol (MCP) Server

High-performance API Gateway, Terminal proxy, and Enterprise Model Context Protocol (MCP) server for interacting with the SAS IDeaS Client Management Application (CMA). It handles automated session management, cookie propagation, batch operations, real-time query execution on port `8555`, and an AI-native 14-tool Model Context Protocol server on port `8556`.

---

## 🌟 Key Architecture & Ports

| Service | Port | Protocol | Description |
|---|---|---|---|
| **CMA REST Gateway** | `8555` | HTTP REST | Upstream proxy to CMA Ad-Hoc SQL view with SSO cookie auto-sync |
| **Edge Virtual Desktop** | `6080` | noVNC / VNC | Headless Edge browser running in Xvfb for automated SSO login |
| **Edge CDP Debugger** | `9222` | Chrome DevTools | Internal interface used by `cookie_sync.py` to extract `JSESSIONID` |
| **CMA MCP Server** | `8556` | MCP (SSE / Stdio) | Enterprise AI MCP Server exposing 14 tools and 3 resources |

---

## 🛠️ Canonical MCP Tools (14 Tools)

The CMA MCP Server exposes 14 canonical tools providing comprehensive, future-proof access across G3 Global, Job, Ratchet, and 19,496+ Tenant databases:

### 1. Query & Batch Execution
- **`cma_execute_query(chain, query, timeout=35.0)`**: Executes arbitrary SQL on any chain (e.g. `global_PROD_1`, `job_PROD_1`, `ratchet_PROD_1`, or tenant chains like `Hilton-ATLFY`). Automatically strips SQL comments and normalizes tenant chain codes.
- **`cma_execute_batch(chains, queries, timeout=60.0)`**: Executes multiple SQL queries across multiple database chains concurrently in a single batch.

### 2. Chain Catalog & Indexing
- **`cma_search_chains(keyword, chain_type='all', limit=50)`**: Instant in-memory search across 19,496+ cached database chains by keyword, hotel code, or client identifier.
- **`cma_list_all_chains(chain_type='all', limit=100, offset=0)`**: Paginated chain browser with breakdown by category (`global`, `job`, `ratchet`, `tenant`).
- **`cma_get_chain_details(chain_name, test_connection=False)`**: Metadata lookup for a chain, with optional live ping (`SELECT 1`) connectivity verification.

### 3. High-Level Enterprise Domain Tools
- **`cma_get_property(property_id, property_code, client_code, global_chain='global_PROD_1', include_audit_history=True)`**: Deep property lookup joining `Property` and `Client`. Resolves raw stage codes to human-readable modes (e.g. `TWO_WAY` -> `Decision Delivery Mode`) and retrieves the last 10 audit changes from `Property_AUD`.
- **`cma_get_property_parameters(client_code, property_code, parameter_names, global_chain='global_PROD_1')`**: Cascading parameter matrix resolution engine across `pacman` -> `pacman.CLIENT` -> `pacman.CLIENT.PROP` contexts.
- **`cma_get_job_execution(job_name, job_chain='job_PROD_1', param_key, param_value, after_datetime, limit=10)`**: Inspects Spring Batch execution history in `job_PROD_x` (`JOB_INSTANCE`, `JOB_EXECUTION`, `JOB_EXECUTION_PARAMS`).
- **`cma_get_tenant_table_data(chain, table_name, where_clause, columns, order_by, limit=100)`**: Safe parameterized reader for tenant tables (`OCCUPANCY`, `TRANSACTIONS`, `RESERVATIONS`, `RATE_CODES`, etc.).

### 4. Infrastructure & Session Management
- **`cma_get_session_status()`**: Probes active cookie validity, circuit breaker status, and gateway health.
- **`cma_trigger_sso_refresh(force=False)`**: Triggers Edge Virtual Desktop SSO re-login to renew an expired `JSESSIONID`.
- **`cma_refresh_chains_cache()`**: Re-synchronizes and indexes all 19,496+ database chains.
- **`cma_get_audit_logs(limit=50, status_code=0, client_name="")`**: Queries query execution audit trail from `api_gateway.db`.
- **`cma_get_system_stats()`**: Telemetry report covering throughput, latencies, cached chains, and error counts.

---

## 📚 MCP Resources (3 URIs)

- `cma://system/status`: Real-time session and circuit breaker state.
- `cma://system/stats`: Throughput, database, and cache metrics.
- `cma://chains/summary`: Production environment inventory (`global`, `job`, `ratchet` chains).

---

## 🚀 Running the CMA MCP Server

### Option 1: Docker (Recommended)
```bash
# Start both CMA Gateway and CMA MCP Server
docker compose up -d --build
```
Verify container health:
```bash
curl http://localhost:8556/health
```

### Option 2: Native Python
```bash
# In SSE Mode
python cma_mcp_server.py --host 0.0.0.0 --port 8556

# In Stdio Mode (for Desktop LLM CLI)
python cma_mcp_server.py --transport stdio
```

---

## 🧪 Verification via Python MCP Client SDK

Run the end-to-end verification script:
```bash
python verify_cma_mcp.py
```
Outputs:
```text
================================================================================
 [*] CMA MCP SERVER PROTOCOL & TOOL VERIFICATION (Python MCP SDK)
================================================================================
[Step 1] Connecting to CMA MCP Server via SSE...
  [PASS] Health Check: Status=HEALTHY, Tools=14
[Step 2] Initializing MCP Session...
  [PASS] Initialized: Server='cma-mcp-server' Protocol=2025-11-25
[Step 3] Discovering Registered MCP Tools...
  [PASS] Discovered 14 Canonical Enterprise Tools
[Step 4] Testing MCP Resources...
  [PASS] Discovered 3 Resources (cma://system/status, cma://system/stats, cma://chains/summary)
...
 [SUCCESS] ALL 13 TEST STEPS COMPLETED SUCCESSFULLY!
```
