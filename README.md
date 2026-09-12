# CMA Middleware & Enterprise Model Context Protocol (MCP) Server

High-performance API Gateway, Terminal proxy, and Enterprise Model Context Protocol (MCP) server for interacting with the SAS IDeaS Client Management Application (CMA). It handles automated session management, cookie propagation, batch operations, real-time query execution on port `8555`, and an AI-native 34-tool Model Context Protocol server on port `8556`.

---

## 🌟 Architecture & Ports

| Service | Port | Protocol | Description |
|---|---|---|---|
| **CMA REST Gateway** | `8555` | HTTP REST | Upstream proxy to CMA Ad-Hoc SQL view with SSO cookie auto-sync |
| **Edge Virtual Desktop** | `6080` | noVNC / VNC | Headless Edge browser running in Xvfb for automated SSO login |
| **Edge CDP Debugger** | `9222` | Chrome DevTools | Internal interface used by `cookie_sync.py` to extract `JSESSIONID` |
| **CMA MCP Server** | `8556` | MCP (SSE / Stdio) | Enterprise AI MCP Server exposing 34 tools and 3 resources |

---

## 🛠️ Canonical MCP Tools Suite (34 Tools)

The CMA MCP Server provides 34 canonical tools across 11 enterprise domains:

### 1. Core SQL Execution Engine (2 Tools)
- **`cma_execute_query(chain, query, timeout=35.0)`**: Executes arbitrary SQL on any chain (e.g. `global_PROD_1`, `job_PROD_1`, `ratchet_PROD_1`, or tenant chains like `Hilton-ATLFY`). Automatically strips SQL comments and normalizes tenant chain codes.
- **`cma_execute_batch(chains, queries, timeout=60.0)`**: Executes multiple SQL queries across multiple database chains concurrently in a single batch.

### 2. Chain Catalog & Environment Resolution (4 Tools)
- **`cma_search_chains(keyword, chain_type='all', limit=50)`**: Instant in-memory search across 19,496+ cached database chains by keyword, hotel code, or client identifier in `<5ms`.
- **`cma_list_all_chains(chain_type='all', limit=100, offset=0)`**: Paginated chain browser with breakdown by category (`global`, `job`, `ratchet`, `tenant`).
- **`cma_get_chain_details(chain_name, test_connection=False)`**: Metadata lookup for a chain, with optional live ping (`SELECT 1`) connectivity verification.
- **`cma_resolve_tenant_environment(client_code, property_code)`**: Automatically identifies which G3 production cluster (`PROD_1..6`) hosts a client or hotel.

### 3. Schema & Metadata Reflection (2 Tools)
- **`cma_list_tables(chain, pattern='*')`**: Wildcard table/view search across any chain (`INFORMATION_SCHEMA.TABLES`).
- **`cma_describe_table(chain, table_name)`**: Column types, nullability, character lengths, and default values (`INFORMATION_SCHEMA.COLUMNS`).

### 4. Global Property & Interface Configuration (4 Tools)
- **`cma_get_property(property_id, property_code, client_code, global_chain, include_audit_history)`**: Deep property lookup joining `Property` and `Client`. Resolves stage modes (`TWO_WAY` -> `Decision Delivery Mode`) and inspects `Property_AUD`.
- **`cma_get_property_parameters(client_code, property_code, parameter_names, global_chain)`**: Cascading parameter matrix resolution engine across `pacman` -> `pacman.CLIENT` -> `pacman.CLIENT.PROP` contexts.
- **`cma_get_datafeed_config(property_id, client_code, global_chain)`**: Queries PMS/RMS interface configurations, datafeed endpoints, and FTP settings.
- **`cma_get_client_portfolio(client_code, global_chain)`**: Retrieves all properties owned or operated by a client group across G3 Global databases.

### 5. Spring Batch Processing & Job Triage (4 Tools)
- **`cma_get_job_execution(job_name, job_chain, param_key, param_value, after_datetime, limit)`**: Inspects Spring Batch execution history in `job_PROD_x`.
- **`cma_get_failed_jobs(job_chain, hours_back, limit)`**: Retrieves failed batch executions with step-level error traces and exit codes.
- **`cma_get_blocked_jobs(job_chain)`**: Checks for stuck, blocked, or throttled batch jobs in `Blocked_Job` and `JOB_STATE`.
- **`cma_get_job_daily_stats(job_chain, days_back)`**: Aggregates daily job throughput, success rates, and average durations from `DAILY_JOB_STATISTICS`.

### 6. Tenant Revenue & Pacing Metrics (4 Tools)
- **`cma_get_tenant_table_data(chain, table_name, where_clause, columns, order_by, limit)`**: Safe parameterized reader for tenant tables.
- **`cma_get_tenant_revenue_summary(tenant_chain, start_date, end_date)`**: Aggregates rooms sold, revenue, and ADR from `Accom_Activity` for discrepancy audits.
- **`cma_get_tenant_pace_data(tenant_chain)`**: Retrieves latest pacing snapshot from `PACE_Accom_Activity`.
- **`cma_get_ratchet_srp_mappings(client_code, property_code, ratchet_chain)`**: Queries Standard Rate Plan (SRP) mappings and channel restrictions in Ratchet DB.

### 7. Salesforce Case $\leftrightarrow$ CMA Execution Audit (2 Tools)
- **`cma_get_sfdc_case_audit_history(case_number, start_date, end_date)`**: Searches CMA portal audit logs for every query executed against a specific Salesforce Case Number.
- **`cma_get_audit_query_text(audit_id)`**: Retrieves the exact SQL query text, parameter hints, and execution metadata for a given CMA audit transaction.

### 8. Curated Engineering Scripts Repository (2 Tools)
- **`cma_search_saved_queries(keyword, tag, query_type, limit)`**: Searches the 2,000+ curated engineering SQL queries saved in CMA by SFDC Case, keyword, or tag (`L2 Support`, `IM`, `Overbooking`, `FPLOS`, `Pace Alert`, `Casper`).
- **`cma_get_saved_query_details(query_id)`**: Retrieves full parameterized SQL code, assigned roles, and parameters for a saved query.

### 9. Real-Time Team Diagnostics & Tasks (2 Tools)
- **`cma_get_team_task_feed(limit)`**: Retrieves live feed of queries and background tasks currently being executed across the team in CMA.
- **`cma_get_task_status_by_job_id(job_id)`**: Searches and inspects real-time progress of a CMA background batch task by its Job ID.

### 10. Remote Server & Log Explorer (1 Tool)
- **`cma_list_server_explorer_directories()`**: Discovers available remote server explorer nodes (`G3 Prod 1..6 Data` and `RSS Prod 1..6`).

### 11. Configuration, Schedules & Maintenance (7 Tools)
- **`cma_get_property_system_parameters_catalog()`**: Master catalog of Property System Parameters (PSPs) and active Cognito JWT tokens.
- **`cma_get_scheduled_sql_jobs()`**: Inspects active recurring scheduled SQL jobs, execution frequencies, and target chains.
- **`cma_get_session_status()`**: Probes active cookie validity, circuit breaker status, and gateway health.
- **`cma_trigger_sso_refresh(force)`**: Triggers Edge Virtual Desktop SSO re-login to renew an expired `JSESSIONID`.
- **`cma_refresh_chains_cache()`**: Re-synchronizes and indexes all 19,496+ database chains.
- **`cma_get_audit_logs(limit, status_code, client_name)`**: Queries query execution audit trail from `api_gateway.db`.
- **`cma_get_system_stats()`**: Telemetry report covering throughput, latencies, cached chains, and error counts.

---

## 📚 MCP Resources (3 URIs)

- `cma://system/status`: Real-time session and circuit breaker state.
- `cma://system/stats`: Throughput, database, and telemetry metrics.
- `cma://chains/summary`: Production environment inventory (**19,496 total**: 6 Global, 5 Job, 5 Ratchet, 19,480 Tenant chains).

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
  [PASS] Health Check: Status=HEALTHY, Tools=34
[Step 2] Initializing MCP Session...
  [PASS] Initialized: Server='cma-mcp-server' Protocol=2025-11-25
[Step 3] Discovering Registered MCP Tools...
  [PASS] Discovered 34 Canonical Enterprise Tools
[Step 4] Testing MCP Resources...
  [PASS] Discovered 3 Resources (cma://system/status, cma://system/stats, cma://chains/summary)
...
 [SUCCESS] ALL 20 TEST STEPS COMPLETED SUCCESSFULLY!
```
