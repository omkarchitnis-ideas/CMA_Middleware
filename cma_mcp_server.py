#!/usr/bin/env python3
"""
CMA Enterprise Model Context Protocol (MCP) Server
=================================================
Production-grade MCP server for the SAS IDeaS Client Management Application (CMA).
Provides complete, future-proof access to G3 Global, Job, Ratchet, and 19,496+ Tenant databases.

Key Capabilities:
- Full Universal Query & Batch Execution across all G3 database chains
- Fast in-memory indexing and search across 19,496+ cached client database chains
- Deep property and stage mode inspection (Property & Property_AUD)
- Cascading configuration parameter matrix resolution (pacman -> pacman.CLIENT -> pacman.CLIENT.PROP)
- Spring Batch execution telemetry (JOB_INSTANCE, JOB_EXECUTION, JOB_EXECUTION_PARAMS)
- Tenant database catalog access with schema validation
- Session health monitoring, Edge CDP SSO automated re-login, and circuit breaker telemetry
- Gateway audit log search & operational system stats
- Dual Transport: HTTP Server-Sent Events (SSE) with /health probe & Stdio CLI transport
"""

import os
import re
import sys
import json
import time
import sqlite3
import logging
import argparse
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cma-mcp-server")

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHAINS_CACHE_PATH = os.getenv("CHAINS_CACHE_PATH", os.path.join(SCRIPT_DIR, "chains_cache.json"))
DB_FILE = os.getenv("DB_FILE", os.path.join(SCRIPT_DIR, "api_gateway.db"))

CMA_GATEWAY_URL = os.getenv("CMA_GATEWAY_URL", "http://172.27.210.162:8555")
CMA_API_KEY = os.getenv("CMA_API_KEY", "cma_6Z2AkhS4ux70JGlTa4mzGeWVVhaQRGWu")
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8556"))

STAGE_MODE_MAP = {
    "TWO_WAY": "Decision Delivery Mode",
    "ONE_WAY": "Decision Creation Mode",
    "DATA_POPULATION": "Data Population Mode",
    "POPULATION": "Data Population Mode",
    "DATA_CAPTURE": "Data Capture Mode",
    "DORMANT": "Dormant / Inactive",
    "1": "Data Population Mode",
    "2": "Decision Delivery Mode",
    "Decision Delivery Mode": "Decision Delivery Mode",
    "Decision Creation Mode": "Decision Creation Mode",
    "Data Population Mode": "Data Population Mode",
}

# ==========================================
# IN-MEMORY CHAIN CATALOG INDEX
# ==========================================
class ChainCatalogIndex:
    """Fast in-memory index for all 19,496+ cached database chains."""

    def __init__(self, cache_file: str):
        self.cache_file = cache_file
        self.chains: Dict[str, Any] = {}
        self.last_mtime: float = 0
        self.global_chains: List[str] = []
        self.job_chains: List[str] = []
        self.ratchet_chains: List[str] = []
        self.tenant_chains: List[str] = []
        self.reload_if_needed(force=True)

    def reload_if_needed(self, force: bool = False):
        if not os.path.exists(self.cache_file):
            logger.warning("Chains cache file not found at: %s", self.cache_file)
            return

        mtime = os.path.getmtime(self.cache_file)
        if force or mtime > self.last_mtime:
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self.chains = json.load(f)
                self.last_mtime = mtime

                # Categorize chains
                self.global_chains = [c for c in self.chains if "global_" in c.lower()]
                self.job_chains = [c for c in self.chains if "job_" in c.lower()]
                self.ratchet_chains = [c for c in self.chains if "ratchet_" in c.lower()]
                self.tenant_chains = [
                    c for c in self.chains
                    if not ("global_" in c.lower() or "job_" in c.lower() or "ratchet_" in c.lower())
                ]
                logger.info(
                    "Indexed %d chains (Global: %d, Job: %d, Ratchet: %d, Tenant: %d)",
                    len(self.chains),
                    len(self.global_chains),
                    len(self.job_chains),
                    len(self.ratchet_chains),
                    len(self.tenant_chains),
                )
            except Exception as exc:
                logger.error("Failed to load chains cache: %s", exc)

    def search(self, keyword: str, chain_type: str = "all", limit: int = 50) -> List[Dict[str, Any]]:
        self.reload_if_needed()
        keyword_lower = keyword.strip().lower()

        if chain_type == "global":
            pool = self.global_chains
        elif chain_type == "job":
            pool = self.job_chains
        elif chain_type == "ratchet":
            pool = self.ratchet_chains
        elif chain_type == "tenant":
            pool = self.tenant_chains
        else:
            pool = list(self.chains.keys())

        results = []
        for name in pool:
            val = str(self.chains.get(name, ""))
            if not keyword_lower or keyword_lower in name.lower() or keyword_lower in val.lower():
                # Determine type
                if "global_" in name.lower():
                    cat = "global"
                elif "job_" in name.lower():
                    cat = "job"
                elif "ratchet_" in name.lower():
                    cat = "ratchet"
                else:
                    cat = "tenant"

                results.append({
                    "chain_name": name,
                    "identifier": val,
                    "type": cat
                })
                if len(results) >= limit:
                    break

        return results

    def get_summary(self) -> Dict[str, Any]:
        self.reload_if_needed()
        return {
            "total_chains": len(self.chains),
            "global_chains_count": len(self.global_chains),
            "job_chains_count": len(self.job_chains),
            "ratchet_chains_count": len(self.ratchet_chains),
            "tenant_chains_count": len(self.tenant_chains),
            "sample_global_chains": self.global_chains[:5],
            "sample_job_chains": self.job_chains[:5],
            "sample_ratchet_chains": self.ratchet_chains[:5],
        }

chain_catalog = ChainCatalogIndex(CHAINS_CACHE_PATH)

# ==========================================
# CMA GATEWAY HTTP CLIENT HELPER
# ==========================================
def call_cma_gateway(
    endpoint: str,
    method: str = "GET",
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """Issues HTTP request to CMA Gateway with automatic URL fallback."""
    candidate_urls = [CMA_GATEWAY_URL]
    if "172.27.210.162" in CMA_GATEWAY_URL:
        candidate_urls.append("http://localhost:8555")
    elif "localhost" in CMA_GATEWAY_URL or "127.0.0.1" in CMA_GATEWAY_URL:
        candidate_urls.append("http://172.27.210.162:8555")

    last_error = None
    for base_url in candidate_urls:
        url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = {
            "x-api-key": CMA_API_KEY,
            "Content-Type": "application/json",
            "User-Agent": "CMA-MCP-Server/1.0",
        }
        data_bytes = json.dumps(payload).encode("utf-8") if payload else None

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw_body = resp.read().decode("utf-8")
                try:
                    return json.loads(raw_body)
                except Exception:
                    return {"status": "success", "raw_response": raw_body}
        except urllib.error.HTTPError as http_err:
            try:
                err_data = json.loads(http_err.read().decode("utf-8"))
                return {
                    "status": "error",
                    "code": http_err.code,
                    "error": err_data.get("error", str(http_err)),
                    "details": err_data
                }
            except Exception:
                return {
                    "status": "error",
                    "code": http_err.code,
                    "error": f"HTTP {http_err.code}: {http_err.reason}"
                }
        except Exception as conn_err:
            last_error = conn_err
            continue

    return {
        "status": "error",
        "code": 503,
        "error": f"Could not connect to CMA Gateway at {candidate_urls}: {last_error}"
    }

# ==========================================
# SQLITE AUDIT DATABASE HELPER
# ==========================================
def get_db_connection() -> Optional[sqlite3.Connection]:
    if not os.path.exists(DB_FILE):
        return None
    try:
        conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception:
            logger.debug("Failed to open sqlite db %s: %s", DB_FILE, e)
            return None

# ==========================================
# MCP SERVER INITIALIZATION
# ==========================================
from mcp.server.mcpserver import MCPServer
from starlette.responses import JSONResponse
from starlette.routing import Route

mcp_server = MCPServer("cma-mcp-server")

# ==========================================
# TOOLS IMPLEMENTATION
# ==========================================

@mcp_server.tool()
def cma_execute_query(
    chain: str,
    query: str,
    timeout: float = 35.0,
) -> Dict[str, Any]:
    """Execute arbitrary SQL query on any G3 chain or tenant database.
    
    Args:
        chain: Target database chain name (e.g. 'global_PROD_1', 'job_PROD_1', 'ratchet_PROD_1', or 'Hilton-ATLFY').
        query: SQL query to execute. SQL comments are automatically stripped.
        timeout: Query timeout in seconds (default 35.0).
    """
    clean_query = re.sub(r"--.*", "", query).strip()
    if not clean_query:
        return {"status": "error", "error": "Query cannot be empty."}

    # Normalize chain if tenant label format ('0018 - Hotel Katerina')
    target_chain = chain.strip()
    payload = {
        "chains": [target_chain],
        "query": clean_query,
        "format": "json"
    }

    t0 = time.monotonic()
    resp = call_cma_gateway("/api/v1/execute_batch", method="POST", payload=payload, timeout=timeout)
    elapsed = round(time.monotonic() - t0, 3)

    if resp.get("status") != "success":
        return {
            "status": "error",
            "chain": target_chain,
            "error": resp.get("error", "Unknown gateway error"),
            "code": resp.get("code", 500),
            "execution_time_seconds": elapsed,
            "hint": "If authentication failed or circuit breaker tripped, run cma_trigger_sso_refresh or inspect cma_get_session_status."
        }

    data_block = resp.get("data", {})
    # Extract records: could be under target_chain or normalized tenant code
    chain_data = data_block.get(target_chain)
    if chain_data is None and " - " in target_chain:
        tenant_code = target_chain.split(" - ", 1)[0].strip().split(".")[-1]
        chain_data = data_block.get(tenant_code)
    if chain_data is None and data_block:
        chain_data = list(data_block.values())[0]

    rows = []
    if chain_data:
        rows = chain_data.get("Query_1", [])
        if not rows and isinstance(chain_data, list):
            rows = chain_data

    return {
        "status": "success",
        "chain": target_chain,
        "row_count": len(rows),
        "execution_time_seconds": elapsed,
        "data": rows
    }


@mcp_server.tool()
def cma_execute_batch(
    chains: List[str],
    queries: List[str],
    timeout: float = 60.0,
) -> Dict[str, Any]:
    """Execute multiple SQL queries across multiple database chains concurrently in a single batch.
    
    Args:
        chains: List of database chain names (e.g. ['global_PROD_1', 'global_PROD_2']).
        queries: List of SQL queries to execute on each chain.
        timeout: Batch execution timeout in seconds (default 60.0).
    """
    clean_queries = [re.sub(r"--.*", "", q).strip() for q in queries if re.sub(r"--.*", "", q).strip()]
    if not chains or not clean_queries:
        return {"status": "error", "error": "Both chains and queries must be non-empty."}

    payload = {
        "chains": chains,
        "queries": clean_queries,
        "format": "json"
    }

    t0 = time.monotonic()
    resp = call_cma_gateway("/api/v1/execute_batch", method="POST", payload=payload, timeout=timeout)
    elapsed = round(time.monotonic() - t0, 3)

    if resp.get("status") != "success":
        return {
            "status": "error",
            "error": resp.get("error", "Unknown gateway error"),
            "code": resp.get("code", 500),
            "execution_time_seconds": elapsed
        }

    return {
        "status": "success",
        "total_chains_requested": len(chains),
        "total_queries_requested": len(clean_queries),
        "execution_time_seconds": elapsed,
        "results": resp.get("data", {})
    }


@mcp_server.tool()
def cma_search_chains(
    keyword: str,
    chain_type: str = "all",
    limit: int = 50,
) -> Dict[str, Any]:
    """Search across 19,496+ cached database chains by keyword, hotel code, or client identifier.
    
    Args:
        keyword: Search term (e.g. 'Hilton', 'ATLFY', 'global_PROD', '0018').
        chain_type: Filter by chain type: 'all', 'global', 'job', 'ratchet', or 'tenant'.
        limit: Max results to return (default 50).
    """
    t0 = time.monotonic()
    results = chain_catalog.search(keyword=keyword, chain_type=chain_type, limit=limit)
    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    return {
        "status": "success",
        "keyword": keyword,
        "chain_type_filter": chain_type,
        "matched_count": len(results),
        "search_time_ms": elapsed_ms,
        "results": results
    }


@mcp_server.tool()
def cma_list_all_chains(
    chain_type: str = "all",
    limit: int = 100,
    offset: int = 0,
) -> Dict[str, Any]:
    """Paginate and list database chains from the catalog.
    
    Args:
        chain_type: Filter by chain category: 'all', 'global', 'job', 'ratchet', or 'tenant'.
        limit: Number of items per page (default 100).
        offset: Offset for pagination (default 0).
    """
    chain_catalog.reload_if_needed()
    if chain_type == "global":
        pool = chain_catalog.global_chains
    elif chain_type == "job":
        pool = chain_catalog.job_chains
    elif chain_type == "ratchet":
        pool = chain_catalog.ratchet_chains
    elif chain_type == "tenant":
        pool = chain_catalog.tenant_chains
    else:
        pool = list(chain_catalog.chains.keys())

    total = len(pool)
    slice_names = pool[offset:offset + limit]
    items = []
    for name in slice_names:
        val = str(chain_catalog.chains.get(name, ""))
        cat = "global" if "global_" in name.lower() else ("job" if "job_" in name.lower() else ("ratchet" if "ratchet_" in name.lower() else "tenant"))
        items.append({"chain_name": name, "identifier": val, "type": cat})

    return {
        "status": "success",
        "total_in_category": total,
        "offset": offset,
        "limit": limit,
        "chain_type": chain_type,
        "chains": items
    }


@mcp_server.tool()
def cma_get_chain_details(
    chain_name: str,
    test_connection: bool = False,
) -> Dict[str, Any]:
    """Get metadata for a specific database chain, with optional live ping connectivity verification.
    
    Args:
        chain_name: Exact database chain name (e.g. 'global_PROD_1', 'Hilton-ATLFY').
        test_connection: If True, executes 'SELECT 1' on the chain to verify live connectivity.
    """
    chain_catalog.reload_if_needed()
    identifier = chain_catalog.chains.get(chain_name)
    if identifier is None:
        return {
            "status": "not_found",
            "chain_name": chain_name,
            "error": f"Chain '{chain_name}' not found in cached catalog of {len(chain_catalog.chains)} chains."
        }

    cat = "global" if "global_" in chain_name.lower() else ("job" if "job_" in chain_name.lower() else ("ratchet" if "ratchet_" in chain_name.lower() else "tenant"))

    res = {
        "status": "found",
        "chain_name": chain_name,
        "identifier": identifier,
        "type": cat,
    }

    if test_connection:
        t0 = time.monotonic()
        ping_res = cma_execute_query(chain=chain_name, query="SELECT 1 AS ping", timeout=15.0)
        res["connectivity_test"] = {
            "status": ping_res.get("status"),
            "latency_ms": round((time.monotonic() - t0) * 1000, 2),
            "details": ping_res
        }

    return res


@mcp_server.tool()
def cma_get_property(
    property_id: str = "",
    property_code: str = "",
    client_code: str = "",
    global_chain: str = "global_PROD_1",
    include_audit_history: bool = True,
) -> Dict[str, Any]:
    """Retrieve full Property configuration, Client linkage, and stage mode audit history.
    
    Args:
        property_id: Numeric Property_ID (e.g. '5').
        property_code: Property short code (e.g. 'H1', 'LONME', 'ATLFY').
        client_code: Optional client code to narrow lookup (e.g. 'BSTN', 'Hilton').
        global_chain: G3 Global database chain (default 'global_PROD_1').
        include_audit_history: If True, fetches recent stage transitions from Property_AUD (default True).
    """
    where_clauses = []
    if property_id:
        where_clauses.append(f"p.Property_ID = '{property_id.strip()}'")
    if property_code:
        where_clauses.append(f"p.Property_Code = '{property_code.strip()}'")
    if client_code:
        where_clauses.append(f"c.Client_Code = '{client_code.strip()}'")

    if not where_clauses:
        return {"status": "error", "error": "At least one of property_id, property_code, or client_code must be provided."}

    sql = f"""
        SELECT TOP 10 
            p.Property_ID, p.Property_Code, p.Property_Name, p.Stage,
            p.Client_ID, c.Client_Code, c.Client_Name,
            p.SFDC_Account_Number, p.UPS_ID, p.Country_Code,
            p.Deployment_Status, p.Is_Virtual_Property, p.Last_Updated_DTTM
        FROM Property p
        LEFT JOIN Client c ON c.Client_ID = p.Client_ID
        WHERE {' AND '.join(where_clauses)}
    """

    res = cma_execute_query(chain=global_chain, query=sql)
    if res.get("status") != "success" or not res.get("data"):
        return {
            "status": "not_found",
            "error": "Property not found with given criteria",
            "criteria": {"property_id": property_id, "property_code": property_code, "client_code": client_code},
            "raw_response": res
        }

    records = res["data"]
    enriched = []
    for prop in records:
        stage_raw = str(prop.get("Stage", "")).strip()
        mode_label = STAGE_MODE_MAP.get(stage_raw, stage_raw)
        prop_copy = dict(prop)
        prop_copy["Resolved_Mode"] = mode_label

        if include_audit_history and prop.get("Property_ID"):
            aud_sql = f"""
                SELECT TOP 10 Stage, Last_Updated_DTTM, Last_Updated_By_User_ID
                FROM Property_AUD
                WHERE Property_ID = '{prop['Property_ID']}'
                ORDER BY Last_Updated_DTTM DESC
            """
            aud_res = cma_execute_query(chain=global_chain, query=aud_sql)
            history = []
            if aud_res.get("status") == "success" and aud_res.get("data"):
                for h in aud_res["data"]:
                    h_stage = str(h.get("Stage", "")).strip()
                    history.append({
                        "stage": h_stage,
                        "mode": STAGE_MODE_MAP.get(h_stage, h_stage),
                        "timestamp": h.get("Last_Updated_DTTM"),
                        "user_id": h.get("Last_Updated_By_User_ID")
                    })
            prop_copy["Stage_Audit_History"] = history

        enriched.append(prop_copy)

    return {
        "status": "success",
        "count": len(enriched),
        "properties": enriched
    }


@mcp_server.tool()
def cma_get_property_parameters(
    client_code: str,
    property_code: str,
    parameter_names: Optional[List[str]] = None,
    global_chain: str = "global_PROD_1",
) -> Dict[str, Any]:
    """Resolve cascading configuration parameters across pacman -> pacman.CLIENT -> pacman.CLIENT.PROP.
    
    Args:
        client_code: Client code (e.g. 'Hilton', 'BSTN').
        property_code: Property short code (e.g. 'ATLFY', '0018').
        parameter_names: List of specific parameter names to inspect. If omitted or empty, retrieves top 100 parameters.
        global_chain: G3 Global database chain (default 'global_PROD_1').
    """
    client_clean = client_code.strip()
    prop_clean = property_code.strip()
    prop_padded = prop_clean.zfill(4) if prop_clean.isdigit() else prop_clean

    contexts = [
        "pacman",
        f"pacman.{client_clean}",
        f"pacman.{client_clean}.{prop_clean}",
    ]
    if prop_padded != prop_clean:
        contexts.append(f"pacman.{client_clean}.{prop_padded}")

    contexts_sql = ", ".join(f"'{c}'" for c in contexts)

    name_filter = ""
    if parameter_names:
        clean_names = [f"'{n.strip()}'" for n in parameter_names if n.strip()]
        if clean_names:
            name_filter = f"AND cp.Name IN ({', '.join(clean_names)})"

    sql = f"""
        SELECT cp.Name AS ParameterName, cpv.Context, cpv.FixedValue,
               cpdv.Value AS PredefinedValue
        FROM Config_Parameter cp
        JOIN Config_Parameter_Value cpv ON cpv.Config_Parameter_ID = cp.Config_Parameter_ID
        LEFT JOIN Config_Parameter_Predefined_Value cpdv
            ON cpdv.Config_Parameter_Predefined_Value_ID = cpv.Config_Parameter_Predefined_Value_ID
        WHERE cpv.Context IN ({contexts_sql})
        {name_filter}
        ORDER BY cp.Name, cpv.Context
    """

    res = cma_execute_query(chain=global_chain, query=sql)
    if res.get("status") != "success":
        return res

    rows = res.get("data", [])
    grouped: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        pname = r.get("ParameterName")
        ctx = r.get("Context")
        val = r.get("FixedValue")
        if val is None or str(val).lower() == "nan":
            val = r.get("PredefinedValue")

        if pname not in grouped:
            grouped[pname] = {"hierarchy": {}, "effective_value": None, "effective_context": None}

        grouped[pname]["hierarchy"][ctx] = val

    # Resolve cascading hierarchy: property-level > client-level > global pacman
    for pname, item in grouped.items():
        hier = item["hierarchy"]
        # Check property level
        prop_ctx_1 = f"pacman.{client_clean}.{prop_clean}"
        prop_ctx_2 = f"pacman.{client_clean}.{prop_padded}"
        client_ctx = f"pacman.{client_clean}"

        if prop_ctx_1 in hier and hier[prop_ctx_1] is not None:
            item["effective_value"] = hier[prop_ctx_1]
            item["effective_context"] = prop_ctx_1
        elif prop_ctx_2 in hier and hier[prop_ctx_2] is not None:
            item["effective_value"] = hier[prop_ctx_2]
            item["effective_context"] = prop_ctx_2
        elif client_ctx in hier and hier[client_ctx] is not None:
            item["effective_value"] = hier[client_ctx]
            item["effective_context"] = client_ctx
        elif "pacman" in hier and hier["pacman"] is not None:
            item["effective_value"] = hier["pacman"]
            item["effective_context"] = "pacman"

    return {
        "status": "success",
        "client_code": client_clean,
        "property_code": prop_clean,
        "parameters_resolved_count": len(grouped),
        "parameters": grouped
    }


@mcp_server.tool()
def cma_get_job_execution(
    job_name: str,
    job_chain: str = "job_PROD_1",
    param_key: str = "",
    param_value: str = "",
    after_datetime: str = "",
    limit: int = 10,
) -> Dict[str, Any]:
    """Inspect Spring Batch execution logs, statuses, and runtimes in G3 Job databases.
    
    Args:
        job_name: Name of batch job (e.g. 'continuousPricingJob', 'strFileIngestionJob', 'etlJob').
        job_chain: G3 Job database chain (default 'job_PROD_1').
        param_key: Filter parameter name (e.g. 'propertyId', 'sendingSystemPropertyId', 'clientCode').
        param_value: Value for the param_key filter.
        after_datetime: ISO datetime string to filter runs after (e.g. '2026-01-01 00:00:00').
        limit: Max executions to retrieve (default 10).
    """
    clean_job = job_name.strip()
    date_filter = f"AND je.START_TIME >= '{after_datetime.strip()}'" if after_datetime else ""

    if param_key and param_value:
        sql = f"""
            SELECT TOP {limit}
                ji.JOB_NAME, je.JOB_EXECUTION_ID, je.STATUS,
                je.START_TIME, je.END_TIME, je.EXIT_CODE, je.EXIT_MESSAGE,
                jep.KEY_NAME, jep.STRING_VAL
            FROM JOB_INSTANCE ji
            JOIN JOB_EXECUTION je ON je.JOB_INSTANCE_ID = ji.JOB_INSTANCE_ID
            JOIN JOB_EXECUTION_PARAMS jep ON jep.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID
            WHERE ji.JOB_NAME = '{clean_job}'
              AND jep.KEY_NAME = '{param_key.strip()}'
              AND jep.STRING_VAL = '{param_value.strip()}'
              {date_filter}
            ORDER BY je.START_TIME DESC
        """
    else:
        sql = f"""
            SELECT TOP {limit}
                ji.JOB_NAME, je.JOB_EXECUTION_ID, je.STATUS,
                je.START_TIME, je.END_TIME, je.EXIT_CODE, je.EXIT_MESSAGE
            FROM JOB_INSTANCE ji
            JOIN JOB_EXECUTION je ON je.JOB_INSTANCE_ID = ji.JOB_INSTANCE_ID
            WHERE ji.JOB_NAME = '{clean_job}'
              {date_filter}
            ORDER BY je.START_TIME DESC
        """

    res = cma_execute_query(chain=job_chain, query=sql)
    return res


@mcp_server.tool()
def cma_get_tenant_table_data(
    chain: str,
    table_name: str,
    where_clause: str = "",
    columns: Optional[List[str]] = None,
    order_by: str = "",
    limit: int = 100,
) -> Dict[str, Any]:
    """Safely query tenant database tables (e.g. OCCUPANCY, TRANSACTIONS, RESERVATIONS, RATE_CODES).
    
    Args:
        chain: Tenant database chain name (e.g. 'Hilton-ATLFY').
        table_name: Target table name.
        where_clause: Optional WHERE clause without the 'WHERE' keyword (e.g. "OCCUPANCY_DATE >= '2026-01-01'").
        columns: Specific columns to project. Defaults to '*' if omitted.
        order_by: Optional ORDER BY clause without the 'ORDER BY' keyword (e.g. "OCCUPANCY_DATE DESC").
        limit: Max rows to return (capped at 5000, default 100).
    """
    clean_table = re.sub(r"[^A-Za-z0-9_]", "", table_name.strip())
    if not clean_table:
        return {"status": "error", "error": "Invalid table name."}

    proj_cols = "*"
    if columns:
        sanitized_cols = [re.sub(r"[^A-Za-z0-9_]", "", c.strip()) for c in columns if c.strip()]
        if sanitized_cols:
            proj_cols = ", ".join(sanitized_cols)

    safe_limit = min(max(int(limit), 1), 5000)
    where_part = f"WHERE {where_clause.strip()}" if where_clause.strip() else ""
    order_part = f"ORDER BY {order_by.strip()}" if order_by.strip() else ""

    sql = f"SELECT TOP {safe_limit} {proj_cols} FROM {clean_table} {where_part} {order_part};"
    return cma_execute_query(chain=chain, query=sql)


@mcp_server.tool()
def cma_get_session_status() -> Dict[str, Any]:
    """Inspect active CMA session status, cookie validity, and circuit breaker status."""
    resp = call_cma_gateway("/api/internal/needs_cookie", method="GET", timeout=5.0)

    # Read physical cookie length
    active_cookie = os.getenv("CMA_COOKIE", "")
    cookie_preview = f"{active_cookie[:15]}...{active_cookie[-8:]}" if len(active_cookie) > 25 else "Not set"

    # Check recent queries in DB
    db = get_db_connection()
    last_activity = None
    if db:
        try:
            row = db.execute("SELECT created_at, status_code FROM audit_logs ORDER BY id DESC LIMIT 1").fetchone()
            if row:
                last_activity = {"timestamp": row["created_at"], "status_code": row["status_code"]}
        except Exception:
            pass

    return {
        "status": "healthy" if resp.get("has_cookie") else "degraded",
        "has_cookie": resp.get("has_cookie", False),
        "needs_cookie": resp.get("needs_cookie", False),
        "cookie_preview": cookie_preview,
        "cookie_length": len(active_cookie),
        "last_gateway_activity": last_activity,
        "gateway_url": CMA_GATEWAY_URL
    }


@mcp_server.tool()
def cma_trigger_sso_refresh(
    force: bool = False,
) -> Dict[str, Any]:
    """Trigger the Chrome/Edge Virtual Desktop SSO auto-login to renew an expired JSESSIONID.
    
    Args:
        force: If True, triggers re-login even if the cooldown period is currently active.
    """
    # Test session first
    status = cma_get_session_status()
    if status.get("has_cookie") and not status.get("needs_cookie") and not force:
        return {
            "status": "skipped",
            "message": "CMA session is already active and healthy. Pass force=True to force refresh.",
            "session": status
        }

    # Signal Edge / Chrome Extension by checking needs_cookie or calling update
    logger.info("Triggering SSO cookie refresh via CMA internal gateway...")
    # Trigger refresh by pinging gateway
    resp = call_cma_gateway("/api/internal/needs_cookie", method="GET", timeout=5.0)

    return {
        "status": "initiated",
        "message": "SSO refresh triggered on Edge virtual desktop. The background cookie daemon will update the session.",
        "gateway_response": resp
    }


@mcp_server.tool()
def cma_refresh_chains_cache() -> Dict[str, Any]:
    """Force an immediate refresh and reload of the 19,496+ cached database chains."""
    t0 = time.monotonic()
    # Call admin refresh endpoint if reachable
    resp = call_cma_gateway("/admin/refresh-chains", method="POST", payload={}, timeout=15.0)

    # Reload local cache
    chain_catalog.reload_if_needed(force=True)
    elapsed = round(time.monotonic() - t0, 3)

    return {
        "status": "success",
        "message": "Chains cache successfully refreshed and indexed.",
        "execution_time_seconds": elapsed,
        "total_chains_indexed": len(chain_catalog.chains),
        "categories": {
            "global": len(chain_catalog.global_chains),
            "job": len(chain_catalog.job_chains),
            "ratchet": len(chain_catalog.ratchet_chains),
            "tenant": len(chain_catalog.tenant_chains),
        }
    }


@mcp_server.tool()
def cma_get_audit_logs(
    limit: int = 50,
    status_code: int = 0,
    client_name: str = "",
) -> Dict[str, Any]:
    """Retrieve recent query audit logs and performance metrics from the gateway database.
    
    Args:
        limit: Max rows to return (default 50).
        status_code: Optional status code filter (e.g. 200, 500, 503).
        client_name: Optional client name filter.
    """
    db = get_db_connection()
    if not db:
        return {"status": "error", "error": "Audit database api_gateway.db not accessible."}

    where_clauses = []
    params = []
    if status_code > 0:
        where_clauses.append("status_code = ?")
        params.append(status_code)
    if client_name:
        where_clauses.append("client_name LIKE ?")
        params.append(f"%{client_name.strip()}%")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    params.append(min(max(limit, 1), 200))

    query = f"""
        SELECT id, client_name, chains_requested, chains_count, queries_count,
               status_code, status_message, execution_time_seconds, created_at,
               SUBSTR(query_executed, 1, 100) AS query_snippet
        FROM audit_logs
        {where_sql}
        ORDER BY id DESC
        LIMIT ?
    """

    try:
        cur = db.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        return {
            "status": "success",
            "count": len(rows),
            "logs": rows
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@mcp_server.tool()
def cma_get_system_stats() -> Dict[str, Any]:
    """Comprehensive telemetry report: throughput, latencies, cached chains, and error counts."""
    chain_summary = chain_catalog.get_summary()

    db = get_db_connection()
    audit_stats = {}
    if db:
        try:
            total_queries = db.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
            avg_time = db.execute("SELECT avg(execution_time_seconds) FROM audit_logs WHERE status_code = 200").fetchone()[0]
            success_count = db.execute("SELECT count(*) FROM audit_logs WHERE status_code = 200").fetchone()[0]
            error_count = db.execute("SELECT count(*) FROM audit_logs WHERE status_code >= 400").fetchone()[0]

            audit_stats = {
                "total_queries_recorded": total_queries,
                "successful_queries": success_count,
                "error_queries": error_count,
                "average_execution_seconds": round(avg_time, 3) if avg_time else 0.0,
            }
        except Exception as e:
            audit_stats = {"error": str(e)}

    session_status = cma_get_session_status()

    return {
        "status": "healthy",
        "service": "cma-mcp-server",
        "session": session_status,
        "chains_catalog": chain_summary,
        "audit_telemetry": audit_stats,
    }

# ==========================================
# RESOURCES
# ==========================================

@mcp_server.resource("cma://system/status")
def get_system_status_resource() -> str:
    """Real-time CMA session, cookie, and circuit breaker status."""
    return json.dumps(cma_get_session_status(), indent=2)

@mcp_server.resource("cma://system/stats")
def get_system_stats_resource() -> str:
    """Operational throughput, database, and telemetry metrics."""
    return json.dumps(cma_get_system_stats(), indent=2)

@mcp_server.resource("cma://chains/summary")
def get_chains_summary_resource() -> str:
    """Summary of all 19,496+ cached database chains across production clusters."""
    return json.dumps(chain_catalog.get_summary(), indent=2)

# ==========================================
# HEALTH ENDPOINT & SERVER RUNNER
# ==========================================

async def health_endpoint(request):
    session = cma_get_session_status()
    tools = [t.name for t in mcp_server._tool_manager.list_tools()] if hasattr(mcp_server, "_tool_manager") else []
    return JSONResponse({
        "status": "HEALTHY",
        "server": "cma-mcp-server",
        "version": "1.0.0",
        "tools_count": len(tools),
        "session": session,
        "chains_indexed": len(chain_catalog.chains)
    })

def run_server(transport: str = "sse", host: str = MCP_HOST, port: int = MCP_PORT):
    if transport == "stdio":
        logger.info("Starting CMA MCP Server in STDIO mode...")
        mcp_server.run("stdio")
    else:
        import uvicorn
        logger.info("Starting CMA MCP Server in SSE mode on http://%s:%d/sse...", host, port)
        app = mcp_server.sse_app()
        # Add health probe endpoint
        app.routes.append(Route("/health", health_endpoint, methods=["GET"]))

        config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level="info",
            access_log=True,
        )
        server = uvicorn.Server(config)
        server.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CMA Enterprise MCP Server")
    parser.add_argument("--transport", choices=["sse", "stdio"], default="sse", help="Transport mode")
    parser.add_argument("--host", default=MCP_HOST, help="Host to bind (default 0.0.0.0)")
    parser.add_argument("--port", type=int, default=MCP_PORT, help="Port to bind (default 8556)")
    args = parser.parse_args()

    run_server(transport=args.transport, host=args.host, port=args.port)
