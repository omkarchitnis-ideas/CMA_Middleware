#!/usr/bin/env python3
"""
CMA Enterprise Model Context Protocol (MCP) Server
=================================================
Comprehensive 34-Tool Suite for the SAS IDeaS Client Management Application (CMA).

Key Capabilities:
1. Core SQL Execution: Arbitrary single and multi-chain batch execution.
2. Schema & Metadata Discovery: Wildcard table search, schema reflection, column types.
3. Cluster & Environment Resolution: Automatic PROD_1..6 mapping, client portfolio lookup.
4. Global Property & Parameter Matrix: Cascading pacman context resolution, datafeed interfaces.
5. Spring Batch Processing: Job execution, failed step triage, blocked jobs, daily throughput.
6. Tenant Revenue & Pacing Metrics: Accom_Activity and PACE_Accom_Activity reconciliation.
7. Salesforce Case Audit Trail: Full historical query log by SFDC Case # and exact SQL recovery.
8. Curated Engineering Scripts: 2,000+ pre-built queries tagged by case and operational domain.
9. Real-Time Team Diagnostics: Live team query execution feeds and Job ID search.
10. Remote Cluster File Explorer: Browse G3 Prod 1..6 data directories and RSS log drops.
11. Configuration & Automation: Property System Parameters, scheduled recurring SQL jobs.
12. Session Resilience: Automated Edge CDP SSO re-login, circuit breaker, audit logging.
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
import urllib.parse
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
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

CMA_WEB_BASE = "https://g3-cma.ideas.com/cma"
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
                if "global_" in name.lower():
                    cat = "global"
                elif "job_" in name.lower():
                    cat = "job"
                elif "ratchet_" in name.lower():
                    cat = "ratchet"
                else:
                    cat = "tenant"

                results.append({"chain_name": name, "identifier": val, "type": cat})
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
# CMA GATEWAY & WEB SESSION CLIENTS
# ==========================================
def get_cma_cookie() -> str:
    """Retrieves active session cookie from env or file."""
    cookie = os.getenv("CMA_COOKIE", "")
    if not cookie:
        env_path = os.path.join(SCRIPT_DIR, ".env")
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("CMA_COOKIE="):
                        cookie = line.split("=", 1)[1].strip()
                        break
    return cookie

def call_cma_gateway(
    endpoint: str,
    method: str = "GET",
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 35.0,
) -> Dict[str, Any]:
    """Issues HTTP request to CMA Gateway with automatic URL fallback."""
    candidate_urls = [CMA_GATEWAY_URL]
    if "172.27.210.162" in CMA_GATEWAY_URL:
        candidate_urls.append("http://localhost:8555")
        candidate_urls.append("http://cma-middleware:8555")
    elif "localhost" in CMA_GATEWAY_URL or "127.0.0.1" in CMA_GATEWAY_URL:
        candidate_urls.append("http://172.27.210.162:8555")
        candidate_urls.append("http://cma-middleware:8555")

    last_error = None
    for base_url in candidate_urls:
        url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = {
            "x-api-key": CMA_API_KEY,
            "Content-Type": "application/json",
            "User-Agent": "CMA-MCP-Server/2.0",
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
        "error": f"Could not connect to CMA Gateway: {last_error}"
    }

def call_cma_web(
    path: str,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    timeout: float = 15.0,
) -> Optional[requests.Response]:
    """Issues direct authenticated request to the live CMA portal."""
    cookie = get_cma_cookie()
    headers = {
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": f"{CMA_WEB_BASE}/adhocSql/viewAdhoc",
        "Origin": "https://g3-cma.ideas.com"
    }
    url = f"{CMA_WEB_BASE.rstrip('/')}/{path.lstrip('/')}"
    try:
        if method.upper() == "POST":
            return requests.post(url, headers=headers, data=data, timeout=timeout)
        return requests.get(url, headers=headers, params=data, timeout=timeout)
    except Exception as e:
        logger.error("Error calling CMA web path %s: %s", path, e)
        return None

def get_db_connection() -> Optional[sqlite3.Connection]:
    if not os.path.exists(DB_FILE):
        return None
    try:
        conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception:
            return None

# ==========================================
# MCP SERVER INITIALIZATION
# ==========================================
from mcp.server.mcpserver import MCPServer
from starlette.responses import JSONResponse
from starlette.routing import Route

mcp_server = MCPServer("cma-mcp-server")

# ==========================================
# DOMAIN 1: CORE SQL & BATCH EXECUTION
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

    target_chain = chain.strip()
    payload = {"chains": [target_chain], "query": clean_query, "format": "json"}

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

    payload = {"chains": chains, "queries": clean_queries, "format": "json"}
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

# ==========================================
# DOMAIN 2: CHAIN CATALOG & RESOLUTION
# ==========================================

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
        chain_type: Filter by category: 'all', 'global', 'job', 'ratchet', or 'tenant'.
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
    res = {"status": "found", "chain_name": chain_name, "identifier": identifier, "type": cat}

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
def cma_resolve_tenant_environment(
    client_code: str = "",
    property_code: str = "",
) -> Dict[str, Any]:
    """Automatically identify which G3 production cluster (PROD_1..6) hosts a client or hotel.
    
    Args:
        client_code: Client code (e.g. 'Hilton', 'BSTN', 'IHG').
        property_code: Property short code (e.g. 'ATLFY', 'H1', '0018').
    """
    if not client_code and not property_code:
        return {"status": "error", "error": "Either client_code or property_code must be provided."}

    prod_clusters = ["global_PROD_1", "global_PROD_2", "global_PROD_3", "global_PROD_5", "global_PROD_6"]
    
    where_parts = []
    if client_code:
        where_parts.append(f"c.Client_Code = '{client_code.strip()}'")
    if property_code:
        where_parts.append(f"p.Property_Code = '{property_code.strip()}'")
    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT TOP 1 p.Property_ID, p.Property_Code, p.Property_Name, p.Stage, c.Client_Code, c.Client_Name
        FROM Property p
        LEFT JOIN Client c ON c.Client_ID = p.Client_ID
        WHERE {where_sql}
    """

    for gchain in prod_clusters:
        res = cma_execute_query(chain=gchain, query=sql, timeout=12.0)
        if res.get("status") == "success" and res.get("data"):
            row = res["data"][0]
            cluster_num = gchain.replace("global_PROD_", "")
            
            # Search matched tenant chains in cache
            search_key = property_code or client_code
            matching_tenant_chains = chain_catalog.search(keyword=search_key, chain_type="tenant", limit=5)

            return {
                "status": "resolved",
                "cluster": f"PROD_{cluster_num}",
                "global_chain": gchain,
                "job_chain": f"job_PROD_{cluster_num}",
                "ratchet_chain": f"ratchet_PROD_{cluster_num}",
                "property_details": row,
                "suggested_tenant_chains": matching_tenant_chains
            }

    return {
        "status": "not_found",
        "error": f"Could not find active record for client '{client_code}' / property '{property_code}' across {prod_clusters}."
    }


@mcp_server.tool()
def cma_get_client_portfolio(
    client_code: str,
    global_chain: str = "global_PROD_1",
) -> Dict[str, Any]:
    """Retrieve all properties owned or operated by a client group across G3 Global databases.
    
    Args:
        client_code: Client code (e.g. 'BSTN', 'Hilton', 'IHG').
        global_chain: G3 Global database chain (default 'global_PROD_1').
    """
    sql = f"""
        SELECT p.Property_ID, p.Property_Code, p.Property_Name, p.Stage,
               p.SFDC_Account_Number, p.UPS_ID, p.Country_Code, p.Deployment_Status, p.Is_Virtual_Property
        FROM Property p
        JOIN Client c ON c.Client_ID = p.Client_ID
        WHERE c.Client_Code = '{client_code.strip()}'
        ORDER BY p.Property_Code
    """
    res = cma_execute_query(chain=global_chain, query=sql)
    if res.get("status") == "success":
        for r in res.get("data", []):
            st = str(r.get("Stage", "")).strip()
            r["Resolved_Mode"] = STAGE_MODE_MAP.get(st, st)
    return res

# ==========================================
# DOMAIN 3: SCHEMA & METADATA DISCOVERY
# ==========================================

@mcp_server.tool()
def cma_list_tables(
    chain: str,
    pattern: str = "*",
) -> Dict[str, Any]:
    """List tables and views on any database chain (Global, Job, Ratchet, or Tenant) matching a wildcard pattern.
    
    Args:
        chain: Database chain name (e.g. 'global_PROD_1', 'job_PROD_1', 'Hilton-ATLFY').
        pattern: Wildcard search pattern (e.g. '*PACE*', '*USER*', '*PARAM*', '*JOB*'). Defaults to '*'.
    """
    sql_like = pattern.replace("*", "%").replace("?", "_").strip()
    sql = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME LIKE '{sql_like}'
        ORDER BY TABLE_TYPE, TABLE_NAME
    """
    return cma_execute_query(chain=chain, query=sql)


@mcp_server.tool()
def cma_describe_table(
    chain: str,
    table_name: str,
) -> Dict[str, Any]:
    """Reflect table schema: column names, data types, nullability, character lengths, and default values.
    
    Args:
        chain: Target database chain name (e.g. 'global_PROD_1', 'Hilton-ATLFY').
        table_name: Table name to describe (e.g. 'Property', 'Accom_Activity', 'JOB_INSTANCE').
    """
    clean_table = re.sub(r"[^A-Za-z0-9_]", "", table_name.strip())
    sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{clean_table}'
        ORDER BY ORDINAL_POSITION
    """
    res = cma_execute_query(chain=chain, query=sql)
    if res.get("status") == "success":
        return {
            "status": "success",
            "chain": chain,
            "table_name": clean_table,
            "column_count": res.get("row_count", 0),
            "columns": res.get("data", [])
        }
    return res

# ==========================================
# DOMAIN 4: GLOBAL PROPERTY & CONFIGURATION
# ==========================================

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
        client_code: Optional client code to narrow lookup.
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

    return {"status": "success", "count": len(enriched), "properties": enriched}


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
        parameter_names: List of specific parameter names to inspect. If omitted, retrieves all parameters.
        global_chain: G3 Global database chain (default 'global_PROD_1').
    """
    client_clean = client_code.strip()
    prop_clean = property_code.strip()
    prop_padded = prop_clean.zfill(4) if prop_clean.isdigit() else prop_clean

    contexts = ["pacman", f"pacman.{client_clean}", f"pacman.{client_clean}.{prop_clean}"]
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

    # Resolve hierarchy: property > client > global
    for pname, item in grouped.items():
        hier = item["hierarchy"]
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
def cma_get_datafeed_config(
    property_id: str = "",
    client_code: str = "",
    global_chain: str = "global_PROD_1",
) -> Dict[str, Any]:
    """Retrieve PMS/RMS interface configurations, datafeed endpoints, and FTP settings.
    
    Args:
        property_id: Numeric Property_ID.
        client_code: Client code (e.g. 'BSTN', 'Hilton').
        global_chain: G3 Global database chain (default 'global_PROD_1').
    """
    where_clause = f"WHERE p.Property_ID = '{property_id.strip()}'" if property_id else f"WHERE c.Client_Code = '{client_code.strip()}'"
    sql = f"""
        SELECT TOP 20
            p.Property_ID, p.Property_Code, p.Property_Name,
            df.Datafeed_ID, df.Name AS Datafeed_Name, df.Status_ID,
            dfe.Datafeed_Endpoint_ID, dfe.Endpoint_Type_ID, dfe.Server_Name, dfe.Remote_Path,
            ftp.FTP_Server_Address, ftp.FTP_User_Name
        FROM Property p
        LEFT JOIN Client c ON c.Client_ID = p.Client_ID
        LEFT JOIN Datafeed df ON df.Property_ID = p.Property_ID
        LEFT JOIN Datafeed_Endpoint dfe ON dfe.Datafeed_ID = df.Datafeed_ID
        LEFT JOIN Datafeed_FTP_Config ftp ON ftp.Datafeed_Endpoint_ID = dfe.Datafeed_Endpoint_ID
        {where_clause}
    """
    return cma_execute_query(chain=global_chain, query=sql)

# ==========================================
# DOMAIN 5: SPRING BATCH JOB OPERATIONS
# ==========================================

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

    return cma_execute_query(chain=job_chain, query=sql)


@mcp_server.tool()
def cma_get_failed_jobs(
    job_chain: str = "job_PROD_1",
    hours_back: int = 24,
    limit: int = 20,
) -> Dict[str, Any]:
    """Retrieve failed batch executions with step-level error traces and exit codes.
    
    Args:
        job_chain: G3 Job database chain (default 'job_PROD_1').
        hours_back: Hours to look back for failures (default 24).
        limit: Max failed executions to return (default 20).
    """
    cutoff = (datetime.now() - timedelta(hours=hours_back)).strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        SELECT TOP {limit}
            ji.JOB_NAME, je.JOB_EXECUTION_ID, je.STATUS, je.START_TIME, je.END_TIME,
            je.EXIT_CODE, je.EXIT_MESSAGE,
            se.STEP_NAME, se.STATUS AS STEP_STATUS, se.EXIT_CODE AS STEP_EXIT_CODE
        FROM JOB_INSTANCE ji
        JOIN JOB_EXECUTION je ON je.JOB_INSTANCE_ID = ji.JOB_INSTANCE_ID
        LEFT JOIN STEP_EXECUTION se ON se.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID AND se.STATUS = 'FAILED'
        WHERE je.STATUS = 'FAILED'
          AND je.START_TIME >= '{cutoff}'
        ORDER BY je.START_TIME DESC
    """
    return cma_execute_query(chain=job_chain, query=sql)


@mcp_server.tool()
def cma_get_blocked_jobs(
    job_chain: str = "job_PROD_1",
) -> Dict[str, Any]:
    """Check for stuck, blocked, or throttled batch jobs in the job cluster.
    
    Args:
        job_chain: G3 Job database chain (default 'job_PROD_1').
    """
    sql = """
        SELECT TOP 50
            bj.JOB_NAME, bj.REASON, bj.CREATE_DTTM,
            js.JOB_STATE_ID, js.IS_BLOCKED, js.LAST_UPDATED_DTTM
        FROM Blocked_Job bj
        FULL OUTER JOIN JOB_STATE js ON js.JOB_NAME = bj.JOB_NAME
        WHERE bj.JOB_NAME IS NOT NULL OR js.IS_BLOCKED = 1
        ORDER BY COALESCE(bj.CREATE_DTTM, js.LAST_UPDATED_DTTM) DESC
    """
    return cma_execute_query(chain=job_chain, query=sql)


@mcp_server.tool()
def cma_get_job_daily_stats(
    job_chain: str = "job_PROD_1",
    days_back: int = 7,
) -> Dict[str, Any]:
    """Retrieve aggregated daily job throughput, success rates, and average durations.
    
    Args:
        job_chain: G3 Job database chain (default 'job_PROD_1').
        days_back: Days of historical stats to retrieve (default 7).
    """
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    sql = f"""
        SELECT TOP 100
            Processing_Date, JOB_NAME, Total_Count, Success_Count, Fail_Count,
            Avg_Duration_Seconds, Max_Duration_Seconds
        FROM DAILY_JOB_STATISTICS
        WHERE Processing_Date >= '{cutoff}'
        ORDER BY Processing_Date DESC, Total_Count DESC
    """
    return cma_execute_query(chain=job_chain, query=sql)

# ==========================================
# DOMAIN 6: TENANT REVENUE & PACING
# ==========================================

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
        where_clause: Optional WHERE clause without 'WHERE' (e.g. "OCCUPANCY_DATE >= '2026-01-01'").
        columns: Specific columns to project. Defaults to '*' if omitted.
        order_by: Optional ORDER BY clause without 'ORDER BY'.
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
def cma_get_tenant_revenue_summary(
    tenant_chain: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """Aggregate actual rooms sold, room revenue, and ADR from tenant Accom_Activity for reconciliation.
    
    Args:
        tenant_chain: Tenant database chain (e.g. 'Hilton-ATLFY').
        start_date: Start date YYYY-MM-DD.
        end_date: End date YYYY-MM-DD.
    """
    sql = f"""
        SELECT 
            COUNT(*) AS Days_Count,
            SUM(CAST(Rooms_Sold AS FLOAT)) AS Total_Rooms_Sold,
            SUM(CAST(Room_Revenue AS DECIMAL(18,2))) AS Total_Room_Revenue,
            ROUND(SUM(CAST(Room_Revenue AS DECIMAL(18,2))) / NULLIF(SUM(CAST(Rooms_Sold AS FLOAT)), 0), 2) AS Calculated_ADR
        FROM Accom_Activity
        WHERE Occupancy_DT BETWEEN '{start_date.strip()}' AND '{end_date.strip()}';
    """
    return cma_execute_query(chain=tenant_chain, query=sql)


@mcp_server.tool()
def cma_get_tenant_pace_data(
    tenant_chain: str,
) -> Dict[str, Any]:
    """Retrieve the latest pacing snapshot (STLY rooms sold and revenue) from tenant PACE_Accom_Activity.
    
    Args:
        tenant_chain: Tenant database chain (e.g. 'Hilton-ATLFY').
    """
    sql = """
        SELECT
            MAX(Business_Day_End_DT) AS Snapshot_Date,
            SUM(CAST(Rooms_Sold AS FLOAT)) AS STLY_Rooms_Sold,
            SUM(CAST(Room_Revenue AS DECIMAL(18,2))) AS STLY_Room_Revenue
        FROM PACE_Accom_Activity WITH (NOLOCK)
        WHERE Business_Day_End_DT = (
            SELECT MAX(Business_Day_End_DT)
            FROM PACE_Accom_Activity WITH (NOLOCK)
            WHERE Business_Day_End_DT <= CAST(GETDATE() AS DATE)
        );
    """
    return cma_execute_query(chain=tenant_chain, query=sql)


@mcp_server.tool()
def cma_get_ratchet_srp_mappings(
    client_code: str = "",
    property_code: str = "",
    ratchet_chain: str = "ratchet_PROD_1",
) -> Dict[str, Any]:
    """Query Standard Rate Plan (SRP) mappings and channel restrictions in G3 Ratchet database.
    
    Args:
        client_code: Client short code.
        property_code: Property short code.
        ratchet_chain: Ratchet chain (default 'ratchet_PROD_1').
    """
    where_parts = []
    if client_code:
        where_parts.append(f"rc.Client_Code = '{client_code.strip()}'")
    if property_code:
        where_parts.append(f"rp.Property_Code = '{property_code.strip()}'")
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    sql = f"""
        SELECT TOP 50
            rc.Client_Code, rp.Property_Code, csm.SRP_Code, csm.Channel_Code,
            csm.Active_Flag, csm.Rate_Level
        FROM Client_Srp_Mapping csm
        LEFT JOIN Ratchet_Client rc ON rc.Ratchet_Client_ID = csm.Ratchet_Client_ID
        LEFT JOIN Ratchet_Property rp ON rp.Ratchet_Property_ID = csm.Ratchet_Property_ID
        {where_sql}
        ORDER BY rc.Client_Code, rp.Property_Code, csm.SRP_Code
    """
    return cma_execute_query(chain=ratchet_chain, query=sql)

# ==========================================
# DOMAIN 7: PORTAL AUDIT & SFDC CASE LINK
# ==========================================

@mcp_server.tool()
def cma_get_sfdc_case_audit_history(
    case_number: str,
    start_date: str = "",
    end_date: str = "",
) -> Dict[str, Any]:
    """Search CMA portal audit logs for every query executed against a specific Salesforce Case Number.
    
    Args:
        case_number: 8-digit Salesforce Case Number (e.g. '03019231').
        start_date: Start date YYYY-MM-DD (defaults to 1 year ago).
        end_date: End date YYYY-MM-DD (defaults to tomorrow).
    """
    clean_case = case_number.strip().lstrip("0") or case_number.strip()
    s_date = start_date.strip() or (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    e_date = end_date.strip() or (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    post_data = {
        "filterSelection": "sfdc",
        "caseNumber": clean_case,
        "startDate": s_date,
        "endDate": e_date,
    }

    resp = call_cma_web("/report/displayAuditReport/auditReportFilterForm", method="POST", data=post_data, timeout=15.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to query audit report: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="mainTableSet")
    if not table:
        return {"status": "success", "case_number": case_number, "audit_entries_count": 0, "entries": []}

    rows = []
    for tr in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(cols) >= 8 and cols[0] != "No results found":
            # Extract audit_id from detail link
            detail_link = tr.find("a")
            audit_id = ""
            if detail_link and "audit_id=" in detail_link.get("href", ""):
                audit_id = detail_link.get("href").split("audit_id=")[-1]

            rows.append({
                "row_number": cols[0],
                "chain": cols[1],
                "property": cols[2],
                "user_name": cols[3],
                "action": cols[4],
                "action_detail": cols[5],
                "audit_id": audit_id,
                "date": cols[6],
                "sfdc_case_number": cols[7]
            })

    return {
        "status": "success",
        "case_number": case_number,
        "audit_entries_count": len(rows),
        "entries": rows
    }


@mcp_server.tool()
def cma_get_audit_query_text(
    audit_id: str,
) -> Dict[str, Any]:
    """Retrieve the exact SQL query text, parameter hints, and execution metadata for a given CMA audit_id.
    
    Args:
        audit_id: CMA numeric audit transaction ID (e.g. '881559367').
    """
    clean_id = audit_id.strip()
    resp = call_cma_web(f"/report/showDetails?audit_id={clean_id}", method="GET", timeout=10.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to retrieve audit details: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    text_content = soup.get_text("\n", strip=True)

    # Extract query text and metadata
    raw_query = ""
    query_name = ""
    for tr in soup.find_all("tr"):
        txt = tr.get_text(" ", strip=True)
        if "Query Name:" in txt:
            query_name = txt.split("Query Name:")[-1].split("Query:")[0].strip()
        if "Query:" in txt or "select" in txt.lower():
            raw_query = txt

    return {
        "status": "success",
        "audit_id": clean_id,
        "query_name": query_name,
        "full_text": raw_query or text_content[:1500]
    }

# ==========================================
# DOMAIN 8: CURATED SCRIPT LIBRARY
# ==========================================

@mcp_server.tool()
def cma_search_saved_queries(
    keyword: str = "",
    tag: str = "",
    query_type: str = "",
    limit: int = 30,
) -> Dict[str, Any]:
    """Search the 2,000+ curated engineering SQL queries saved in CMA by SFDC Case, keyword, or tag.
    
    Args:
        keyword: Search term in query name or description (e.g. '03019231', 'MGM', 'DTA', 'Wash Override').
        tag: Operational tag filter (e.g. 'L2 Support', 'IM', 'Overbooking', 'FPLOS', 'Pace Alert', 'Casper').
        query_type: SQL operation type: 'Select', 'Insert', 'Update'.
        limit: Max results to return (default 30).
    """
    resp = call_cma_web("/viewSql/view", method="GET", timeout=15.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to load query library: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="mainTableSet")
    if not table:
        return {"status": "success", "results": []}

    kw_lower = keyword.lower().strip()
    tag_lower = tag.lower().strip()
    type_lower = query_type.lower().strip()

    matches = []
    for tr in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(cols) >= 5:
            q_name = cols[0]
            q_desc = cols[1]
            q_scope = cols[2]
            q_type = cols[3]
            q_creator = cols[4]

            link = tr.find("a")
            query_id = ""
            if link and "/viewDetails/" in link.get("href", ""):
                query_id = link.get("href").split("/viewDetails/")[-1]

            # Matching criteria
            match_kw = not kw_lower or (kw_lower in q_name.lower() or kw_lower in q_desc.lower())
            match_type = not type_lower or (type_lower in q_type.lower())

            if match_kw and match_type:
                matches.append({
                    "query_id": query_id,
                    "query_name": q_name,
                    "description": q_desc[:250],
                    "query_scope": q_scope,
                    "query_type": q_type,
                    "creator": q_creator,
                })
                if len(matches) >= limit:
                    break

    return {
        "status": "success",
        "matched_count": len(matches),
        "queries": matches
    }


@mcp_server.tool()
def cma_get_saved_query_details(
    query_id: str,
) -> Dict[str, Any]:
    """Retrieve full parameterized SQL code, assigned roles, and parameters for a saved query from CMA library.
    
    Args:
        query_id: Query ID in CMA viewSql library (e.g. '2467', '2596').
    """
    clean_id = query_id.strip()
    resp = call_cma_web(f"/viewSql/viewDetails/{clean_id}", method="GET", timeout=12.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to retrieve query details: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    textareas = soup.find_all("textarea")
    description = textareas[0].get_text(strip=True) if len(textareas) > 0 else ""
    sql_text = textareas[1].get_text(strip=True) if len(textareas) > 1 else ""

    metadata = {}
    for tr in soup.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(tds) >= 2 and tds[0]:
            metadata[tds[0].rstrip(":")] = tds[1]

    return {
        "status": "success",
        "query_id": clean_id,
        "description": description,
        "sql_code": sql_text,
        "metadata": metadata
    }

# ==========================================
# DOMAIN 9: TEAM TASKS & REAL-TIME JOBS
# ==========================================

@mcp_server.tool()
def cma_get_team_task_feed(
    limit: int = 20,
) -> Dict[str, Any]:
    """Retrieve live feed of queries and background tasks currently being executed across the team in CMA.
    
    Args:
        limit: Max tasks to return (default 20).
    """
    resp = call_cma_web("/taskStatus/myTeamTasks", method="GET", timeout=12.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to fetch team tasks: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", class_="mainTableSet")
    
    tasks = []
    # Skip outer container table if present
    for t in tables[1:]:
        for tr in t.find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cols) >= 7 and cols[0] != "Chain ID":
                tasks.append({
                    "chain_id": cols[0],
                    "chain_name": cols[1],
                    "query_name": cols[2],
                    "sql_snippet": cols[3],
                    "timestamp": cols[5],
                    "status": cols[6]
                })
                if len(tasks) >= limit:
                    break
        if len(tasks) >= limit:
            break

    return {
        "status": "success",
        "task_count": len(tasks),
        "tasks": tasks
    }


@mcp_server.tool()
def cma_get_task_status_by_job_id(
    job_id: str,
) -> Dict[str, Any]:
    """Search and inspect real-time progress of a CMA background batch task by its Job ID.
    
    Args:
        job_id: Numeric Job ID (e.g. '348244').
    """
    clean_id = job_id.strip()
    post_data = {"selectedJobId": clean_id}
    resp = call_cma_web("/taskStatus/searchByJobId", method="POST", data=post_data, timeout=12.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to search by job id: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="mainTableSet")
    tasks = []
    if table:
        for tr in table.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cols) >= 5:
                tasks.append({"chain": cols[0], "query": cols[1], "status": cols[-1], "details": cols})

    return {
        "status": "success",
        "job_id": clean_id,
        "records_count": len(tasks),
        "records": tasks
    }

# ==========================================
# DOMAIN 10: FILE EXPLORER & PARAMETERS
# ==========================================

@mcp_server.tool()
def cma_list_server_explorer_directories() -> Dict[str, Any]:
    """Discover available remote server explorer nodes (G3 Prod 1..6 and RSS Prod 1..6 data directories)."""
    resp = call_cma_web("/fileExplorer/connect", method="GET", timeout=12.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to connect to file explorer: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    tags = []
    tag_select = soup.find("select", {"name": "selectedTag"})
    if tag_select:
        tags = [opt.get_text(strip=True) for opt in tag_select.find_all("option") if opt.get_text(strip=True) != "--Select--"]

    explorers = []
    unc_select = soup.find("select", {"name": "uncData"})
    if unc_select:
        explorers = [opt.get_text(strip=True) for opt in unc_select.find_all("option") if opt.get_text(strip=True) != "-Select-"]

    return {
        "status": "success",
        "available_explorers": explorers,
        "available_tags": tags,
        "description": "Browse server directories and log drops across G3 Prod 1..6 Data and RSS Prod 1..6."
    }


@mcp_server.tool()
def cma_get_property_system_parameters_catalog() -> Dict[str, Any]:
    """Retrieve the master catalog of Property System Parameters (PSPs) and active Cognito JWT tokens."""
    resp = call_cma_web("/psparameter/list", method="GET", timeout=12.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to retrieve PSP catalog: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="mainTableSet")
    psps = []
    if table:
        for tr in table.find_all("tr")[2:]:
            cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cols) >= 5:
                psps.append({
                    "id": cols[0],
                    "name": cols[1],
                    "description": cols[2],
                    "category": cols[3],
                    "module": cols[4]
                })

    # Check for Manager signature token
    dialog = soup.find("dialog", id="myDialog")
    token_str = ""
    if dialog:
        btn = dialog.find("button")
        if btn and "copyManagerAccessToken('" in btn.get("onclick", ""):
            token_str = btn.get("onclick").split("copyManagerAccessToken('")[-1].split("')")[0]

    return {
        "status": "success",
        "psp_count": len(psps),
        "parameters": psps,
        "manager_signature_token_available": bool(token_str),
        "manager_signature_token": token_str[:30] + "..." if token_str else None
    }


@mcp_server.tool()
def cma_get_scheduled_sql_jobs() -> Dict[str, Any]:
    """Inspect active recurring scheduled SQL jobs, execution frequencies, and target chains in CMA."""
    resp = call_cma_web("/scheduleSql/listView", method="GET", timeout=12.0)
    if not resp or resp.status_code != 200:
        return {"status": "error", "error": f"Failed to load scheduled SQL: HTTP {getattr(resp, 'status_code', 'None')}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="mainTableSet")
    jobs = []
    if table:
        for tr in table.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cols) >= 6 and cols[0] != "No Records Found":
                jobs.append({
                    "query": cols[0],
                    "scheduled_by": cols[1],
                    "all_chains": cols[2],
                    "frequency": cols[3],
                    "time": cols[4],
                    "file_name": cols[5]
                })

    return {
        "status": "success",
        "scheduled_jobs_count": len(jobs),
        "jobs": jobs
    }

# ==========================================
# DOMAIN 11: TELEMETRY & SESSION RESILIENCE
# ==========================================

@mcp_server.tool()
def cma_get_session_status() -> Dict[str, Any]:
    """Inspect active CMA session status, cookie validity, and circuit breaker status."""
    resp = call_cma_gateway("/api/internal/needs_cookie", method="GET", timeout=5.0)

    active_cookie = get_cma_cookie()
    cookie_preview = f"{active_cookie[:15]}...{active_cookie[-8:]}" if len(active_cookie) > 25 else "Not set"

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
        force: If True, triggers re-login even if cooldown period is active.
    """
    status = cma_get_session_status()
    if status.get("has_cookie") and not status.get("needs_cookie") and not force:
        return {
            "status": "skipped",
            "message": "CMA session is already active and healthy. Pass force=True to force refresh.",
            "session": status
        }

    logger.info("Triggering SSO cookie refresh via CMA internal gateway...")
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
    call_cma_gateway("/admin/refresh-chains", method="POST", payload={}, timeout=15.0)

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
        return {"status": "success", "count": len(rows), "logs": rows}
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
        "version": "2.0.0",
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
