import os
import io
import re
import json
import time
import zipfile
import sqlite3
import secrets
import threading
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from flask import Flask, request, jsonify, send_file, render_template_string, redirect, url_for, session, g, Response
import logging
import subprocess

# Configure terminal logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()

BASE_URL = "https://g3-cma.ideas.com/cma/adhocSql"
DB_FILE = "api_gateway.db"
PORT = int(os.getenv("PORT", 8555))
ADMIN_USER = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASSWORD", "admin123")

ACTIVE_CMA_COOKIE = os.getenv("CMA_COOKIE", "")
COOKIE_LOCK = threading.Lock()
AUTH_RECOVERY_EVENT = threading.Event()

def get_headers():
    global ACTIVE_CMA_COOKIE
    with COOKIE_LOCK:
        cookie = ACTIVE_CMA_COOKIE
    
    # Log if the cookie is empty or its length
    if not cookie:
        logger.warning("ACTIVE_CMA_COOKIE is empty!")
    else:
        logger.debug(f"Using ACTIVE_CMA_COOKIE: {cookie[:15]}... (Length: {len(cookie)})")
        
    return {
        "Referer": f"{BASE_URL}/viewAdhoc",
        "Origin": "https://g3-cma.ideas.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Cookie": cookie
    }

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "quant_terminal_pro_99")

# ==========================================
# CIRCUIT BREAKER & MAINTENANCE RESILIENCE
# ==========================================
CIRCUIT_BREAKER_UNTIL = 0
CONSECUTIVE_AUTH_FAILURES = 0
CIRCUIT_BREAKER_BASE_SECONDS = 180  # 3 minutes

def is_circuit_breaker_active():
    global CIRCUIT_BREAKER_UNTIL
    remaining = CIRCUIT_BREAKER_UNTIL - time.time()
    if remaining > 0:
        return True, int(remaining)
    return False, 0

def trip_circuit_breaker(reason="CMA auto-login timed out or login is blocked"):
    global CIRCUIT_BREAKER_UNTIL, CONSECUTIVE_AUTH_FAILURES
    CONSECUTIVE_AUTH_FAILURES += 1
    duration = min(CIRCUIT_BREAKER_BASE_SECONDS * CONSECUTIVE_AUTH_FAILURES, 600)
    CIRCUIT_BREAKER_UNTIL = time.time() + duration
    logger.warning(f"⚠️ CIRCUIT BREAKER TRIPPED ({reason}). Suppressing CMA auto-login for {duration}s.")

def reset_circuit_breaker():
    global CIRCUIT_BREAKER_UNTIL, CONSECUTIVE_AUTH_FAILURES
    if CONSECUTIVE_AUTH_FAILURES > 0 or CIRCUIT_BREAKER_UNTIL > 0:
        logger.info("✅ Circuit breaker RESET. CMA login is fully restored.")
    CONSECUTIVE_AUTH_FAILURES = 0
    CIRCUIT_BREAKER_UNTIL = 0

def save_chains_cache():
    try:
        cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chains_cache.json')
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(GLOBAL_CHAINS, f)
        logger.debug(f"Saved {len(GLOBAL_CHAINS)} chains to chains_cache.json")
    except Exception as e:
        logger.error(f"Failed to save chains cache: {e}")

def load_chains_cache():
    global GLOBAL_CHAINS
    try:
        cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chains_cache.json')
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached = json.load(f)
                if cached and isinstance(cached, dict):
                    GLOBAL_CHAINS = cached
                    logger.info(f"Loaded {len(GLOBAL_CHAINS)} chains from local cache chains_cache.json")
                    return True
    except Exception as e:
        logger.error(f"Failed to load chains cache: {e}")
    return False

# ==========================================
# AUTO-LOGIN TRIGGER LOGIC
# ==========================================
LAST_BROWSER_LAUNCH = 0
LAUNCH_COOLDOWN_SECONDS = 15  

def trigger_sso_login(force=False):
    global LAST_BROWSER_LAUNCH
    # If circuit breaker is active, do NOT launch Chrome
    active, remaining = is_circuit_breaker_active()
    if active and not force:
        logger.warning(f"Skipping Chrome launch: Circuit breaker is active ({remaining}s remaining). CMA login is blocked/maintenance.")
        return False

    current_time = time.time()
    time_since_last = current_time - LAST_BROWSER_LAUNCH
    
    logger.debug(f"trigger_sso_login called (force={force}). Last launch was {time_since_last:.1f}s ago.")
    
    if force or (time_since_last > LAUNCH_COOLDOWN_SECONDS):
        try:
            bat_dir = os.path.dirname(os.path.abspath(__file__))
            bat_path = os.path.join(bat_dir, 'launch_chrome.bat')
            
            logger.info(f"Session expired! Attempting to launch batch file at: {bat_path}")
            
            if not os.path.exists(bat_path):
                logger.error(f"CRITICAL: Batch file NOT FOUND at {bat_path}")
                return False

            # Execute the batch file
            subprocess.Popen(['cmd.exe', '/c', 'launch_chrome.bat'], cwd=bat_dir)
            logger.info("Batch file execution command sent successfully.")
            LAST_BROWSER_LAUNCH = current_time
            return True
            
        except Exception as e:
            logger.error(f"Failed to launch Chrome: {e}", exc_info=True)
            return False
    else:
        logger.debug(f"Skipping Chrome launch due to {LAUNCH_COOLDOWN_SECONDS}s cooldown ({time_since_last:.1f}s elapsed).")
        return False
# ==========================================
# 1. DATABASE & AUDIT LOGGING
# ==========================================

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_FILE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(error):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key TEXT UNIQUE NOT NULL,
            client_name TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key_id INTEGER,
            client_name TEXT,
            endpoint TEXT NOT NULL,
            http_method TEXT NOT NULL,
            ip_address TEXT,
            chains_requested TEXT,
            query_executed TEXT,
            chains_count INTEGER,
            queries_count INTEGER,
            status_code INTEGER,
            status_message TEXT,
            execution_time_seconds REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
        )
    """)
    
    try:
        cursor.execute("ALTER TABLE audit_logs ADD COLUMN query_executed TEXT")
    except sqlite3.OperationalError:
        pass 
        
    conn.commit()
    conn.close()

init_db()

def log_audit_entry(status_code, status_message, client_name="Anonymous", chains_list=None, queries_count=0, exec_time=0.0, query_executed=""):
    try:
        db = get_db()
        chains_str = ",".join(chains_list) if chains_list else ""
        db.execute("""
            INSERT INTO audit_logs (
                client_name, endpoint, http_method, ip_address,
                chains_requested, query_executed, chains_count, queries_count, status_code,
                status_message, execution_time_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            client_name, request.path, request.method,
            request.headers.get('X-Forwarded-For', request.remote_addr),
            chains_str, query_executed, len(chains_list) if chains_list else 0,
            queries_count, status_code, status_message, round(exec_time, 3)
        ))
        db.commit()
    except Exception as e:
        app.logger.error(f"Audit log failed: {e}")

# ==========================================
# 2. INSTANT CACHE & BACKGROUND SYNC
# ==========================================

GLOBAL_CHAINS = {}

def sync_chains():
    global GLOBAL_CHAINS
    logger.info("Background chain sync thread started.")
    while True:
        try:
            logger.debug("Pinging CMA Server to check session...")
            res = requests.get(f"{BASE_URL}/viewAdhoc", headers=get_headers(), timeout=15)
            
            if "login" in res.url.lower() or "sso" in res.url.lower():
                logger.warning("SSO Redirect detected in background thread.")
                trigger_sso_login()
            elif res.status_code in [401, 403]:
                logger.warning(f"Server returned HTTP {res.status_code}. Session likely invalid. Triggering login.")
                trigger_sso_login()
            elif res.status_code == 200:
                soup = BeautifulSoup(res.text, "html.parser")
                select = soup.find("select", {"id": "chainCode"})
                if select:
                    new_chains = {opt.text.strip(): opt.get("value") for opt in select.find_all("option") if opt.get("value")}
                    if new_chains:
                        GLOBAL_CHAINS = new_chains
                        logger.debug(f"Successfully loaded {len(GLOBAL_CHAINS)} chains.")
                        save_chains_cache()
        except Exception as e:
            logger.error(f"Sync Thread Exception: {e}")
            
        time.sleep(300)  # REQUIRED: This stops the infinite spam loop!

# --- GLOBAL STARTUP FETCH ---
load_chains_cache()

try:
    logger.debug("Running initial startup chain fetch...")
    res = requests.get(f"{BASE_URL}/viewAdhoc", headers=get_headers(), timeout=15)
    
    if "login" in res.url.lower() or "sso" in res.url.lower():
        logger.warning("Startup detected expired token. Auto-login will trigger once server starts listening.")
    elif res.status_code == 200:
        soup = BeautifulSoup(res.text, "html.parser")
        sel = soup.find("select", {"id": "chainCode"})
        if sel:
            GLOBAL_CHAINS = {o.text.strip(): o.get("value") for o in sel.find_all("option") if o.get("value")}
            logger.debug(f"Startup successfully loaded {len(GLOBAL_CHAINS)} chains.")
            save_chains_cache()
except Exception as e:
    logger.error(f"Startup chain fetch failed: {e}")

# Start the background thread
threading.Thread(target=sync_chains, daemon=True, name="sync_chains").start()



# ==========================================
# 3. HIGH-SPEED CONCURRENT EXECUTION
# ==========================================

def wait_for_auth_recovery(timeout=45, trigger_login=True):
    """Waits for the Chrome extension to push a new cookie via /api/internal/update_cookie.
    Uses zero-latency thread signaling (threading.Event) with Circuit Breaker protection."""
    active, remaining = is_circuit_breaker_active()
    if active:
        logger.warning(f"Circuit breaker active ({remaining}s remaining). Aborting auto-login wait.")
        return False

    AUTH_RECOVERY_EVENT.clear()
    if trigger_login:
        launched = trigger_sso_login()
        if not launched:
            active, remaining = is_circuit_breaker_active()
            if active:
                return False

    logger.info(f"Waiting up to {timeout}s for auth recovery signal from Chrome extension...")
    recovered = AUTH_RECOVERY_EVENT.wait(timeout=timeout)
    
    active, remaining = is_circuit_breaker_active()
    if active:
        logger.warning("Auth recovery aborted: Circuit breaker was tripped.")
        return False

    if recovered:
        logger.info("Auth recovery signal received successfully.")
        reset_circuit_breaker()
        return True
        
    logger.warning(f"Auth recovery timed out after {timeout}s. Tripping circuit breaker.")
    trip_circuit_breaker(reason=f"Timeout after {timeout}s waiting for Chrome auto-login")
    return False

def fetch_single_query(chain_name, chain_code, q_idx, sql):
    multipart = {
        "chainCode":  (None, chain_code),
        "resultType": (None, "export"),
        "adhocQuery": (None, sql),
        "chainName":  (None, chain_name),
    }
    try:
        hdrs = get_headers()
        hdrs.pop("Content-Type", None)
        res = requests.post(f"{BASE_URL}/runViewAdhocQuery", headers=hdrs, files=multipart, timeout=90)
        
        # --- Detect expiration ---
        if "login" in res.url.lower() or "sso" in res.url.lower():
            return chain_name, q_idx, None, "EXPIRED"

        if res.status_code == 200:
            df = pd.read_excel(io.BytesIO(res.content), engine="openpyxl", dtype=str)
            return chain_name, q_idx, df, None
        return chain_name, q_idx, None, f"HTTP {res.status_code}"
    except Exception as e:
        return chain_name, q_idx, None, str(e)

def execute_cma_batch(requested_chains, queries_list, is_retry=False):
    global GLOBAL_CHAINS
    
    # Fast-fail immediately if CMA login is blocked / undergoing maintenance
    active, remaining = is_circuit_breaker_active()
    if active:
        return None, f"IDeaS CMA login is currently blocked or under maintenance. Auto-recovery paused for {remaining}s."

    # 1. Catch case where server restarts with a dead token and chains are completely empty
    if not GLOBAL_CHAINS:
        logger.warning("GLOBAL_CHAINS is empty. Triggering auto-login and waiting...")
        if not wait_for_auth_recovery(trigger_login=True):
            active, remaining = is_circuit_breaker_active()
            if active:
                return None, f"IDeaS CMA login is currently blocked or under maintenance. Auto-recovery paused for {remaining}s."
            return None, "CMA session expired. Auto-login triggered, but timed out waiting for Chrome."

    chain_results = {c.split(" - ")[0] if " - " in c else c: {} for c in requested_chains}
    errors = []
    needs_reauth = False

    tasks = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for chain_identifier in requested_chains:
            chain_code = GLOBAL_CHAINS.get(chain_identifier, chain_identifier)
            chain_name = chain_identifier.split(" - ")[0] if " - " in chain_identifier else chain_identifier
            for q_idx, sql in enumerate(queries_list):
                tasks.append(executor.submit(fetch_single_query, chain_name, chain_code, q_idx, sql))

        for future in as_completed(tasks):
            chain_name, q_idx, df, err = future.result()
            if df is not None:
                chain_results[chain_name][f"Query_{q_idx + 1}"] = df
            else:
                if err == "EXPIRED":
                    needs_reauth = True
                else:
                    errors.append(f"{chain_name} (Q{q_idx + 1}): {err}")

    # 2. Catch case where token died silently during actual query execution
    if needs_reauth and not is_retry:
        logger.warning("Session expired during execution. Holding request and triggering re-auth...")
        if wait_for_auth_recovery(trigger_login=True):
            logger.info("Re-auth successful! Retrying the batch invisibly...")
            return execute_cma_batch(requested_chains, queries_list, is_retry=True)
        else:
            active, remaining = is_circuit_breaker_active()
            if active:
                return None, f"IDeaS CMA login is currently blocked or under maintenance. Auto-recovery paused for {remaining}s."
            return None, "CMA session expired. Auto-login was triggered, but timed out waiting for Chrome."
    elif needs_reauth and is_retry:
        return None, "Execution failed: Session expired again during the retry attempt."

    if not any(chain_results.values()):
        return None, f"Execution failed: {', '.join(errors)}"

    return chain_results, None


def create_excel(queries_dict):
    """Generates a single multi-sheet Excel file in memory."""
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        for sheet in sorted(queries_dict.keys()):
            queries_dict[sheet].to_excel(writer, index=False, sheet_name=sheet[:31])
    excel_buffer.seek(0)
    return excel_buffer

def create_zip(chain_results):
    """Generates a ZIP archive containing multiple Excel files."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for chain, queries_dict in chain_results.items():
            if not queries_dict: continue
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                for sheet in sorted(queries_dict.keys()):
                    queries_dict[sheet].to_excel(writer, index=False, sheet_name=sheet[:31])
            safe_chain = "".join(c for c in chain if c.isalnum() or c in (' ', '_', '-')).strip()
            zip_file.writestr(f"{safe_chain}_TaskDetailReport.xlsx", excel_buffer.getvalue())

    zip_buffer.seek(0)
    return zip_buffer
    


# ==========================================
# 4. HTML TEMPLATES 
# ==========================================

CLIENT_HTML = """
<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CMA TERMINAL | Workspace</title>
    
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/tom-select@2.2.2/dist/css/tom-select.bootstrap5.min.css" rel="stylesheet">
    
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/codemirror.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/theme/material-darker.min.css">

    <style>
        :root {
            --bg-main: #09090b;      
            --bg-panel: #18181b;     
            --border-color: #27272a; 
            --primary: #3b82f6;
            --primary-hover: #2563eb;
        }
        body { background-color: var(--bg-main); font-family: 'Inter', sans-serif; overflow-x: hidden; height: 100vh; display: flex; flex-direction: column;}
        
        .navbar-custom { background-color: var(--bg-panel); border-bottom: 1px solid var(--border-color); padding: 12px 24px; display: flex; justify-content: space-between; align-items: center;}
        .brand { font-size: 1.15rem; font-weight: 600; letter-spacing: -0.5px;}
        .brand i { color: var(--primary); margin-right: 8px;}
        
        .workspace { display: flex; flex: 1; overflow: hidden; }
        .sidebar { width: 340px; background-color: var(--bg-panel); border-right: 1px solid var(--border-color); padding: 24px; display: flex; flex-direction: column; overflow-y: auto;}
        .main-area { flex: 1; padding: 24px; display: flex; flex-direction: column;}
        
        .section-title { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1px; color: #a1a1aa; font-weight: 600; margin-bottom: 12px; }
        
        .form-control, .form-select { background-color: var(--bg-main) !important; border-color: var(--border-color) !important; color: #f4f4f5 !important; }
        .form-control:focus { border-color: var(--primary) !important; box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.25) !important; }
        
        .ts-control { background-color: var(--bg-main) !important; border: 1px solid var(--border-color) !important; color: #fff !important; border-radius: 6px; padding: 10px; }
        .ts-dropdown { background-color: var(--bg-panel) !important; border: 1px solid var(--border-color) !important; border-radius: 6px; }
        .ts-dropdown .option { color: #d4d4d8; padding: 10px 12px; }
        .ts-dropdown .active { background-color: #27272a !important; color: #fff !important; }
        .ts-control .item { background-color: #27272a !important; color: #fff !important; border: 1px solid #3f3f46 !important; border-radius: 4px; }
        
        .editor-wrapper { flex: 1; border: 1px solid var(--border-color); border-radius: 8px; overflow: hidden; display: flex; flex-direction: column; }
        .editor-header { background-color: var(--bg-panel); padding: 10px 16px; border-bottom: 1px solid var(--border-color); font-size: 0.85rem; color: #a1a1aa; display: flex; align-items: center;}
        .CodeMirror { flex: 1; height: 100% !important; font-family: 'JetBrains Mono', Consolas, monospace; font-size: 14px; padding-top: 10px;}
        
        .btn-execute { background: var(--primary); border: none; color: white; font-weight: 500; padding: 12px; border-radius: 6px; transition: all 0.2s ease; display: flex; align-items: center; justify-content: center; gap: 8px;}
        .btn-execute:hover { background: var(--primary-hover); color: white;}
        .btn-secondary-custom { background: var(--bg-panel); border: 1px solid var(--border-color); color: #fff; }
        .btn-secondary-custom:hover { background: #27272a; color: #fff;}
    </style>
</head>
<body>

    <div class="navbar-custom">
        <div class="brand"><i class="fa-solid fa-terminal"></i> CMA Terminal</div>
        <a href="/admin" class="btn btn-sm btn-outline-secondary rounded-pill px-3"><i class="fa-solid fa-shield-halved me-2"></i>Admin Console</a>
    </div>

    <form method="POST" action="/client/run" id="queryForm" class="workspace">
        
        <div class="sidebar">
            <div class="section-title">Authentication</div>
            
            {% if error %}
            <div class="alert alert-danger p-2 small mb-3 border-0 bg-danger text-white bg-opacity-25 rounded-2">
                <i class="fa-solid fa-circle-exclamation me-1"></i> {{ error }}
            </div>
            {% endif %}

            <div class="mb-4">
                <div class="input-group">
                    <span class="input-group-text bg-dark border-secondary text-secondary"><i class="fa-solid fa-key"></i></span>
                    <input type="password" name="api_key" class="form-control" placeholder="Enter API Key" required autocomplete="off">
                </div>
            </div>

            <div class="section-title mt-2">Execution Target</div>
            <div class="mb-4">
                <select id="chain-select" name="chains" multiple placeholder="Search target chains..." autocomplete="off" required>
                    {% for chain in chains %}
                    <option value="{{ chain }}">{{ chain }}</option>
                    {% endfor %}
                </select>
                <div class="form-text text-muted small mt-2"><i class="fa-solid fa-circle-info me-1"></i> Select properties for concurrent execution.</div>
            </div>
            
            <div class="mt-auto d-flex flex-column gap-2">
                <button type="submit" name="action" value="display" class="btn-execute btn-secondary-custom w-100" id="btnDisplay">
                    <i class="fa-solid fa-table"></i> Run & Display
                </button>
                <button type="submit" name="action" value="download" class="btn-execute w-100" id="btnDownload">
                    <i class="fa-solid fa-file-arrow-down"></i> Download File
                </button>
            </div>
        </div>

        <div class="main-area">
            <div class="editor-wrapper shadow-sm">
                <div class="editor-header">
                    <i class="fa-solid fa-database me-2 text-primary"></i> SQL Batch Editor
                </div>
                <textarea id="sql-editor" name="query" required>-- Enter your SQL queries below.
-- Note: Comments starting with '--' will be automatically stripped out.

SELECT TOP 10 * FROM Property;
SELECT TOP 10 * FROM Accom_Type;</textarea>
            </div>
        </div>
        
    </form>

<script src="https://cdn.jsdelivr.net/npm/tom-select@2.2.2/dist/js/tom-select.complete.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/codemirror.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/mode/sql/sql.min.js"></script>
<script>
    document.addEventListener('DOMContentLoaded', function() {
        new TomSelect('#chain-select', {
            plugins: ['remove_button'],
            maxItems: null,
            placeholder: 'Search chains...'
        });

        const editor = CodeMirror.fromTextArea(document.getElementById('sql-editor'), {
            mode: 'text/x-sql',
            theme: 'material-darker',
            lineNumbers: true,
            lineWrapping: true,
            matchBrackets: true,
            indentUnit: 4
        });

        const form = document.getElementById('queryForm');
        const btnDisplay = document.getElementById('btnDisplay');
        const btnDownload = document.getElementById('btnDownload');
        let submitAction = 'download';

        btnDisplay.addEventListener('click', () => submitAction = 'display');
        btnDownload.addEventListener('click', () => submitAction = 'download');

        form.addEventListener('submit', function(e) {
            editor.save();
            if(form.checkValidity()){
                const activeBtn = submitAction === 'display' ? btnDisplay : btnDownload;
                
                const originalText = activeBtn.innerHTML;
                activeBtn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Processing...';
                
                btnDisplay.style.pointerEvents = 'none';
                btnDownload.style.pointerEvents = 'none';
                btnDisplay.style.opacity = '0.6';
                btnDownload.style.opacity = '0.6';
                
                setTimeout(() => {
                    activeBtn.innerHTML = originalText;
                    btnDisplay.style.pointerEvents = 'auto';
                    btnDownload.style.pointerEvents = 'auto';
                    btnDisplay.style.opacity = '1';
                    btnDownload.style.opacity = '1';
                }, 8000);
            }
        });
    });
</script>
</body>
</html>
"""

RESULTS_HTML = """
<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
    <meta charset="UTF-8">
    <title>Query Results | CMA Terminal</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        body { background-color: #09090b; font-family: 'Inter', sans-serif; color: #f4f4f5; }
        .table-container { background-color: #18181b; border: 1px solid #27272a; border-radius: 8px; overflow: auto; max-height: 500px; }
        .table th { position: sticky; top: 0; background-color: #27272a; z-index: 1; border-bottom: 2px solid #3f3f46; font-size: 0.85rem;}
        .table td { font-size: 0.9rem; border-color: #27272a;}
        .chain-header { background-color: #18181b; border: 1px solid #27272a; padding: 12px 20px; border-radius: 8px; margin-top: 32px; margin-bottom: 16px; border-left: 4px solid #3b82f6;}
    </style>
</head>
<body class="p-4">
    <div class="container-fluid">
        <div class="d-flex justify-content-between align-items-center mb-4 pb-3 border-bottom border-secondary">
            <h4 class="mb-0 fw-semibold"><i class="fa-solid fa-table text-primary me-2"></i> Execution Results</h4>
            <a href="/" class="btn btn-outline-secondary rounded-pill px-4"><i class="fa-solid fa-arrow-left me-2"></i>Back to Workspace</a>
        </div>
        
        {% for chain, queries in results.items() %}
            <div class="chain-header shadow-sm">
                <h5 class="mb-0 text-white fw-semibold"><i class="fa-solid fa-server me-2 text-muted"></i>{{ chain }}</h5>
            </div>
            {% for sheet, data in queries.items() %}
                <div class="d-flex justify-content-between align-items-end mt-4 mb-2 px-2">
                    <h6 class="mb-0 text-secondary fw-semibold"><i class="fa-solid fa-code text-info me-2"></i>{{ sheet }}</h6>
                    <span class="badge bg-dark border border-secondary text-light">Showing {{ data.shown_rows }} of {{ data.total_rows }} rows</span>
                </div>
                <div class="table-container shadow-sm mb-4">
                    {{ data.html | safe }}
                </div>
            {% endfor %}
        {% endfor %}
    </div>
</body>
</html>
"""

ADMIN_LOGIN_HTML = """
<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
    <meta charset="UTF-8">
    <title>Gateway Authentication</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        body { background-color: #09090b; font-family: 'Inter', sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; }
        .card { background-color: #18181b; border: 1px solid #27272a; border-radius: 12px; width: 100%; max-width: 380px; padding: 32px;}
        .form-control { background-color: #09090b; border: 1px solid #27272a; color: #fff; border-radius: 6px; padding: 12px;}
        .form-control:focus { border-color: #3b82f6; box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2); }
        .btn-primary { background: #3b82f6; border: none; padding: 12px; border-radius: 6px; font-weight: 500;}
    </style>
</head>
<body>
<div class="card shadow-lg">
    <h5 class="text-center text-white mb-4 fw-semibold">Gateway Access</h5>
    {% if error %}
    <div class="alert alert-danger small py-2 border-0 bg-danger text-white bg-opacity-25">{{ error }}</div>
    {% endif %}
    <form method="POST" action="/admin/login">
        <div class="mb-3">
            <input type="text" name="username" class="form-control" placeholder="Admin Username" required autocomplete="off">
        </div>
        <div class="mb-4">
            <input type="password" name="password" class="form-control" placeholder="Password" required>
        </div>
        <button type="submit" class="btn btn-primary w-100">Authenticate</button>
        <div class="text-center mt-4">
            <a href="/" class="text-secondary text-decoration-none small">Return to Workspace</a>
        </div>
    </form>
</div>
</body>
</html>
"""

ADMIN_DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
    <meta charset="UTF-8">
    <title>Admin Console</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        body { background-color: #09090b; font-family: 'Inter', sans-serif; }
        .navbar-custom { background-color: #18181b; border-bottom: 1px solid #27272a; padding: 16px 24px; }
        .card { background-color: #18181b; border: 1px solid #27272a; border-radius: 8px; }
        .table { font-size: 0.9rem;}
        .table thead th { border-bottom: 1px solid #27272a; color: #a1a1aa; font-weight: 500; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px;}
        .table td { border-bottom: 1px solid #27272a; padding: 16px 12px; vertical-align: middle; color: #d4d4d8;}
        .form-control { background-color: #09090b; border: 1px solid #27272a; }
        .badge { font-weight: 500; border-radius: 4px; }
    </style>
</head>
<body>

<div class="navbar-custom d-flex justify-content-between align-items-center mb-4">
    <div class="fw-semibold fs-5"><i class="fa-solid fa-shield-halved text-primary me-2"></i> Security & Gateway Admin</div>
    <div>
        <a href="/" class="btn btn-sm btn-outline-secondary me-2 px-3">Workspace</a>
        <a href="/admin/logout" class="btn btn-sm btn-danger px-3">Logout</a>
    </div>
</div>

<div class="container-fluid px-4 pb-5">

    <!-- Toast Notification -->
    <div id="refreshToast" style="position:fixed;top:24px;right:24px;z-index:9999;min-width:300px;display:none;">
        <div id="refreshToastInner" style="padding:14px 18px;border-radius:8px;font-size:0.9rem;font-family:'Inter',sans-serif;display:flex;align-items:center;gap:10px;box-shadow:0 4px 20px rgba(0,0,0,0.4);">
            <i id="refreshToastIcon" class="fa-solid"></i>
            <span id="refreshToastMsg"></span>
        </div>
    </div>

    <div class="row g-4 mb-4">
        <div class="col-md-4">
            <div class="card p-4 h-100 shadow-sm">
                <h6 class="mb-3 text-white fw-semibold">Provision API Key</h6>
                <form method="POST" action="/admin/keys/create">
                    <div class="mb-3">
                        <input type="text" name="client_name" class="form-control" placeholder="System/Client Name" required autocomplete="off">
                    </div>
                    <button type="submit" class="btn btn-primary w-100 fw-medium" style="background:#3b82f6; border:none; padding:10px; border-radius:6px; color:white;">Generate Secret</button>
                </form>
                {% if new_key %}
                <div class="mt-4 p-3 border border-success border-opacity-50 rounded-2" style="background-color: rgba(34, 197, 94, 0.1);">
                    <div class="small text-success fw-semibold mb-1">Success! Store this key securely:</div>
                    <code class="text-white fs-6">{{ new_key }}</code>
                </div>
                {% endif %}

                <hr style="border-color:#27272a;margin:20px 0;">
                <h6 class="mb-2 text-white fw-semibold">Refresh CMA Session</h6>
                <p class="text-muted small mb-3">After updating <code>CMA_COOKIE</code> in <code>.env</code>, click below to reload chains immediately — no restart needed.</p>
                <button id="btnRefreshChains" onclick="refreshChains()" class="btn w-100 fw-medium" style="background:#18181b;border:1px solid #3f3f46;color:#d4d4d8;padding:10px;border-radius:6px;transition:all 0.2s;">
                    <i class="fa-solid fa-rotate me-2 text-info"></i>Refresh CMA Chains
                </button>
            </div>
        </div>

        <div class="col-md-8">
            <div class="card p-4 h-100 shadow-sm overflow-hidden">
                <h6 class="mb-3 text-white fw-semibold">Active Credentials</h6>
                <div class="table-responsive">
                    <table class="table table-borderless align-middle mb-0">
                        <thead>
                            <tr>
                                <th>Client Name</th>
                                <th>Identifier</th>
                                <th>Status</th>
                                <th>Created</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for key in keys %}
                            <tr>
                                <td class="text-white fw-medium">{{ key.client_name }}</td>
                                <td><code class="text-secondary bg-dark px-2 py-1 rounded">{{ key.api_key[:8] }}...{{ key.api_key[-4:] }}</code></td>
                                <td>
                                    <span class="badge bg-{{ 'success' if key.is_active == 1 else 'danger' }} bg-opacity-25 text-{{ 'success' if key.is_active == 1 else 'danger' }}">
                                        {{ 'Active' if key.is_active == 1 else 'Revoked' }}
                                    </span>
                                </td>
                                <td class="text-muted small">{{ key.created_at[:10] }}</td>
                                <td>
                                    <form method="POST" action="/admin/keys/toggle" style="display:inline;">
                                        <input type="hidden" name="key_id" value="{{ key.id }}">
                                        <button type="submit" class="btn btn-sm btn-outline-warning px-3">Toggle</button>
                                    </form>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <div class="card p-4 shadow-sm">
        <h6 class="mb-3 text-white fw-semibold">Network Audit Trail</h6>
        <div class="table-responsive">
            <table class="table table-borderless table-sm align-middle mb-0">
                <thead>
                    <tr>
                        <th>Timestamp</th>
                        <th>Identity</th>
                        <th>Target Route</th>
                        <th>Chains Executed</th>
                        <th>SQL Query Executed</th>
                        <th>Status</th>
                        <th>Latency</th>
                        <th>Result Note</th>
                    </tr>
                </thead>
                <tbody>
                    {% for log in logs %}
                    <tr>
                        <td class="text-muted small">{{ log.created_at }}</td>
                        <td class="text-white fw-medium">{{ log.client_name }}</td>
                        <td><code class="text-secondary bg-dark px-2 py-1 rounded">{{ log.endpoint }}</code></td>
                        <td class="text-muted text-truncate" style="max-width: 150px;" title="{{ log.chains_requested }}">
                            <span class="badge bg-info bg-opacity-25 text-info me-1">{{ log.chains_count }}</span>
                            {{ log.chains_requested }}
                        </td>
                        <td class="text-muted text-truncate" style="max-width: 250px;" title="{{ log.query_executed }}">
                            <code class="text-secondary">{{ log.query_executed }}</code>
                        </td>
                        <td>
                            <span class="badge bg-{{ 'success' if log.status_code == 200 else 'danger' }} bg-opacity-25 text-{{ 'success' if log.status_code == 200 else 'danger' }}">
                                {{ log.status_code }}
                            </span>
                        </td>
                        <td class="text-muted">{{ log.execution_time_seconds }}s</td>
                        <td class="text-muted text-truncate" style="max-width: 150px;" title="{{ log.status_message }}">{{ log.status_message }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</div>
<script>
function refreshChains() {
    const btn = document.getElementById('btnRefreshChains');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2" style="width:14px;height:14px;border-width:2px;"></span>Refreshing...';

    fetch('/admin/refresh-chains', { method: 'POST' })
        .then(r => r.json())
        .then(data => {
            if (data.status === 'success') {
                showToast('success', 'fa-circle-check', `Session refreshed \u2014 ${data.chains_loaded} chains loaded.`);
            } else {
                showToast('error', 'fa-circle-exclamation', data.message || 'Unknown error');
            }
        })
        .catch(() => showToast('error', 'fa-circle-exclamation', 'Network error calling refresh endpoint.'))
        .finally(() => {
            btn.disabled = false;
            btn.innerHTML = '<i class="fa-solid fa-rotate me-2 text-info"></i>Refresh CMA Chains';
        });
}

function showToast(type, icon, msg) {
    const toast = document.getElementById('refreshToast');
    const inner = document.getElementById('refreshToastInner');
    const iconEl = document.getElementById('refreshToastIcon');
    const msgEl = document.getElementById('refreshToastMsg');

    iconEl.className = `fa-solid ${icon}`;
    msgEl.textContent = msg;

    if (type === 'success') {
        inner.style.background = 'rgba(34,197,94,0.15)';
        inner.style.border = '1px solid rgba(34,197,94,0.35)';
        inner.style.color = '#86efac';
        iconEl.style.color = '#4ade80';
    } else {
        inner.style.background = 'rgba(239,68,68,0.15)';
        inner.style.border = '1px solid rgba(239,68,68,0.35)';
        inner.style.color = '#fca5a5';
        iconEl.style.color = '#f87171';
    }

    toast.style.display = 'block';
    toast.style.opacity = '1';
    clearTimeout(toast._hideTimer);
    toast._hideTimer = setTimeout(() => {
        toast.style.transition = 'opacity 0.4s';
        toast.style.opacity = '0';
        setTimeout(() => { toast.style.display = 'none'; toast.style.transition = ''; }, 400);
    }, 4000);
}
</script>
</body>
</html>
"""

# ==========================================
# 5. ROUTES & ENDPOINTS
# ==========================================

@app.route('/', methods=['GET'])
def client_page():
    return render_template_string(CLIENT_HTML, chains=list(GLOBAL_CHAINS.keys()), error=None)

@app.route('/client/run', methods=['POST'])
def client_run():
    start_time = time.time()
    api_key = request.form.get('api_key', '').strip()
    requested_chains = request.form.getlist('chains')
    raw_query = request.form.get('query', '')
    action = request.form.get('action', 'download')

    clean_query = re.sub(r'--.*', '', raw_query)
    queries_list = [q.strip() for q in clean_query.split(';') if q.strip()]

    db = get_db()
    client = db.execute("SELECT * FROM api_keys WHERE api_key = ? AND is_active = 1", (api_key,)).fetchone()
    
    if not client:
        log_audit_entry(401, "Invalid or Missing API Key from Web UI", "Unauthorized_Web", requested_chains, len(queries_list), time.time() - start_time, raw_query)
        return render_template_string(CLIENT_HTML, chains=list(GLOBAL_CHAINS.keys()), error="Invalid or Inactive API Key.")
    
    client_name = client['client_name']

    if not requested_chains or not queries_list:
        log_audit_entry(400, "Validation failed", client_name, requested_chains, len(queries_list), time.time() - start_time, raw_query)
        return render_template_string(CLIENT_HTML, chains=list(GLOBAL_CHAINS.keys()), error="Chain selection and valid SQL queries are required.")

    chain_results, error_msg = execute_cma_batch(requested_chains, queries_list)
    exec_duration = time.time() - start_time

    if error_msg:
        log_audit_entry(500, error_msg, client_name, requested_chains, len(queries_list), exec_duration, raw_query)
        return render_template_string(CLIENT_HTML, chains=list(GLOBAL_CHAINS.keys()), error=error_msg)

    log_audit_entry(200, f"Success ({action})", client_name, requested_chains, len(queries_list), exec_duration, raw_query)

    if action == 'display':
        html_results = {}
        for chain, q_dict in chain_results.items():
            html_results[chain] = {}
            for sheet, df in q_dict.items():
                count = len(df)
                df_display = df.head(500)
                html_results[chain][sheet] = {
                    "html": df_display.to_html(classes="table table-dark table-striped table-hover mb-0", index=False),
                    "total_rows": count,
                    "shown_rows": len(df_display)
                }
        return render_template_string(RESULTS_HTML, results=html_results)

    if len(chain_results) == 1:
        chain_name = list(chain_results.keys())[0]
        excel_buffer = create_excel(chain_results[chain_name])
        safe_chain = "".join(c for c in chain_name if c.isalnum() or c in (' ', '_', '-')).strip()
        return send_file(excel_buffer, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", as_attachment=True, download_name=f"{safe_chain}_TaskDetailReport.xlsx")
    else:
        zip_buffer = create_zip(chain_results)
        return send_file(zip_buffer, mimetype="application/zip", as_attachment=True, download_name="TaskDetailReports_Batch.zip")
        
######
@app.route('/api/internal/update_cookie', methods=['POST'])
def internal_update_cookie():
    """Receives new cookie directly from the Chrome Extension, updates .env, and refreshes."""
    logger.info(f"Received /api/internal/update_cookie request from IP: {request.remote_addr}")

    data = request.get_json(silent=True) or {}
    new_cookie = data.get("cookie")
    
    if not new_cookie:
        logger.warning("Rejected /api/internal/update_cookie: No cookie provided in JSON payload.")
        return jsonify({"status": "error", "message": "No cookie provided"}), 400
        
    try:
        clean_cookie = new_cookie.strip()

        # 1. Update in-memory cookie immediately under lock
        global ACTIVE_CMA_COOKIE
        with COOKIE_LOCK:
            ACTIVE_CMA_COOKIE = clean_cookie

        # 2. Update physical .env file for persistence across restarts
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        if os.path.exists(env_path):
            with open(env_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            with open(env_path, 'w', encoding='utf-8') as file:
                cookie_written = False
                for line in lines:
                    if line.startswith('CMA_COOKIE='):
                        file.write(f'CMA_COOKIE={clean_cookie}\n')
                        cookie_written = True
                    else:
                        file.write(line)
                if not cookie_written:
                    file.write(f'CMA_COOKIE={clean_cookie}\n')
        else:
            with open(env_path, 'w', encoding='utf-8') as file:
                file.write(f'CMA_COOKIE={clean_cookie}\n')
                
        # 3. Update environment variable in current process
        os.environ["CMA_COOKIE"] = clean_cookie
        
        # 4. Instantly refresh the chains
        global GLOBAL_CHAINS
        res = requests.get(f"{BASE_URL}/viewAdhoc", headers=get_headers(), timeout=15)
        if res.status_code == 200 and "login" not in res.url.lower():
            soup = BeautifulSoup(res.text, "html.parser")
            select = soup.find("select", {"id": "chainCode"})
            if select:
                GLOBAL_CHAINS = {opt.text.strip(): opt.get("value") for opt in select.find_all("option") if opt.get("value")}
                logger.info(f"✅ Extension updated cookie successfully. {len(GLOBAL_CHAINS)} chains loaded.")
                save_chains_cache()
                reset_circuit_breaker()
                
                # Signal waiting threads immediately!
                AUTH_RECOVERY_EVENT.set()
                return jsonify({"status": "success", "chains_loaded": len(GLOBAL_CHAINS)})
                
        # Even if chain dropdown wasn't found, set the event if authentication succeeded
        AUTH_RECOVERY_EVENT.set()
        return jsonify({"status": "error", "message": "Failed to load chains with new cookie."}), 502
        
    except Exception as e:
        logger.error(f"Failed to process extension update: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/internal/cma_status', methods=['POST'])
def internal_cma_status():
    """Receives maintenance or login-blocked signal directly from the Chrome Extension."""
    data = request.get_json(silent=True) or {}
    status = data.get("status", "")
    reason = data.get("reason", "CMA Login blocked on page")
    
    logger.warning(f"Received CMA status signal from Chrome: status='{status}', reason='{reason}'")
    if status in ("blocked", "maintenance"):
        trip_circuit_breaker(reason=reason)
        # Signal waiting execution threads to abort waiting immediately
        AUTH_RECOVERY_EVENT.set()
        return jsonify({"status": "acknowledged", "action": "circuit_breaker_tripped"}), 200
        
    return jsonify({"status": "acknowledged"}), 200
        

# --- Admin Portal ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('is_admin_logged_in'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        if request.form.get('username') == ADMIN_USER and request.form.get('password') == ADMIN_PASS:
            session['is_admin_logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        return render_template_string(ADMIN_LOGIN_HTML, error="Invalid credentials.")
    return render_template_string(ADMIN_LOGIN_HTML, error=None)

@app.route('/admin/logout')
def admin_logout():
    session.pop('is_admin_logged_in', None)
    return redirect(url_for('admin_login'))

@app.route('/admin')
@admin_required
def admin_dashboard():
    db = get_db()
    keys = db.execute("SELECT * FROM api_keys ORDER BY id DESC").fetchall()
    logs = db.execute("SELECT * FROM audit_logs ORDER BY id DESC LIMIT 100").fetchall()
    return render_template_string(ADMIN_DASHBOARD_HTML, keys=keys, logs=logs, new_key=session.pop('new_key_generated', None))

@app.route('/admin/keys/create', methods=['POST'])
@admin_required
def create_key():
    client_name = request.form.get('client_name', '').strip()
    if client_name:
        new_key = f"cma_{secrets.token_urlsafe(24)}"
        db = get_db()
        db.execute("INSERT INTO api_keys (api_key, client_name) VALUES (?, ?)", (new_key, client_name))
        db.commit()
        session['new_key_generated'] = new_key
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/keys/toggle', methods=['POST'])
@admin_required
def toggle_key():
    key_id = request.form.get('key_id')
    if key_id:
        db = get_db()
        db.execute("UPDATE api_keys SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE id = ?", (key_id,))
        db.commit()
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/refresh-chains', methods=['POST'])
@admin_required
def refresh_chains():
    """Force-reload GLOBAL_CHAINS from CMA using the latest CMA_COOKIE from .env.
    Useful after updating the session ID without restarting the server."""
    global GLOBAL_CHAINS, ACTIVE_CMA_COOKIE
    try:
        load_dotenv(override=True)
        with COOKIE_LOCK:
            ACTIVE_CMA_COOKIE = os.getenv("CMA_COOKIE", "")
        res = requests.get(f"{BASE_URL}/viewAdhoc", headers=get_headers(), timeout=15)
        if res.status_code == 200 and "login" not in res.url.lower():
            soup = BeautifulSoup(res.text, "html.parser")
            select = soup.find("select", {"id": "chainCode"})
            if select:
                new_chains = {opt.text.strip(): opt.get("value") for opt in select.find_all("option") if opt.get("value")}
                if new_chains:
                    GLOBAL_CHAINS = new_chains
                    return jsonify({"status": "success", "chains_loaded": len(GLOBAL_CHAINS)}), 200
            return jsonify({"status": "error", "message": "Could not find chain dropdown — session may be invalid."}), 502
        return jsonify({"status": "error", "message": f"CMA returned HTTP {res.status_code} or redirected to login."}), 502
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# --- Headless REST API ---
def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get("x-api-key")
        if not api_key:
            log_audit_entry(401, "Missing API Key", "Unauthorized_API")
            return jsonify({"error": "Unauthorized. Missing 'x-api-key' header."}), 401
        
        db = get_db()
        client = db.execute("SELECT * FROM api_keys WHERE api_key = ? AND is_active = 1", (api_key,)).fetchone()
        if not client:
            log_audit_entry(401, "Invalid or Inactive API Key", "Unauthorized_API")
            return jsonify({"error": "Unauthorized."}), 401
            
        g.client = dict(client)
        return f(*args, **kwargs)
    return decorated_function

@app.route('/api/v1/execute_batch', methods=['POST'])
@require_api_key
def api_execute_batch():
    start_time = time.time()
    data = request.get_json(silent=True) or {}
    requested_chains = data.get("chains", [])
    raw_query = data.get("query", "")
    queries_list = data.get("queries", [])
    response_format = data.get("format", "file").lower()

    if raw_query and not queries_list:
        clean_query = re.sub(r'--.*', '', raw_query)
        queries_list = [q.strip() for q in clean_query.split(';') if q.strip()]
    elif queries_list:
        queries_list = [re.sub(r'--.*', '', q).strip() for q in queries_list if re.sub(r'--.*', '', q).strip()]

    client_name = g.client.get('client_name', 'API_Client')

    if not requested_chains or not queries_list:
        log_audit_entry(400, "Validation failed", client_name, requested_chains, len(queries_list), time.time() - start_time, raw_query or str(queries_list))
        return jsonify({"error": "Both 'chains' and valid queries are required."}), 400

    chain_results, error_msg = execute_cma_batch(requested_chains, queries_list)
    exec_duration = time.time() - start_time

    if error_msg:
        status_code = 503 if ("maintenance" in error_msg.lower() or "blocked" in error_msg.lower()) else 500
        log_audit_entry(status_code, error_msg, client_name, requested_chains, len(queries_list), exec_duration, raw_query or str(queries_list))
        return jsonify({"error": error_msg, "code": status_code}), status_code

    log_audit_entry(200, f"Success ({response_format})", client_name, requested_chains, len(queries_list), exec_duration, raw_query or str(queries_list))

    if response_format == "json":
        json_data = {}
        for chain, q_dict in chain_results.items():
            json_data[chain] = {}
            for sheet, df in q_dict.items():
                clean_df = df.where(pd.notnull(df), None)
                json_data[chain][sheet] = clean_df.to_dict(orient='records')
        return jsonify({"status": "success", "execution_time_seconds": round(exec_duration, 3), "data": json_data})

    elif response_format == "xml":
        xml_parts = ['<?xml version="1.0" encoding="UTF-8" ?>\n<Results>']
        for chain, q_dict in chain_results.items():
            safe_chain = str(chain).replace("&", "&amp;").replace('"', '&quot;').replace("<", "&lt;").replace(">", "&gt;")
            xml_parts.append(f'  <Chain name="{safe_chain}">')
            for sheet, df in q_dict.items():
                xml_parts.append(f'    <Query name="{sheet}">')
                df_xml = df.to_xml(index=False, root_name="Data", row_name="Row")
                df_xml = re.sub(r'<\?xml.*\?>', '', df_xml).strip()
                xml_parts.append(df_xml)
                xml_parts.append('    </Query>')
            xml_parts.append('  </Chain>')
        xml_parts.append('</Results>')
        return Response("\n".join(xml_parts), mimetype='application/xml')

    else:
        if len(chain_results) == 1:
            chain_name = list(chain_results.keys())[0]
            excel_buffer = create_excel(chain_results[chain_name])
            safe_chain = "".join(c for c in chain_name if c.isalnum() or c in (' ', '_', '-')).strip()
            return send_file(excel_buffer, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", as_attachment=True, download_name=f"{safe_chain}_TaskDetailReport.xlsx")
        else:
            zip_buffer = create_zip(chain_results)
            return send_file(zip_buffer, mimetype="application/zip", as_attachment=True, download_name="TaskDetailReports_Batch.zip")

if __name__ == '__main__':
    print(f"[*] CMA Terminal Running on http://localhost:{PORT}")
    if not GLOBAL_CHAINS:
        logger.info("Startup chains empty. Triggering Chrome auto-login in 2s once port is listening...")
        threading.Thread(target=lambda: (time.sleep(2), trigger_sso_login(force=True)), daemon=True, name="startup_auto_login").start()
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)