import os
import time
import json
import asyncio
import urllib.request
import requests
import logging
import websockets
try:
    from teams_notifier import send_teams_alert
except ImportError:
    def send_teams_alert(*args, **kwargs): pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [COOKIE_SYNC] %(levelname)s: %(message)s'
)
logger = logging.getLogger("cookie_sync")

CMA_URL = "https://g3-cma.ideas.com/cma/adhocSql/viewAdhoc"
UPDATE_ENDPOINT = "http://localhost:8555/api/internal/update_cookie"
CDP_LIST_URL = "http://localhost:9222/json/list"

async def get_cma_tab():
    try:
        req = urllib.request.Request(CDP_LIST_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
        for t in tabs:
            if "cma" in t.get("url", "") or "ideas.com" in t.get("url", ""):
                return t
        for t in tabs:
            if t.get("type") == "page":
                return t
    except Exception as e:
        logger.debug(f"CDP list error: {e}")
    return None

async def perform_cdp_auto_login():
    """Checks the Edge tab, clicks 'Login with SSO' if needed, and returns the fresh JSESSIONID."""
    tab = await get_cma_tab()
    if not tab:
        logger.warning("No Edge tab found via CDP on port 9222.")
        return None
        
    ws_url = tab.get("webSocketDebuggerUrl")
    if not ws_url:
        return None

    try:
        async with websockets.connect(ws_url, timeout=5) as ws:
            # 1. Check current URL
            await ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {"expression": "window.location.href"}}))
            res = json.loads(await ws.recv())
            curr_url = res.get("result", {}).get("result", {}).get("value", "")
            logger.info(f"Edge current URL: {curr_url}")
            
            # If on login page, click SSO button
            if "login" in curr_url.lower() or "auth" in curr_url.lower():
                logger.info("Detected login page! Triggering SSO button click...")
                await ws.send(json.dumps({
                    "id": 2,
                    "method": "Runtime.evaluate",
                    "params": {"expression": "if (document.getElementById('ssoButton')) { document.getElementById('ssoButton').click(); 'clicked'; } else { 'not_found'; }"}
                }))
                click_res = json.loads(await ws.recv())
                logger.info(f"SSO click result: {click_res.get('result', {}).get('result', {}).get('value')}")
                
                # Wait for Cognito & CMA redirect
                await asyncio.sleep(4)
            elif "adhocsql" not in curr_url.lower():
                # Navigate to adhocSql to establish/verify session
                logger.info("Navigating Edge tab to viewAdhoc...")
                await ws.send(json.dumps({
                    "id": 3,
                    "method": "Page.navigate",
                    "params": {"url": CMA_URL}
                }))
                await ws.recv()
                await asyncio.sleep(3)
                
            # 2. Extract cookies via Storage.getCookies
            await ws.send(json.dumps({"id": 4, "method": "Network.enable"}))
            await ws.recv()
            await ws.send(json.dumps({"id": 5, "method": "Storage.getCookies"}))
            cookie_res = json.loads(await ws.recv())
            cookies = cookie_res.get("result", {}).get("cookies", [])
            
            for c in cookies:
                if c.get("name") == "JSESSIONID" and "cma" in c.get("domain", ""):
                    sess_id = c.get("value")
                    logger.info(f"Extracted JSESSIONID via CDP: {sess_id[:15]}...")
                    return sess_id
    except Exception as e:
        logger.error(f"Error during CDP auto-login: {e}")
        
    return None

def is_cookie_valid(sess_id):
    if not sess_id:
        return False
    try:
        resp = requests.get(
            CMA_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Cookie": f"JSESSIONID={sess_id}"
            },
            allow_redirects=False,
            timeout=10
        )
        return resp.status_code == 200
    except Exception as e:
        logger.warning(f"Error validating cookie: {e}")
        return False

def push_cookie_to_cma(sess_id):
    try:
        cookie_header = f"JSESSIONID={sess_id}"
        resp = requests.post(UPDATE_ENDPOINT, json={"cookie": cookie_header}, timeout=5)
        if resp.status_code == 200:
            logger.info("Successfully pushed updated JSESSIONID to CMA Middleware!")
            return True
        else:
            logger.error(f"Failed to push cookie to CMA Middleware: HTTP {resp.status_code}")
    except Exception as e:
        logger.error(f"Exception pushing cookie: {e}")
    return False

def run_sync_loop():
    logger.info("Starting Autonomous Cookie Auto-Login & Sync Daemon...")
    last_pushed_cookie = None
    
    while True:
        try:
            needs_refresh = False
            try:
                status_res = requests.get("http://localhost:8555/api/internal/needs_cookie", timeout=2)
                if status_res.status_code == 200:
                    data = status_res.json()
                    needs_refresh = data.get("needs_cookie", False)
            except Exception:
                pass
                
            if last_pushed_cookie:
                if not is_cookie_valid(last_pushed_cookie):
                    logger.warning("Currently cached JSESSIONID has EXPIRED! Initiating auto-login...")
                    needs_refresh = True
            else:
                needs_refresh = True
                
            if needs_refresh:
                logger.info("Triggering autonomous CDP auto-login cycle...")
                send_teams_alert(
                    service_name="CMA Middleware",
                    title="Session Expired - Re-authenticating",
                    message="CMA Undertow session expired. Initiating autonomous in-container SSO re-login.",
                    severity="WARNING",
                    details={"Engine": "Edge CDP", "Port": 8555}
                )
                sess_id = asyncio.run(perform_cdp_auto_login())
                if sess_id and is_cookie_valid(sess_id):
                    logger.info("Auto-login SUCCESS! New valid JSESSIONID obtained.")
                    if push_cookie_to_cma(sess_id):
                        last_pushed_cookie = sess_id
                        send_teams_alert(
                            service_name="CMA Middleware",
                            title="Session Restored Successfully",
                            message="Autonomous SSO re-login completed. New JSESSIONID injected into CMA Gateway.",
                            severity="SUCCESS",
                            details={"Session Prefix": f"{sess_id[:15]}...", "Status": "Active"}
                        )
                else:
                    logger.warning("Auto-login cycle did not yield a valid session yet. Retrying next interval...")
        except Exception as e:
            logger.error(f"Error in sync loop: {e}")
            
        time.sleep(15)

if __name__ == "__main__":
    run_sync_loop()
