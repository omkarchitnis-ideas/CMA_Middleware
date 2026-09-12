import os
import time
import shutil
import sqlite3
import requests
import subprocess
import logging
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [COOKIE_SYNC] %(levelname)s: %(message)s'
)
logger = logging.getLogger("cookie_sync")

COOKIES_DB = "/app/edge_profile/Default/Cookies"
CMA_URL = "https://g3-cma.ideas.com/cma/adhocSql/viewAdhoc"
UPDATE_ENDPOINT = "http://localhost:8555/api/internal/update_cookie"

def get_chromium_key():
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA1(),
        length=16,
        salt=b'saltysalt',
        iterations=1,
        backend=default_backend()
    )
    return kdf.derive(b'peanuts')

AES_KEY = get_chromium_key()
IV = b' ' * 16

def extract_cma_cookie():
    if not os.path.exists(COOKIES_DB):
        return None
        
    tmp_db = "/tmp/cookies_reader.sqlite"
    try:
        shutil.copyfile(COOKIES_DB, tmp_db)
        conn = sqlite3.connect(tmp_db)
        cur = conn.cursor()
        cur.execute("SELECT encrypted_value FROM cookies WHERE host_key LIKE '%cma%' AND name='JSESSIONID'")
        row = cur.fetchone()
        conn.close()
        
        if not row or not row[0]:
            return None
            
        enc = row[0]
        if not (enc.startswith(b'v10') or enc.startswith(b'v11')):
            return None
            
        ciphertext = enc[3:]
        cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(IV), backend=default_backend())
        dec = cipher.decryptor().update(ciphertext)
        
        # In Linux Chromium, plaintext starts at byte offset 32
        candidate = dec[32:].split(b'\x00')[0]
        pad = candidate[-1]
        if isinstance(pad, int) and pad < 16:
            candidate = candidate[:-pad]
            
        return candidate.decode('latin1', errors='ignore')
    except Exception as e:
        logger.error(f"Failed to read/decrypt cookie: {e}")
        return None

def is_cookie_valid(sess_id):
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
        logger.warning(f"Error checking cookie validity: {e}")
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

def trigger_edge_navigation():
    logger.info("Triggering Edge navigation to refresh session...")
    try:
        subprocess.Popen(
            ["microsoft-edge-stable", "--no-sandbox", CMA_URL],
            env=dict(os.environ, DISPLAY=":99")
        )
    except Exception as e:
        logger.error(f"Failed to trigger Edge navigation: {e}")

def run_sync_loop():
    last_known_cookie = None
    logger.info("Starting background Cookie Sync daemon...")
    
    while True:
        try:
            sess_id = extract_cma_cookie()
            if sess_id:
                if sess_id != last_known_cookie:
                    logger.info(f"Detected new JSESSIONID candidate ({sess_id[:15]}...). Checking validity...")
                    if is_cookie_valid(sess_id):
                        logger.info("Candidate JSESSIONID is valid! Updating CMA Middleware...")
                        if push_cookie_to_cma(sess_id):
                            last_known_cookie = sess_id
                    else:
                        logger.warning("Candidate JSESSIONID returned redirect / unauthenticated.")
                else:
                    # Periodic validity check every 5 minutes
                    pass
        except Exception as e:
            logger.error(f"Error in sync loop: {e}")
            
        time.sleep(15)

if __name__ == "__main__":
    run_sync_loop()
