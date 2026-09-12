# CMA Middleware & Terminal API Gateway

High-performance API Gateway and Terminal proxy for interacting with the Client Management Application (CMA). It handles automated session management, cookie propagation, batch operations, and real-time query execution on port `8555`.

---

## 🌟 Key Features

- **Port 8555 Microservice**: Dedicated REST API gateway for internal team scripts and automations.
- **Automated Session Maintenance**: Validates and injects active `JSESSIONID` cookies into upstream requests.
- **Local Transaction Logging**: Persists audit logs and cached responses in `api_gateway.db`.
- **Flexible Execution Modes**: Run natively via Python, headless via VBScript background launcher, or isolated inside Docker.

---

## 🚀 Quick Start

### 1. Configure Environment
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Fill in your active `CMA_COOKIE` (JSESSIONID) and secret keys.

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Service

#### Native Windows (Console Window)
Double-click `start_cma.bat` or run in terminal:
```cmd
start_cma.bat
```

#### Headless / Background (No Window)
Double-click `start_cma_silent.vbs` or execute:
```cmd
wscript start_cma_silent.vbs
```

#### Docker
```bash
docker compose up -d --build
```

---

## 📡 Core API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Web UI Dashboard / Terminal view |
| `GET` | `/api/status` | Health check & active session status |
| `POST` | `/api/query` | Execute direct CMA queries |
| `POST` | `/api/batch` | Batch process multiple CMA operations |
| `POST` | `/api/cookie/update` | Update active CMA session cookie dynamically |

---

## 🐍 Python Usage Example

```python
import requests

CMA_BASE_URL = "http://localhost:8555"

def check_cma_health():
    resp = requests.get(f"{CMA_BASE_URL}/api/status", timeout=5)
    resp.raise_for_status()
    print("CMA Status:", resp.json())

if __name__ == "__main__":
    check_cma_health()
```

---

## 🛠️ Windows Startup Automation

To have CMA run automatically in the background on system startup:
Create a shortcut of `start_cma_silent.vbs` in:
`%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\CMA_Terminal.lnk`
