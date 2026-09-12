#!/bin/bash
set -e

echo "========================================================"
echo " Starting CMA Middleware + Edge SSO Virtual Desktop"
echo "========================================================"

# 1. Start Xvfb Virtual Framebuffer on display :99
Xvfb :99 -screen 0 1280x800x24 &
export DISPLAY=:99
sleep 1

# 2. Start lightweight window manager
fluxbox &
sleep 1

# 3. Start VNC server (no password for local container)
x11vnc -display :99 -forever -nopw -shared -rfbport 5900 &
sleep 1

# 4. Start noVNC Web bridge on port 6080
if [ -d "/usr/share/novnc" ]; then
    websockify --web /usr/share/novnc 6080 localhost:5900 &
elif [ -d "/usr/share/novnc/utils" ]; then
    websockify --web /usr/share/novnc 6080 localhost:5900 &
fi

# 5. Launch Microsoft Edge in the virtual desktop with persistent profile
echo "Launching Microsoft Edge for SSO verification..."
microsoft-edge-stable \
    --no-sandbox \
    --disable-dev-shm-usage \
    --user-data-dir=/app/edge_profile \
    "https://g3-cma.ideas.com/cma/adhocSql/viewAdhoc" &

# 6. Start Cookie Auto-Sync Background Daemon
echo "Starting Cookie Sync Daemon..."
python cookie_sync.py &

# 7. Start CMA API Gateway
echo "Starting CMA Gateway on Port 8555..."
python cma.py
