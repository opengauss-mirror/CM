#!/bin/bash
set -e

# Pre-check: NOT root
if [ $EUID -eq 0 ]; then
    echo "Error: This script should NOT be executed as root. Please run as omm user."
    exit 1
fi

# Variable Definition
SERVICE_NAME="atf"
DEFAULT_USER="omm"
CURRENT_USER=$(whoami)

# Pre-check: Current user is authorized (omm)
if [ "${CURRENT_USER}" != "${DEFAULT_USER}" ]; then
    echo "Error: This script can only be run by ${DEFAULT_USER} (current: ${CURRENT_USER})"
    exit 1
fi

# Reload the systemd configuration, clear the cache
echo "Reloading systemd configuration..."
sudo systemctl daemon-reload

# Execution Logic: Only start ATF service
echo "Starting ATF service (user: ${CURRENT_USER})..."
sudo systemctl restart "${SERVICE_NAME}"
sleep 2

# Verify start status
if systemctl is-active --quiet "${SERVICE_NAME}"; then
    echo -e "\n ATF service started successfully!"
    echo "Service status: $(systemctl status "${SERVICE_NAME}" --no-pager | grep 'Active:' | awk '{print $2, $3}')"
    echo "Running binary path: $(which atf)"
    echo "Binary modification time: $(stat -c %y /usr/local/atf/bin/atf | cut -d' ' -f1-2)"
else
    echo -e "\n ATF service failed to start!"
    echo "Please check logs: journalctl -u ${SERVICE_NAME} -e"
    exit 1
fi