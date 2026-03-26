#!/bin/bash
set -e

# Pre-check: NOT root
if [ $EUID -eq 0 ]; then
    echo "Error: This script should NOT be executed as root. Please run as the designated service user."
    exit 1
fi

# --- Dynamic Variable Definition ---
SERVICE_NAME="atf"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
CURRENT_USER=$(whoami)

# Pre-check: Ensure the service file exists
if [ ! -f "${SERVICE_FILE}" ]; then
    echo "Error: Service file ${SERVICE_FILE} not found. Has the ATF service been installed?"
    exit 1
fi

# --- Dynamically get the authorized user from the service file ---
AUTHORIZED_USER=$(grep -Po '^User=\K.*' "${SERVICE_FILE}" | tr -d '[:space:]')

if [ -z "${AUTHORIZED_USER}" ]; then
    echo "Error: Could not determine the authorized user from ${SERVICE_FILE}."
    exit 1
fi

# Pre-check: Current user is the one specified in the service file
if [ "${CURRENT_USER}" != "${AUTHORIZED_USER}" ]; then
    echo "Error: This script can only be run by the authorized user '${AUTHORIZED_USER}' (current user is '${CURRENT_USER}')"
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
    INSTALLED_PATH=$(which atf || echo "not found")
    echo "Running binary path: ${INSTALLED_PATH}"
    if [ -f "${INSTALLED_PATH}" ]; then
        echo "Binary modification time: $(stat -c %y "${INSTALLED_PATH}" | cut -d'.' -f1)"
    fi
else
    echo -e "\n ATF service failed to start!"
    echo "Please check logs: journalctl -u ${SERVICE_NAME} -e"
    exit 1
fi