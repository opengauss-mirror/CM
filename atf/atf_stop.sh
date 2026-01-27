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

# Execution Logic: Only stop ATF service (sudo免密)
echo "Stopping ATF service (user: ${CURRENT_USER})..."
sudo systemctl stop "${SERVICE_NAME}"

# Verify stop status
if ! systemctl is-active --quiet "${SERVICE_NAME}"; then
    echo -e "\n ATF service stopped successfully!"
else
    echo -e "\n ATF service failed to stop!"
    echo "Please check logs: journalctl -u ${SERVICE_NAME} -e"
    exit 1
fi