#!/bin/bash
set -e

# Pre-check
if [ $EUID -ne 0 ]; then
    echo "Error: This script must be executed as the root user. Please use sudo or switch to root and run it."
    exit 1
fi

# Variable Definition
DEFAULT_SERVICE_FILE="/etc/systemd/system/atf.service"
PREFIX="/usr/local/atf"
DEFAULT_USER="omm"
SUDOERS_FILE="/etc/sudoers.d/atf"

# Execution Flow
echo -e "\n ATF One-click Uninstallation Process\n"

# Step 1: Stop ATF service (root only)
echo "[1/7] Stopping ATF service..."
if systemctl is-active --quiet atf; then
    systemctl stop atf
    echo "ATF service stopped"
else
    echo "ATF service is not running"
fi

# Step 2: Disable and delete service file
echo "[2/7] Removing ATF systemd service..."
systemctl disable atf >/dev/null 2>&1
rm -f "${DEFAULT_SERVICE_FILE}"
systemctl daemon-reload
echo "ATF service file deleted"

# Step 3: Remove sudoers configuration
echo "[3/7] Cleaning up sudo permissions..."
rm -f "${SUDOERS_FILE}"
echo "Sudo configuration for ${DEFAULT_USER} removed"

# Step 4: Perform basic uninstallation (make uninstall)
echo "[4/7] Uninstalling ATF core files..."
make uninstall PREFIX="${PREFIX}"

# Step 5: Clean up ATF residual processes
echo "[5/7] Cleaning up ATF residual processes..."
if pgrep -u "${DEFAULT_USER}" -f "atf" >/dev/null; then
    pkill -u "${DEFAULT_USER}" -f "atf"
    echo "Terminated ATF residual processes (user: ${DEFAULT_USER})"
else
    echo "No ATF residual processes"
fi

# Step 6: Clean up compilation artifacts in the source code directory
echo "[6/7] Cleaning up compilation artifacts (dist folder)..."
if [ -f "Makefile" ]; then
    make clean >/dev/null 2>&1
    echo "Compilation artifacts (dist folder) cleaned up"
else
    echo "Makefile not found, skip cleaning compilation artifacts"
fi

# Step 7: Clean up residual directories (retain config/logs by default)
echo "[7/7] Cleaning up residual directories..."
rm -rf "${PREFIX}/scripts" >/dev/null 2>&1
rm -rf "${PREFIX}/ssl" >/dev/null 2>&1
rm -f /usr/local/bin/atf /usr/local/bin/atf_start /usr/local/bin/atf_stop >/dev/null 2>&1

# Post-uninstallation Prompt
echo -e "\n ATF uninstallation completed!"
echo "Note: Configuration files (${PREFIX}/conf) and logs (/var/log/atf) are retained."
echo "To delete completely, run: rm -rf ${PREFIX} && rm -rf /var/log/atf && rm -rf /var/run/atf"
echo "========================================"