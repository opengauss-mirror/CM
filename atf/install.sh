#!/bin/bash
set -e

# Pre-check
if [ $EUID -ne 0 ]; then
    echo "Error: This script must be executed as the root user. Please use sudo or switch to root and run it."
    exit 1
fi

# Variable Definition (Core: Add PREFIX definition to keep consistent with Makefile)
VERSION="1.0.0"
DEFAULT_USER="omm"
DEFAULT_GROUP="dbgrp"
PREFIX="/usr/local/atf"
DEFAULT_WORK_DIR="${PREFIX}"
DEFAULT_SERVICE_FILE="/etc/systemd/system/atf.service"
SUDOERS_FILE="/etc/sudoers.d/atf"
SSL_LOCAL_DIR="./ssl"
SSL_KEY="${SSL_LOCAL_DIR}/server.key"
SSL_CSR="${SSL_LOCAL_DIR}/server.csr"
SSL_PEM="${SSL_LOCAL_DIR}/server.pem"
CM_CTL_PATH="/opt/huawei2/install/app/bin:/home/${DEFAULT_USER}/gauss_om/script:/opt/huawei2/install/om/script/gspylib/pssh/bin:/opt/huawei2/install/om/script:/usr/local/apache-maven-3.6.3/bin:/usr/local/bin:/usr/bin:/usr/local/maven/bin"
CM_CTL_LD_PATH="/opt/huawei2/install/app/lib:/opt/huawei2/install/om/lib:/opt/huawei2/install/om/script/gspylib/clib:/script/gspylib/clib:/usr/local/softbus/ctrlbus/lib:"
GAUSSHOME="/opt/huawei2/install/app"
GS_CLUSTER_NAME="dbCluster"
PGPORT="26000"
PGDATABASE="postgres"
PGHOST="/opt/huawei2/tmp"
PGDATA="/opt/huawei2/install/data/db1"
GAUSSLOG="/var/log2/omm/omm"
GAUSS_ENV="2"
GAUSS_VERSION="7.0.0-RC2"
CODE_DIR="/home/omm/atf"
cd "${CODE_DIR}" || { echo "Error: Code directory ${CODE_DIR} not found!"; exit 1; }

# Execution Flow
echo -e "\n ATF ${VERSION} One-click Installation & Service Registration\n"

# Step 1: Force clean all compilation artifacts
# (including old binaries and dependency files)
echo "[1/11] Cleaning up old compilation artifacts..."
# Force delete the dist directory (to avoid incomplete make clean)
rm -rf "${CODE_DIR}/dist"
make distclean >/dev/null 2>&1
make clean >/dev/null 2>&1

# Step 2: Switch to the omm user to compile
echo "[2/11] Compiling the ATF server (as omm user)..."
su - "${DEFAULT_USER}" -c "cd ${CODE_DIR} && make all -j$(nproc)"
# Verify whether compilation artifacts exist
if [ ! -f "${CODE_DIR}/dist/atf" ]; then
    echo "Error: Compilation failed! dist/atf not found."
    exit 1
fi

# Step 3: Generate an SSL certificate
echo "[3/11] Generating SSL certificates..."
mkdir -p "./ssl"
openssl genrsa -out "./ssl/server.key" 2048 >/dev/null 2>&1
openssl req -new -key "./ssl/server.key" -out "./ssl/server.csr" \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=ATF/OU=ATFServer/CN=atf.local" >/dev/null 2>&1
openssl x509 -req -days 3650 -in "./ssl/server.csr" -signkey "./ssl/server.key" -out "./ssl/server.pem" >/dev/null 2>&1
chown -R omm:dbgrp "./ssl"
chmod 600 "./ssl/server.key"
chmod 644 "./ssl/server.csr" "./ssl/server.pem"

# Step 4: Install core files（force overwrite）
echo "[4/11] Installing ATF core files (force overwrite)..."
rm -rf "${PREFIX}" >/dev/null 2>&1 || true
make install PREFIX="${PREFIX}"
if [ ! -f "${PREFIX}/bin/atf" ]; then
    echo "Error: Installation failed! ${PREFIX}/bin/atf not found."
    exit 1
fi

# Step 5: Checking config file existence
echo "[5/11] Checking config file existence..."
if [ ! -f "./conf/atf_config.conf" ]; then
    echo "Error: Config file ./conf/atf_config.conf not found!"
    echo "Please put atf_config.conf in ./conf/ directory of the source code."
    exit 1
fi

# Step 6: Synchronize configuration file paths
echo "[6/11] Adapting configuration file paths..."
sed -i "s|DIST_DIR = .*|DIST_DIR = /var/log/atf|g" "${PREFIX}/conf/atf_config.conf"
sed -i "s|PID_FILE = .*|PID_FILE = /var/run/atf/atf.pid|g" "${PREFIX}/conf/atf_config.conf"
sed -i "s|SSL_CERT_FILE = .*|SSL_CERT_FILE = ${PREFIX}/ssl/server.pem|g" "${PREFIX}/conf/atf_config.conf"
sed -i "s|SSL_KEY_FILE = .*|SSL_KEY_FILE = ${PREFIX}/ssl/server.key|g" "${PREFIX}/conf/atf_config.conf"

# Step 7: Create user/group (core logic from atf_start.sh)
echo "[7/11] Checking/Creating ATF runtime user/group..."
# Check group
if ! getent group "${DEFAULT_GROUP}" >/dev/null 2>&1; then
    echo "Group ${DEFAULT_GROUP} not found, creating..."
    groupadd "${DEFAULT_GROUP}"
fi
# Check user
if ! id -u "${DEFAULT_USER}" >/dev/null 2>&1; then
    echo "User ${DEFAULT_USER} not found, creating..."
    useradd -g "${DEFAULT_GROUP}" -m -d "/home/${DEFAULT_USER}" "${DEFAULT_USER}"
fi

# Step 8: Create working directory and set permissions
echo "[8/11] Configuring working directory permissions..."
mkdir -p "${DEFAULT_WORK_DIR}"
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" "${DEFAULT_WORK_DIR}"
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" "${PREFIX}"
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" /var/log/atf
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" /var/run/atf
chmod 755 "${DEFAULT_WORK_DIR}"

# Step 9: Create systemd service file (register service, root only)
echo "[9/11] Registering ATF systemd service..."
ATF_EXEC="${PREFIX}/bin/atf"
cat > "${DEFAULT_SERVICE_FILE}" << EOF
[Unit]
Description=ATF Server (Primary node IP query service)
After=network.target
Wants=network.target

[Service]
User=${DEFAULT_USER}
Group=${DEFAULT_GROUP}
WorkingDirectory=${DEFAULT_WORK_DIR}
ExecStart=${ATF_EXEC}

Environment="HOME=/home/${DEFAULT_USER}"
Environment="USER=${DEFAULT_USER}"
Environment="LANG=en_US.UTF-8"

Environment="PATH=${CM_CTL_PATH}"
Environment="LD_LIBRARY_PATH=${CM_CTL_LD_PATH}"
Environment="GAUSSHOME=${GAUSSHOME}"
Environment="GS_CLUSTER_NAME=${GS_CLUSTER_NAME}"
Environment="PGPORT=${PGPORT}"
Environment="PGDATABASE=${PGDATABASE}"
Environment="PGHOST=${PGHOST}"
Environment="PGDATA=${PGDATA}"
Environment="GAUSSLOG=${GAUSSLOG}"
Environment="GAUSS_ENV=${GAUSS_ENV}"
Environment="GAUSS_VERSION=${GAUSS_VERSION}"

Restart=on-failure
RestartSec=3
StartLimitInterval=60
StartLimitBurst=5
StandardOutput=journal
StandardError=journal
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
EOF

# Step 10: Configure sudoers (allow ${DEFAULT_USER} to start/stop atf without password)
echo "[10/11] Configuring sudo permissions for ${DEFAULT_USER}..."
cat > "${SUDOERS_FILE}" << EOF
${DEFAULT_USER} ALL=(ALL) NOPASSWD: /usr/bin/systemctl start atf, /usr/bin/systemctl stop atf, /usr/bin/systemctl status atf, /usr/bin/systemctl daemon-reload, /usr/bin/systemctl restart atf
EOF
chmod 0440 "${SUDOERS_FILE}"

# Step 11: Reload systemd + restart
echo "[11/11] Reloading systemd configuration and restarting service..."
systemctl daemon-reload
# First stop the old service (if running), then enable/restart it
systemctl stop atf >/dev/null 2>&1 || true
systemctl enable atf
echo "ATF service registered successfully (enabled on boot)"

# Post-installation Prompt
echo -e "\n========================================"
echo "ATF ${VERSION} installed and service registered successfully!"
echo "Install path: ${PREFIX}"
echo "Service file: ${DEFAULT_SERVICE_FILE}"
echo "Runtime user: ${DEFAULT_USER}"
echo "SSL certificates: $(pwd)/${SSL_LOCAL_DIR}/ (copied to ${PREFIX}/ssl/)"
echo "========================================"
echo "Usage (run as ${DEFAULT_USER}):"
echo "  Start ATF: atf_start"
echo "  Stop ATF: atf_stop"
echo "  Check status: systemctl status atf (as omm, with sudo)"
echo "========================================"