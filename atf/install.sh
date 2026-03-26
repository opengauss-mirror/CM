#!/bin/bash
set -e

# Pre-check
if [ $EUID -ne 0 ]; then
    echo "Error: This script must be executed as the root user. Please use sudo or switch to root and run it."
    exit 1
fi

# Variable Definition (Core: Add PREFIX definition to keep consistent with Makefile)
VERSION="1.0.0"

DEFAULT_USER=""
DEFAULT_GROUP=""

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -u|--user)
            DEFAULT_USER="$2"
            shift # past argument
            ;;
        -g|--group)
            DEFAULT_GROUP="$2"
            shift # past argument
            ;;
        *) # unknown option
            echo "Unknown parameter: $1"
            echo "Usage: $0 -u <user> -g <group>"
            exit 1
            ;;
    esac
    shift # past value
done

if [ -z "${DEFAULT_USER}" ] || [ -z "${DEFAULT_GROUP}" ]; then
    echo "Error: Both user and group must be specified."
    echo "Usage: $0 -u <user> -g <group>"
    exit 1
fi

echo "Running installation for user: ${DEFAULT_USER}, group: ${DEFAULT_GROUP}"

PREFIX="/usr/local/atf"
DEFAULT_WORK_DIR="${PREFIX}"
DEFAULT_SERVICE_FILE="/etc/systemd/system/atf.service"
SUDOERS_FILE="/etc/sudoers.d/atf"
SSL_LOCAL_DIR="./ssl"
SSL_KEY="${SSL_LOCAL_DIR}/server.key"
SSL_CSR="${SSL_LOCAL_DIR}/server.csr"
SSL_PEM="${SSL_LOCAL_DIR}/server.pem"
CM_CTL_PATH="/opt/huawei/install/app/bin:/usr/local/atf/bin:/usr/bin:/bin"
CM_CTL_LD_PATH="/opt/software/openGauss/libcgroup/lib:/opt/huawei/install/app_d8b33b17/lib"
GAUSSHOME="/opt/huawei/install/app"
GAUSSLOG="/opt/huawei/gaussdb/log"
GS_CLUSTER_NAME="dbCluster"
PGPORT="26000"
PGDATABASE="postgres"
GAUSS_ENV="2"
GAUSS_VERSION="7.0.0-RC3"
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
su - "${DEFAULT_USER}" -c "cd ${CODE_DIR} && export LD_LIBRARY_PATH=/lib64:/usr/lib64:\$LD_LIBRARY_PATH && make all -j$(nproc)"
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
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" ./ssl
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
mkdir -p /var/log/atf
mkdir -p /var/run/atf
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" "${DEFAULT_WORK_DIR}"
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" "${PREFIX}"
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" /var/log/atf
chown -R "${DEFAULT_USER}:${DEFAULT_GROUP}" /var/run/atf
chmod 755 "${DEFAULT_WORK_DIR}"

# Step 9: Create systemd service file (register service, root only)
echo "[9/11] Registering ATF systemd service..."

# Use su to run a non-interactive login shell for the user, which sources .bashrc/.profile
# Then print the variables we need.
# We use a unique separator to handle multi-line or complex variable values.
SEPARATOR="--ENV_VAR_SEPARATOR--"
ENV_VARS=$(su - "${DEFAULT_USER}" -c "
    source ~/.bashrc >/dev/null 2>&1 || true;
    env | grep -E '^(HOME|USER|LANG|PATH|LD_LIBRARY_PATH|GAUSSHOME|GAUSSLOG)=' | while IFS= read -r line; do
        echo \"\${line}${SEPARATOR}\"
    done
")

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

$(echo "${ENV_VARS}" | while IFS= read -r line; do
    if [[ -n "$line" ]]; then
        formatted_line=$(echo "$line" | sed "s/${SEPARATOR}$//")
        
        if [[ "${formatted_line}" == LD_LIBRARY_PATH=* ]]; then
            original_val="${formatted_line#LD_LIBRARY_PATH=}"
            echo "Environment=\"LD_LIBRARY_PATH=/lib64:/usr/lib64:${original_val}\""
        else
            echo "Environment=\"${formatted_line}\""
        fi
    fi
done)

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
echo "  Check status: systemctl status atf (as ${DEFAULT_USER}, with sudo)"
echo "========================================"