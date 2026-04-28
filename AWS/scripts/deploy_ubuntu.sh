#!/bin/bash

# ==============================================================================
# AWS Project Deployment Script for Vultr (Ubuntu 22.04 x64)
# HTTPS Repo | CUDA Runtime Validation | Llama-CPP Wheels | Zsh Memory
# ==============================================================================

set -e

# --- Configuration ---
REPO_URL="https://github.com/YoTro/Python_repository.git"
PROJECT_NAME="Python_repository"
APP_SUBDIR="AWS"
PYTHON_VERSION="3.11"
STATUS_FILE=".deploy_status"

# Qwen2.5-3B-Instruct-GGUF (Q4_K_M)
MODEL_URL="https://huggingface.co/Qwen/Qwen2.5-3B-Instruct-GGUF/resolve/main/qwen2.5-3b-instruct-q4_k_m.gguf"
MODEL_NAME="qwen2.5-3b-instruct-q4_k_m.gguf"

# --- Helper Functions ---
function check_step() {
    if [ -f "$STATUS_FILE" ] && grep -q "^$1$" "$STATUS_FILE"; then
        echo "⏭️  Step '$1' already completed, skipping..."
        return 0
    else
        return 1
    fi
}

function mark_step() {
    echo "$1" >> "$STATUS_FILE"
    echo "✅ Step '$1' finished."
}

echo "----------------------------------------------------------------"
echo "🚀 Starting High-Performance Deployment"
echo "----------------------------------------------------------------"

# 1. Base System & Python 3.11
if ! check_step "base_install"; then
    echo "📦 Updating system and installing base dependencies..."
    sudo apt update && sudo apt upgrade -y
    sudo apt install -y git redis-server software-properties-common curl build-essential \
        zsh chromium-browser chromium-chromedriver xvfb qrencode
    sudo add-apt-repository ppa:deadsnakes/ppa -y
    sudo apt update
    sudo apt install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-venv python${PYTHON_VERSION}-dev
    mark_step "base_install"
fi

# 2. Configure Zsh Memory (Auto-suggestions)
if ! check_step "zsh_config"; then
    echo "🐚 Configuring Zsh with Command Memory..."
    sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended || true
    git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions || true
    git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting || true
    sed -i 's/plugins=(git)/plugins=(git zsh-autosuggestions zsh-syntax-highlighting)/' ~/.zshrc
    sudo chsh -s $(which zsh) $USER
    mark_step "zsh_config"
fi

# Always ensure Chromium is on PATH (idempotent)
grep -qxF 'export PATH=$PATH:/usr/lib/chromium-browser/' ~/.zshrc \
    || echo 'export PATH=$PATH:/usr/lib/chromium-browser/' >> ~/.zshrc
export PATH=$PATH:/usr/lib/chromium-browser/

# Server identity env vars (used by cookie_helper.py for SSH tunnel instructions)
export SERVER_USER="${SERVER_USER:-$USER}"
SERVER_IP_DETECTED=$(curl -sf https://api.ipify.org || curl -sf https://ifconfig.me || echo "<server-ip>")
export SERVER_IP="${SERVER_IP:-$SERVER_IP_DETECTED}"
grep -qxF "export SERVER_USER=$SERVER_USER" ~/.zshrc \
    || echo "export SERVER_USER=$SERVER_USER" >> ~/.zshrc
grep -qF "export SERVER_IP=" ~/.zshrc \
    || echo "export SERVER_IP=$SERVER_IP" >> ~/.zshrc
echo "🌐 Server identity: $SERVER_USER@$SERVER_IP"

# 3. Clone / Update Repository
if [ ! -d "$PROJECT_NAME" ]; then
    echo "📥 Cloning repository via HTTPS..."
    git clone "$REPO_URL"
    cd "$PROJECT_NAME/$APP_SUBDIR"
else
    echo "🔄 Repository exists, pulling latest changes..."
    cd "$PROJECT_NAME/$APP_SUBDIR"
    git pull
fi

# 4. Directory Structure
if ! check_step "dirs_created"; then
    mkdir -p data/cache data/checkpoints data/intelligence data/sessions data/reports
    mkdir -p models/llm config/auth logs
    mark_step "dirs_created"
fi

# 5. Download Model
if [ ! -f "models/llm/$MODEL_NAME" ]; then
    echo "🤖 Downloading Qwen 3B Model..."
    wget -O "models/llm/$MODEL_NAME" "$MODEL_URL"
fi

# 6. Virtual Environment (venv311)
if [ ! -d ".venv311" ]; then
    echo "🛠️ Creating venv311 (Python 3.11)..."
    python${PYTHON_VERSION} -m venv .venv311
fi
source .venv311/bin/activate

# 7. CUDA Detection — validate runtime, not just driver
# -------------------------------------------------------
# nvidia-smi only proves the kernel driver is loaded.
# llama-cpp-python (and any training workload) needs libcudart.so at
# runtime; if it is absent the process crashes with:
#   "cannot open shared object file: libcudart.so.12"
#
# Detection order:
#   a) nvidia-smi present  → GPU exists, CUDA runtime is REQUIRED
#   b) libcudart.so in ldconfig cache → already installed, nothing to do
#   c) libcudart.so found in a non-standard path → add to LD_LIBRARY_PATH
#   d) not found anywhere → install nvidia-cuda-toolkit (mandatory for GPU)
#   e) no nvidia-smi → no GPU, CPU-only build
echo "🔍 Detecting CUDA for llama-cpp-python..."
CUDA_VERSION=""
CUDA_RUNTIME_OK=false

if command -v nvidia-smi &> /dev/null; then
    # Prefer nvcc (actual toolkit version) over nvidia-smi (driver max version).
    # nvcc gives e.g. "release 11.5" → "115"; nvidia-smi gives major only → "12" → "121" (wrong minor).
    if command -v nvcc &> /dev/null; then
        CUDA_VERSION=$(nvcc --version | grep "release" | sed 's/.*release \([0-9]*\)\.\([0-9]*\).*/\1\2/')
    else
        # Fall back to major version from nvidia-smi; assume minor=1 (common on fresh installs)
        CUDA_VERSION=$(nvidia-smi | grep "CUDA Version" | awk '{print $9}' | awk -F. '{printf "%s%s", $1, $2}')
    fi
    echo "⚡ CUDA toolkit detected (wheel suffix: cu$CUDA_VERSION) — runtime is required"

    # Stage b: already in ldconfig
    if ldconfig -p 2>/dev/null | grep -q "libcudart.so"; then
        CUDA_RUNTIME_OK=true
        echo "✅ libcudart.so found in ldconfig cache"
    else
        # Stage c: search known CUDA paths
        CUDART_PATH=$(find /usr/local/cuda* /usr/lib/x86_64-linux-gnu 2>/dev/null \
                      -name "libcudart.so*" 2>/dev/null | head -1)
        if [ -n "$CUDART_PATH" ]; then
            CUDA_LIB_DIR=$(dirname "$CUDART_PATH")
            echo "✅ libcudart.so found at $CUDART_PATH — registering path"
            export LD_LIBRARY_PATH="$CUDA_LIB_DIR:${LD_LIBRARY_PATH:-}"
            grep -qxF "export LD_LIBRARY_PATH=$CUDA_LIB_DIR:\$LD_LIBRARY_PATH" ~/.zshrc \
                || echo "export LD_LIBRARY_PATH=$CUDA_LIB_DIR:\$LD_LIBRARY_PATH" >> ~/.zshrc
            sudo ldconfig
            CUDA_RUNTIME_OK=true
        else
            # Stage d: GPU present but runtime missing — must install
            echo "📦 libcudart.so not found — installing nvidia-cuda-toolkit (required for GPU)..."
            sudo apt install -y nvidia-cuda-toolkit
            sudo ldconfig
            if ldconfig -p 2>/dev/null | grep -q "libcudart.so"; then
                CUDA_RUNTIME_OK=true
                echo "✅ nvidia-cuda-toolkit installed, libcudart.so now available"
            else
                echo "❌ CUDA runtime still missing after install — check NVIDIA driver compatibility"
                exit 1
            fi
        fi
    fi
else
    echo "💻 No GPU detected — using CPU build"
fi

pip install --upgrade pip

if [ -n "$CUDA_VERSION" ] && [ "$CUDA_RUNTIME_OK" = true ]; then
    # CUDA_VERSION is major+minor digits, e.g. "115" or "121"
    WHL_INDEX="cu${CUDA_VERSION}"
    WHL_URL="https://abetlen.github.io/llama-cpp-python/whl/$WHL_INDEX"

    # Check whether a pre-built wheel index exists for this CUDA version.
    # CUDA 12.x wheels are published; CUDA 11.x wheels are NOT (index returns 404).
    HTTP_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$WHL_URL/" 2>/dev/null || echo "000")

    if [ "$HTTP_STATUS" = "200" ]; then
        echo "⚙️ Installing llama-cpp-python from pre-built wheel (CUDA $CUDA_VERSION)..."
        pip install llama-cpp-python --extra-index-url "$WHL_URL"
    else
        # No pre-built wheel for this CUDA version — compile from source.
        # Required for CUDA 11.x (cu115, cu118, etc.) which have no published wheels.
        echo "⚙️ No pre-built wheel for cu$CUDA_VERSION — compiling llama-cpp-python from source..."
        pip install cmake ninja
        CMAKE_ARGS="-DGGML_CUDA=ON" FORCE_CMAKE=1 pip install llama-cpp-python --no-binary llama-cpp-python
    fi
else
    echo "⚙️ Installing llama-cpp-python (CPU only)..."
    pip install llama-cpp-python
fi

# 8. Remaining Dependencies
echo "⚙️ Installing other requirements..."
if [ -f "requirements.txt" ]; then
    grep -v "llama-cpp-python" requirements.txt > /tmp/temp_req.txt || true
    pip install -r /tmp/temp_req.txt
    rm /tmp/temp_req.txt
fi
pip install pycausalimpact redis

# 9. Redis Configuration + Service (background daemon)
REDIS_CONF="/etc/redis/redis.conf"
if [ -f "$REDIS_CONF" ]; then
    echo "⚙️  Patching Redis config..."
    sudo sed -i 's/^daemonize .*/daemonize yes/'          "$REDIS_CONF"
    sudo sed -i 's/^# daemonize .*/daemonize yes/'        "$REDIS_CONF"
    sudo sed -i 's/^maxmemory .*/maxmemory 2gb/'          "$REDIS_CONF"
    sudo sed -i 's/^# maxmemory .*/maxmemory 2gb/'        "$REDIS_CONF"
    sudo sed -i 's/^appendonly .*/appendonly yes/'         "$REDIS_CONF"
    sudo sed -i 's/^# appendonly .*/appendonly yes/'       "$REDIS_CONF"
    # Replace all existing save lines, then append the three required ones
    sudo sed -i '/^save /d'                                "$REDIS_CONF"
    printf '\nsave 900 1\nsave 300 10\nsave 60 10000\n' | sudo tee -a "$REDIS_CONF" > /dev/null
    echo "✅ Redis config patched."
fi
if command -v systemctl &>/dev/null && systemctl list-units --type=service &>/dev/null 2>&1; then
    sudo systemctl enable redis-server
    sudo systemctl start redis-server &
else
    # Fallback for containers / minimal Ubuntu without systemd
    redis-server --daemonize yes --logfile logs/redis.log
fi

# Set REDIS_URL (used by data_cache.py to select RedisBackend over JsonFile)
export REDIS_URL="${REDIS_URL:-redis://localhost:6379}"
grep -qxF "export REDIS_URL=$REDIS_URL" ~/.zshrc \
    || echo "export REDIS_URL=$REDIS_URL" >> ~/.zshrc
echo "🗄️  REDIS_URL=$REDIS_URL"

echo ""
echo "----------------------------------------------------------------"
echo "✅ Deployment Complete!"
echo "----------------------------------------------------------------"
echo "💡 To activate Zsh:  exec zsh"
echo "💡 To start app:     source .venv311/bin/activate && python main.py"
echo "----------------------------------------------------------------"
