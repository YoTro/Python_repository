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

# 4b. Create .env template if not present
if [ ! -f ".env" ]; then
    echo "📝 Creating .env template — fill in values before starting the app..."
    cat > .env << 'EOF'
# ── LLM Providers ──────────────────────────────────────────────────────
DEFAULT_LLM_PROVIDER=gemini
GEMINI_API_KEY=
ANTHROPIC_API_KEY=
DEEPSEEK_API_KEY=
LOCAL_MODEL_PATH=models/llm/qwen2.5-3b-instruct-q4_k_m.gguf
MAX_LLM_OUTPUT_TOKENS=

# ── Amazon Ads API ──────────────────────────────────────────────────────
AMAZON_ADS_DEFAULT_STORE=US
AMAZON_ADS_CLIENT_ID=
AMAZON_ADS_CLIENT_SECRET=
AMAZON_ADS_REFRESH_TOKEN_US=
AMAZON_ADS_PROFILE_ID_US=
AMAZON_ADS_FALLBACK_ASIN_US=

# ── Amazon SP-API / LWA ─────────────────────────────────────────────────
AMAZON_LWA_CLIENT_ID=
AMAZON_LWA_CLIENT_SECRET=
AMAZON_SP_API_REFRESH_TOKEN_US=

# ── Feishu / Lark — amazon_bot ──────────────────────────────────────────
FEISHU_AMAZON_BOT_APP_ID=
FEISHU_AMAZON_BOT_APP_SECRET=
FEISHU_AMAZON_BOT_USER_ACCESS_TOKEN=
FEISHU_AMAZON_BOT_WEBHOOK_URL=

# ── Feishu / Lark — test_bot ────────────────────────────────────────────
FEISHU_TEST_BOT_APP_ID=
FEISHU_TEST_BOT_APP_SECRET=
FEISHU_TEST_BOT_USER_ACCESS_TOKEN=
FEISHU_TEST_BOT_WEBHOOK_URL=

# ── Third-party Market Data ─────────────────────────────────────────────
SELLERSPRITE_EMAIL=
SELLERSPRITE_PASSWORD=
XIYOUZHAOCI_PHONE=
LINGXING_ACCOUNT=
LINGXING_PASSWORD=

# ── Infrastructure ──────────────────────────────────────────────────────
REDIS_URL=redis://localhost:6379
SERVER_IP=
SERVER_USER=
EOF
    echo "⚠️  .env created — edit it and fill in all required values before running the app."
else
    echo "⏭️  .env already exists, skipping template creation."
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
FORCE_SOURCE_BUILD=false

if command -v nvidia-smi &> /dev/null; then
    # ── Runtime check: libcudart.so must exist for any GPU build ────────
    if ldconfig -p 2>/dev/null | grep -q "libcudart.so"; then
        CUDA_RUNTIME_OK=true
        echo "✅ libcudart.so found in ldconfig cache"
    else
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
            echo "📦 libcudart.so not found — installing nvidia-cuda-toolkit..."
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

    # ── Version: nvcc is the only authoritative source ──────────────────
    # nvidia-smi reports the driver's *maximum supported* CUDA version, not
    # the installed toolkit version — using it caused wrong wheel selection
    # (e.g. cu115 host installing a cu12x wheel).  If nvcc is absent the
    # toolkit is incomplete and its version cannot be trusted; compile from
    # source instead so CMake links against the actual libcudart.so.
    if command -v nvcc &> /dev/null; then
        CUDA_VERSION=$(nvcc --version | grep "release" | sed 's/.*release \([0-9]*\)\.\([0-9]*\).*/\1\2/')
        echo "⚡ nvcc reports CUDA $CUDA_VERSION (wheel suffix: cu$CUDA_VERSION)"
    else
        echo "⚠️  nvcc not found — toolkit version unknown; will compile from source"
        FORCE_SOURCE_BUILD=true
    fi
else
    echo "💻 No GPU detected — using CPU build"
fi

pip install --upgrade pip

if [ "$CUDA_RUNTIME_OK" = true ]; then
    if [ "$FORCE_SOURCE_BUILD" = true ]; then
        # nvcc absent: version unknown, compile against the libcudart.so that is present
        echo "⚙️ Compiling llama-cpp-python from source (nvcc unavailable, version unknown)..."
        pip install cmake ninja
        CMAKE_ARGS="-DGGML_CUDA=ON" FORCE_CMAKE=1 pip install llama-cpp-python --no-binary llama-cpp-python
    else
        WHL_INDEX="cu${CUDA_VERSION}"
        WHL_URL="https://abetlen.github.io/llama-cpp-python/whl/$WHL_INDEX"
        HTTP_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$WHL_URL/" 2>/dev/null || echo "000")
        if [ "$HTTP_STATUS" = "200" ]; then
            echo "⚙️ Installing llama-cpp-python from pre-built wheel (CUDA $CUDA_VERSION)..."
            pip install llama-cpp-python --extra-index-url "$WHL_URL"
        else
            # No pre-built wheel for this CUDA version (e.g. cu115, cu118)
            echo "⚙️ No pre-built wheel for cu$CUDA_VERSION — compiling from source..."
            pip install cmake ninja
            CMAKE_ARGS="-DGGML_CUDA=ON" FORCE_CMAKE=1 pip install llama-cpp-python --no-binary llama-cpp-python
        fi
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
