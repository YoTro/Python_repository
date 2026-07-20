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

# Guard: abort if script is being run from inside the project directory.
# Running from within Python_repository/AWS would cause a nested clone and put
# venv311 in the wrong location.
_cwd=$(pwd)
if [[ "$_cwd" == *"/$PROJECT_NAME/$APP_SUBDIR"* || "$_cwd" == *"/$PROJECT_NAME/$APP_SUBDIR" ]]; then
    echo "❌ ERROR: Run this script from your HOME directory, not from inside the project." >&2
    echo "   cd ~ && bash $_cwd/scripts/deploy_ubuntu.sh" >&2
    exit 1
fi
unset _cwd
PYTHON_VERSION="3.11"
STATUS_FILE="$HOME/.deploy_status"

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
        zsh xvfb qrencode
    sudo add-apt-repository ppa:deadsnakes/ppa -y
    sudo apt update
    sudo apt install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-venv python${PYTHON_VERSION}-dev

    # Chromium: Ubuntu 22.04+ ships it as a snap only (chromium-browser deb is a
    # transitional stub that silently installs via snap).  On those releases the
    # apt package may not exist at all, so we install via snap directly.
    # On older Ubuntu (≤ 20.04) the deb package is still available.
    UBUNTU_VER=$(lsb_release -rs 2>/dev/null || echo "0")
    if dpkg --compare-versions "$UBUNTU_VER" ge "22.04" 2>/dev/null; then
        echo "📦 Ubuntu $UBUNTU_VER: installing Chromium via snap..."
        sudo snap install chromium
    else
        echo "📦 Ubuntu $UBUNTU_VER: installing Chromium via apt..."
        sudo apt install -y chromium-browser chromium-chromedriver
    fi

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

# Chrome env for IdentityPool / CookieBrowserPool (src/core/identity/pool.py)
# CHROME_EXECUTABLE: point DrissionPage at the system Chromium binary.
# Resolution order: /snap/bin/chromium (Ubuntu 22.04+ snap install) →
#   /usr/bin/chromium-browser (deb install) → chromium (any PATH entry).
# _resolve_chrome_path() in pool.py already checks these candidates, but
# setting the env var ensures DrissionPage skips its own (often wrong) detection.
CHROMIUM_BIN=""
for _candidate in /snap/bin/chromium /usr/bin/chromium-browser /usr/bin/chromium; do
    if [ -x "$_candidate" ]; then
        CHROMIUM_BIN="$_candidate"
        break
    fi
done
if [ -z "$CHROMIUM_BIN" ]; then
    CHROMIUM_BIN=$(command -v chromium-browser || command -v chromium || echo "")
fi
if [ -n "$CHROMIUM_BIN" ]; then
    export CHROME_EXECUTABLE="$CHROMIUM_BIN"
    grep -qF "export CHROME_EXECUTABLE=" ~/.zshrc \
        || echo "export CHROME_EXECUTABLE=$CHROMIUM_BIN" >> ~/.zshrc
    echo "🌐 Chrome binary: $CHROMIUM_BIN"
else
    echo "⚠️  Chromium binary not found — Tier-3 browser scraping unavailable."
fi
# CHROME_HEADLESS: Ubuntu server — always headless=new (no display server).
# Override with CHROME_HEADLESS=0 for manual-login / debug sessions via Xvfb.
export CHROME_HEADLESS="${CHROME_HEADLESS:-1}"
grep -qF "export CHROME_HEADLESS=" ~/.zshrc \
    || echo "export CHROME_HEADLESS=1" >> ~/.zshrc

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

# ── Object Storage (export_html / export_csv image/file upload) ─────────
# Backend choices: s3_compatible (R2 / S3 / MinIO)  |  local_http (VPS nginx)
STORAGE_BACKEND=s3_compatible

# --- Cloudflare R2 (recommended) ---
CLOUDFLARE_R2_ACCOUNT_ID=          # from R2 dashboard; endpoint auto-built from this
STORAGE_ACCESS_KEY_ID=             # R2 API token → Access Key ID
STORAGE_SECRET_ACCESS_KEY=         # R2 API token → Secret Access Key
STORAGE_BUCKET_NAME=               # your R2 bucket name
STORAGE_PUBLIC_URL=                # e.g. https://pub-<hash>.r2.dev or custom domain
STORAGE_REGION=auto

# --- AWS S3 (omit CLOUDFLARE_R2_ACCOUNT_ID, set region) ---
# STORAGE_ACCESS_KEY_ID=
# STORAGE_SECRET_ACCESS_KEY=
# STORAGE_BUCKET_NAME=
# STORAGE_PUBLIC_URL=https://<bucket>.s3.<region>.amazonaws.com
# STORAGE_REGION=us-east-1

# --- MinIO / self-hosted S3 (set explicit endpoint) ---
# STORAGE_ENDPOINT_URL=https://minio.yourdomain.com
# STORAGE_ACCESS_KEY_ID=
# STORAGE_SECRET_ACCESS_KEY=
# STORAGE_BUCKET_NAME=
# STORAGE_PUBLIC_URL=https://files.yourdomain.com
# STORAGE_REGION=auto

# --- VPS local directory + nginx (no cloud dependency) ---
# STORAGE_BACKEND=local_http
# STORAGE_LOCAL_DIR=/var/www/files
# STORAGE_PUBLIC_URL=https://files.yourdomain.com
EOF
    echo "⚠️  .env created — edit it and fill in all required values before running the app."
else
    echo "⏭️  .env already exists, skipping template creation."
fi

# 5. Download Model
# Q4_K_M quantised Qwen2.5-3B GGUF is ~1.9 GB; reject anything under 1 GB as truncated.
MODEL_MIN_BYTES=1073741824
_model_path="models/llm/$MODEL_NAME"
_model_size=$(stat -c%s "$_model_path" 2>/dev/null || echo 0)

if [ ! -f "$_model_path" ]; then
    echo "🤖 Downloading Qwen 3B Model..."
elif [ "$_model_size" -lt "$MODEL_MIN_BYTES" ]; then
    echo "⚠️  Model file is only ${_model_size} bytes — resuming interrupted download..."
else
    echo "⏭️  Model already downloaded ($(( _model_size / 1048576 )) MB), skipping."
fi

if [ ! -f "$_model_path" ] || [ "$_model_size" -lt "$MODEL_MIN_BYTES" ]; then
    # -c resumes from current file size; partial file is kept on failure for next run
    if ! wget -c --show-progress -O "$_model_path" "$MODEL_URL"; then
        echo "❌ Model download failed — partial file kept; re-run script to resume." >&2
        exit 1
    fi
    _model_size=$(stat -c%s "$_model_path" 2>/dev/null || echo 0)
    if [ "$_model_size" -lt "$MODEL_MIN_BYTES" ]; then
        echo "❌ Downloaded file is only ${_model_size} bytes — likely truncated. Removing." >&2
        rm -f "$_model_path"
        exit 1
    fi
    echo "✅ Model ready ($(( _model_size / 1048576 )) MB)."
fi

# 6. Virtual Environment (venv311)
# Uses the Python venv API directly (upgrade_deps=True bumps pip+setuptools to
# latest inside the new env, avoiding stale pip behaviour on older system Pythons).
if [ ! -d "venv311" ]; then
    echo "🛠️ Creating venv311 (Python 3.11)..."
    python${PYTHON_VERSION} -c "import venv; venv.create('venv311', with_pip=True, upgrade_deps=True)"
fi
source venv311/bin/activate

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

# Ubuntu 22.04 ships CUDA 11.5 via nvidia-cuda-toolkit.  CUDA < 12 cannot
# compile llama.cpp's C++17 code with GCC 11 (std_function.h parameter-pack
# bug).  Use g++-10 as the nvcc host compiler when that combination is
# detected; this avoids a 4 GB CUDA 12 reinstall.
CUDA_HOST_COMPILER_ARG=""
if [ -n "$CUDA_VERSION" ] && [ "$CUDA_VERSION" -lt 120 ] 2>/dev/null; then
    if ! command -v g++-10 &>/dev/null; then
        echo "📦 Installing g++-10 (CUDA $CUDA_VERSION + GCC 11 C++17 workaround)..."
        sudo apt install -y g++-10
    fi
    if command -v g++-10 &>/dev/null; then
        CUDA_HOST_COMPILER_ARG="-DCMAKE_CUDA_HOST_COMPILER=$(which g++-10)"
        echo "🔧 Using g++-10 as nvcc host compiler (CUDA $CUDA_VERSION < 12.0)"
    else
        echo "⚠️  g++-10 unavailable — source build may fail on GCC 11 + CUDA $CUDA_VERSION"
    fi
fi

pip install --upgrade pip

# ── llama-cpp-python installation helpers ────────────────────────────────────
LLAMA_WHEEL_CACHE="$HOME/.cache/llama_cpp_wheels"

# Install the minimal CUDA 12 runtime (libcudart.so.12) from NVIDIA's apt repo.
# Does NOT install nvcc or the full toolkit — download is ~50 MB.
# Required so pre-built cu12x wheels can dlopen libcudart at runtime.
function _ensure_cuda12_runtime() {
    if ldconfig -p 2>/dev/null | grep -q "libcudart.so.12"; then
        echo "✅ libcudart.so.12 already present"
        return 0
    fi
    # Detect Ubuntu release for the repo URL
    local ubuntu_codename
    ubuntu_codename=$(lsb_release -cs 2>/dev/null || echo "jammy")
    local keyring_deb="/tmp/cuda-keyring.deb"
    if ! apt-cache show cuda-cudart-12-4 &>/dev/null 2>&1; then
        echo "📦 Adding NVIDIA CUDA 12 apt repository..."
        wget -q -O "$keyring_deb" \
            "https://developer.download.nvidia.com/compute/cuda/repos/ubuntu${ubuntu_codename/./}/x86_64/cuda-keyring_1.1-1_all.deb" \
            || wget -q -O "$keyring_deb" \
            "https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb"
        sudo dpkg -i "$keyring_deb"
        sudo apt-get update -q
    fi
    # Try versions in descending order; install runtime only (not full toolkit)
    for _ver in 12-6 12-4 12-2 12-1; do
        if sudo apt-get install -y "cuda-cudart-${_ver}" 2>/dev/null; then
            sudo ldconfig
            echo "✅ Installed cuda-cudart-${_ver} (CUDA 12 runtime)"
            return 0
        fi
    done
    echo "⚠️  Could not install CUDA 12 runtime via apt" >&2
    return 1
}

# Build from source using all CPU cores; cache the .whl so re-runs skip compile.
function _install_llama_from_source() {
    local cmake_args="$1"
    local cpu_count
    cpu_count=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    mkdir -p "$LLAMA_WHEEL_CACHE"
    local cached
    cached=$(ls "$LLAMA_WHEEL_CACHE"/llama_cpp_python-*.whl 2>/dev/null | head -1)
    if [ -n "$cached" ]; then
        echo "⚡ Re-using cached wheel (skipping compile): $(basename "$cached")"
        pip install "$cached"
        return
    fi
    pip install cmake ninja
    echo "⏱  Compiling with $cpu_count parallel jobs — wheel cached for future re-runs..."
    CMAKE_ARGS="$cmake_args" CMAKE_BUILD_PARALLEL_LEVEL="$cpu_count" FORCE_CMAKE=1 \
        pip wheel llama-cpp-python --no-binary llama-cpp-python \
        --no-deps -w "$LLAMA_WHEEL_CACHE"
    cached=$(ls "$LLAMA_WHEEL_CACHE"/llama_cpp_python-*.whl 2>/dev/null | head -1)
    if [ -z "$cached" ]; then
        echo "❌ Wheel build produced no output file." >&2; exit 1
    fi
    pip install "$cached"
}

# Try pre-built cu12x wheels when the installed CUDA is too old for abetlen's index.
# Queries the driver's max CUDA capability from nvidia-smi (forward-compatible),
# installs libcudart.so.12, then downloads the highest available cu12x wheel.
# Returns 0 on success, 1 if upgrade is not possible.
function _try_cuda12_prebuilt_wheel() {
    local driver_cuda_major
    driver_cuda_major=$(nvidia-smi 2>/dev/null \
        | grep -oP "CUDA Version: \K[0-9]+" | head -1 || echo "0")
    if [ "$driver_cuda_major" -lt 12 ] 2>/dev/null; then
        echo "⚠️  Driver max CUDA is $driver_cuda_major — cannot use cu12x pre-built wheels"
        return 1
    fi
    echo "ℹ️  Driver supports up to CUDA $driver_cuda_major — trying CUDA 12 pre-built wheel..."
    _ensure_cuda12_runtime || return 1
    for _cu in cu126 cu125 cu124 cu122 cu121; do
        local _url="https://abetlen.github.io/llama-cpp-python/whl/$_cu"
        local _status
        _status=$(curl -sf -o /dev/null -w "%{http_code}" "$_url/" 2>/dev/null || echo "000")
        if [ "$_status" = "200" ]; then
            echo "⚙️  Installing llama-cpp-python pre-built wheel ($_cu)..."
            pip install llama-cpp-python --extra-index-url "$_url" && return 0
        fi
    done
    echo "⚠️  No cu12x wheel found on abetlen index"
    return 1
}
# ─────────────────────────────────────────────────────────────────────────────

if [ "$CUDA_RUNTIME_OK" = true ]; then
    if [ "$FORCE_SOURCE_BUILD" = true ]; then
        # nvcc absent — driver CUDA version unknown; try cu12x pre-built first
        echo "⚙️  nvcc unavailable — trying CUDA 12 pre-built wheel before source build..."
        if ! _try_cuda12_prebuilt_wheel; then
            echo "⚙️  Falling back to source compilation..."
            _install_llama_from_source "-DGGML_CUDA=ON $CUDA_HOST_COMPILER_ARG"
        fi
    else
        WHL_INDEX="cu${CUDA_VERSION}"
        WHL_URL="https://abetlen.github.io/llama-cpp-python/whl/$WHL_INDEX"
        HTTP_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$WHL_URL/" 2>/dev/null || echo "000")
        if [ "$HTTP_STATUS" = "200" ]; then
            echo "⚙️ Installing llama-cpp-python from pre-built wheel (CUDA $CUDA_VERSION)..."
            pip install llama-cpp-python --extra-index-url "$WHL_URL"
        else
            # No pre-built wheel for this CUDA version (e.g. cu115, cu118) —
            # try CUDA 12 upgrade path before falling back to source compilation.
            echo "⚠️  No pre-built wheel for cu$CUDA_VERSION — trying CUDA 12 upgrade..."
            if ! _try_cuda12_prebuilt_wheel; then
                echo "⚙️  Falling back to source compilation..."
                _install_llama_from_source "-DGGML_CUDA=ON $CUDA_HOST_COMPILER_ARG"
            fi
        fi
    fi
else
    echo "⚙️ Installing llama-cpp-python (CPU only)..."
    pip install llama-cpp-python
fi

# 8. Remaining Dependencies
echo "⚙️ Installing package dependencies..."
# Pin the already-installed llama-cpp-python (GPU/CPU variant) so pip does not replace it
pip freeze | grep -i "llama" > /tmp/llama_pin.txt
pip install -e . -c /tmp/llama_pin.txt
rm /tmp/llama_pin.txt
# Downgrade arviz if needed — 0.19+ emits FutureWarning and INFO noise about
# missing modular sub-packages (arviz-base/stats/plots) that causalimpact doesn't install.
pip install "arviz<0.19"

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
    sudo systemctl start redis-server
    # Verify Redis came up — fail fast rather than silently continue
    if ! sudo systemctl is-active --quiet redis-server; then
        echo "❌ Redis failed to start. Journal:" >&2
        sudo journalctl -u redis-server -n 20 --no-pager >&2
        exit 1
    fi
    echo "✅ Redis is running."
else
    # Fallback for containers / minimal Ubuntu without systemd
    redis-server --daemonize yes --logfile logs/redis.log
    # Brief wait then ping to confirm
    sleep 2
    if ! redis-cli ping &>/dev/null; then
        echo "❌ Redis daemon failed to start — check logs/redis.log" >&2
        exit 1
    fi
    echo "✅ Redis is running (daemon mode)."
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
echo "💡 To start app:     source venv311/bin/activate && python main.py"
echo "----------------------------------------------------------------"
