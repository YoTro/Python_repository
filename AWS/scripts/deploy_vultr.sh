#!/bin/bash

# ==============================================================================
# AWS Project Deployment Script for Vultr (Ubuntu 22.04 x64)
# HTTPS Repo | CUDA Auto-Detection | Llama-CPP Wheels | Zsh Memory
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
    sudo apt install -y git redis-server software-properties-common curl build-essential zsh chromium-browser chromium-chromedriver xvfb
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
echo 'export PATH=$PATH:/usr/lib/chromium-browser/' >> ~/.zshrc
source ~/.zshrc
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

# 7. Optimized Llama-CPP Installation
echo "🔍 Detecting CUDA for llama-cpp-python..."
CUDA_VERSION=""
if command -v nvidia-smi &> /dev/null; then
    # Extract major version (e.g., 12)
    CUDA_VERSION=$(nvidia-smi | grep "CUDA Version" | awk '{print $9}' | cut -d. -f1)
    echo "⚡ CUDA detected! Version: $CUDA_VERSION"
else
    echo "💻 No CUDA detected, falling back to CPU."
fi

pip install --upgrade pip

if [ -n "$CUDA_VERSION" ]; then
    echo "⚙️ Installing llama-cpp-python with CUDA $CUDA_VERSION support..."
    # Map CUDA version to pre-compiled wheel index
    # Note: 121, 122 etc are common suffixes
    WHL_INDEX="cu${CUDA_VERSION}1" # Attempt cu121, cu111 style
    pip install llama-cpp-python --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/$WHL_INDEX
else
    echo "⚙️ Installing llama-cpp-python (CPU only)..."
    pip install llama-cpp-python
fi

# 8. Remaining Dependencies
echo "⚙️ Installing other requirements..."
if [ -f "requirements.txt" ]; then
    # Filter out llama-cpp-python if already installed to avoid conflict
    grep -v "llama-cpp-python" requirements.txt > temp_req.txt || true
    pip install -r temp_req.txt
    rm temp_req.txt
fi
pip install pycausalimpact redis

# 9. Redis Service
sudo systemctl enable redis-server
sudo systemctl start redis-server

echo "----------------------------------------------------------------"
echo "✅ Deployment Sync Complete!"
echo "----------------------------------------------------------------"
echo "💡 To activate Zsh and Memory, run: exec zsh"
echo "💡 To start app: source .venv311/bin/activate && python main.py"
echo "----------------------------------------------------------------"
