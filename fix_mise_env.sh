#!/bin/bash
echo "=== current /etc/environment ==="
cat /etc/environment

echo "=== home dir ==="
echo $HOME

echo "=== mise shims exists? ==="
ls ~/.local/share/mise/shims/tldr 2>/dev/null && echo "EXISTS" || echo "NOT FOUND"

echo "=== appending to /etc/environment ==="
SHIMS_PATH="$HOME/.local/share/mise/shims"
# Read current PATH from /etc/environment if it exists, else use system default
if grep -q "^PATH=" /etc/environment; then
    # Prepend to existing PATH line
    sudo sed -i "s|^PATH=|PATH=${SHIMS_PATH}:|" /etc/environment
else
    echo "PATH=${SHIMS_PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" | sudo tee -a /etc/environment
fi

echo "=== updated /etc/environment ==="
cat /etc/environment
