#!/bin/bash
MISE="$HOME/.local/bin/mise"
SHIMS="$HOME/.local/share/mise/shims"

echo "=== updating /etc/environment to also include ~/.local/bin ==="
sudo sed -i "s|${SHIMS}:|${SHIMS}:$HOME/.local/bin:|" /etc/environment
cat /etc/environment

echo "=== installing tldr via mise ==="
$MISE use --global pipx:tldr

echo "=== verifying tldr shim ==="
ls "$SHIMS/tldr" && echo "tldr shim created OK" || echo "MISSING"
