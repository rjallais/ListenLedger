#!/bin/bash
# Create a script that bash will source for all non-interactive invocations
BASH_ENV_SCRIPT="/etc/mise-env.sh"

sudo tee "$BASH_ENV_SCRIPT" << 'EOF'
# Sourced by bash for non-interactive shells via BASH_ENV
export PATH="/home/rjallais/.local/share/mise/shims:/home/rjallais/.local/bin:$PATH"
EOF

sudo chmod 644 "$BASH_ENV_SCRIPT"
echo "=== created $BASH_ENV_SCRIPT ==="
cat "$BASH_ENV_SCRIPT"

# Append BASH_ENV to /etc/environment (only if not already set)
if ! grep -q "^BASH_ENV=" /etc/environment; then
    echo "BASH_ENV=/etc/mise-env.sh" | sudo tee -a /etc/environment
fi

echo "=== /etc/environment ==="
cat /etc/environment

echo "=== testing non-interactive resolution ==="
BASH_ENV=/etc/mise-env.sh bash -c "which tldr; which topgrade"
