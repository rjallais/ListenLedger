#!/bin/bash
echo 'export PATH="$HOME/.local/share/mise/shims:$PATH"' | sudo tee /etc/profile.d/mise-shims.sh
cat /etc/profile.d/mise-shims.sh
echo "--- verifying topgrade in non-interactive shell ---"
export PATH="$HOME/.local/share/mise/shims:$PATH"
which topgrade
