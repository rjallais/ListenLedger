#!/bin/bash
# Source /etc/environment manually since WSL non-login shells don't get PAM
if [ -f /etc/environment ]; then
    set -a
    source /etc/environment
    set +a
fi
echo "which tldr: $(which tldr)"
echo "which topgrade: $(which topgrade)"
