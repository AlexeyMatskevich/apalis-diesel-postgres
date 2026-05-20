#!/usr/bin/env bash
# Development environment setup script
# Called automatically by nix develop shellHook

export PATH="$HOME/.cargo/bin:$(pwd)/scripts:$PATH"

ra_info="$(which rust-analyzer):$(rust-analyzer --version 2>/dev/null || echo 'unknown')"
nixd_info="$(which nixd):$(nixd --version 2>/dev/null || echo 'unknown')"
current_hash=$(echo "$ra_info:$nixd_info" | sha256sum | cut -d' ' -f1)
stored_hash=""
if [ -f .zed/.lsp-hash ]; then
  stored_hash=$(cat .zed/.lsp-hash)
fi

if [ ! -f .zed/settings.json ] || [ "$current_hash" != "$stored_hash" ]; then
  echo "Generating .zed/settings.json..."
  mkdir -p .zed
  cat > .zed/settings.json << EOF
{
  "lsp": {
    "rust-analyzer": {
      "binary": {
        "path": "$(which rust-analyzer)"
      }
    },
    "nixd": {
      "binary": {
        "path": "$(which nixd)"
      }
    },
    "nil": {
      "binary": {
        "path": "$(which nixd)"
      }
    }
  },
  "languages": {
    "TOML": {
      "language_servers": ["taplo", "!package-version-server"]
    }
  }
}
EOF
  echo "$current_hash" > .zed/.lsp-hash
fi

if [ ! -f .mcp.json ] || [ "$current_hash" != "$stored_hash" ]; then
  echo "Generating .mcp.json..."
  cat > .mcp.json << EOF
{
  "mcpServers": {
    "context7": {
      "command": "$(pwd)/scripts/context7-mcp",
      "args": []
    },
    "github": {
      "command": "$(pwd)/scripts/github-mcp",
      "args": []
    }
  }
}
EOF
fi
