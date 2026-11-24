#!/usr/bin/env bash
# Runs one command against the real Tigris bucket: scripts/provider.sh with the provider named.
exec "$(dirname "$0")/provider.sh" tigris "$@"
