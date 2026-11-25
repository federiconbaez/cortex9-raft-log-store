#!/usr/bin/env bash
# Fails if the pure domain imports I/O, time, threads, async runtimes or outer layers.
set -euo pipefail
cd "$(dirname "$0")/.."
pattern='std::(fs|net|io|time|thread|process|env)|tokio|async_std|crate::(adapters|application)'
if grep -rnE "$pattern" src/domain; then
  echo "architecture violation: src/domain must stay pure" >&2
  exit 1
fi
if grep -rnE 'crate::(adapters|application)' src/ports.rs; then
  echo "architecture violation: ports must not depend on adapters/application" >&2
  exit 1
fi
echo "architecture OK"
