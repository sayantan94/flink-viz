#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

if [[ -d "$FLINK_HOME" ]]; then
  echo "Flink already at $FLINK_HOME"
  exit 0
fi

mkdir -p "$FV_ROOT/infra/local"
cd "$FV_ROOT/infra/local"

TARBALL="flink-1.18.1-bin-scala_2.12.tgz"
if [[ ! -f "$TARBALL" ]]; then
  echo "Downloading Flink 1.18.1 (~460 MB)..."
  curl -L --fail -o "$TARBALL" \
    "https://archive.apache.org/dist/flink/flink-1.18.1/$TARBALL"
fi

echo "Extracting..."
tar xzf "$TARBALL"
echo "Flink installed at $FLINK_HOME"
