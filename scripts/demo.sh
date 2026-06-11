#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${PILSMER_DEMO_DIR:-}" ]]; then
  DEMO_DIR="$PILSMER_DEMO_DIR"
else
  DEMO_DIR="$(mktemp -d /tmp/pilsmer-demo.XXXXXX)"
fi
DB="$DEMO_DIR/db"
DEMO_PREFIX_BYTES="${PILSMER_DEMO_PREFIX_BYTES:-65536}"

run() {
  printf '\n$ %s\n' "$*" >&2
  "$@"
}

pilsmer() {
  cargo run -q --manifest-path "$ROOT/Cargo.toml" --bin pilsmer -- --prefix-bytes "$DEMO_PREFIX_BYTES" "$@"
}

mkdir -p "$DEMO_DIR"

printf '{"total":49.99,"status":"paid"}' > "$DEMO_DIR/invoice.json"
python3 -c 'import base64,sys; sys.stdout.buffer.write(base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII="))' > "$DEMO_DIR/tiny.png"

printf 'demo_dir: %s\n' "$DEMO_DIR"
printf 'demo_prefix_bytes: %s\n' "$DEMO_PREFIX_BYTES"

run pilsmer init "$DB"
run pilsmer put "$DB" invoice:123 "$DEMO_DIR/invoice.json"
run pilsmer put "$DB" image:tiny "$DEMO_DIR/tiny.png"
run pilsmer explain "$DB" invoice:123

run pilsmer --max-k 1 --plan-codec ceremonial-cbor plan-key "$DB" invoice:123
run pilsmer --max-k 1 --plan-codec ceremonial-cbor plan-key "$DB" image:tiny
run pilsmer explain "$DB" invoice:123
run pilsmer get "$DB" invoice:123
printf '\n'
run pilsmer get "$DB" image:tiny > "$DEMO_DIR/readback.png"
cmp "$DEMO_DIR/tiny.png" "$DEMO_DIR/readback.png"

run pilsmer --max-k 3 vacuum-meaning "$DB" --all --budget 30s
run pilsmer metrics "$DB"
run pilsmer bench "$DEMO_DIR/bench" --values 10 --size 64
