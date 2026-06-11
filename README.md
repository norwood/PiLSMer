# PiLSMer

PiLSMer is a SlateDB-backed, data-free key-value store.

It writes your data normally, then uses planning or compaction to replace stored
values with instructions for finding equivalent byte sequences inside a
deterministic stream. Reads still work. Everything else gets worse.

## Status

This is an MVP implementation of the core joke:

- `pilsmer-core` encodes raw values and reconstruction plans.
- `pilsmer-core` includes deterministic `sha256-counter:v1` and
  `pi.hex-fraction-prefix:v1` streams, packed k-gram indexes,
  dynamic-programming planning, reconstruction, and `explain`.
- `pilsmer-slate` wraps SlateDB and can rewrite values through guarded app-level
  `plan-key`, `vacuum-meaning`, or a SlateDB compaction filter.
- `pilsmer-cli` exposes local demo commands and scan-derived metrics.

The full benchmark suite and custom scheduler are still future work. A basic
CLI benchmark and ceremonial codec are available.

## Demo

Run commands from the repo root.

For the full local demo, including JSON, a tiny PNG, ceremonial planning,
`VACUUM MEANING`, metrics, and the benchmark matrix:

```sh
scripts/demo.sh
```

```sh
cargo run -q --bin pilsmer -- init /tmp/pilsmer-demo/db
printf '{"total":49.99,"status":"paid"}' > /tmp/pilsmer-demo/invoice.json
cargo run -q --bin pilsmer -- put /tmp/pilsmer-demo/db invoice:123 /tmp/pilsmer-demo/invoice.json
cargo run -q --bin pilsmer -- explain /tmp/pilsmer-demo/db invoice:123
cargo run -q --bin pilsmer -- plan-key /tmp/pilsmer-demo/db invoice:123
cargo run -q --bin pilsmer -- explain /tmp/pilsmer-demo/db invoice:123
cargo run -q --bin pilsmer -- get /tmp/pilsmer-demo/db invoice:123
```

`plan-key` is the deterministic single-key demo path. It rewrites the latest
value through the wrapper after checking that the source envelope has not
changed.
`explain` reports literal bytes, average chunk length, metadata amplification,
and philosophical compression status.

The default stream is `sha256-counter:v1` because it can produce arbitrary
prefix sizes. The pi demo stream is available with `--stream pi-prefix` and a
256-byte built-in prefix:

```sh
printf '\x24\x3f\x6a\x88' > /tmp/pilsmer-demo/pi-bytes.bin
cargo run -q --bin pilsmer -- --stream pi-prefix put /tmp/pilsmer-demo/db pi:first4 /tmp/pilsmer-demo/pi-bytes.bin
cargo run -q --bin pilsmer -- --stream pi-prefix plan-key /tmp/pilsmer-demo/db pi:first4
cargo run -q --bin pilsmer -- --stream pi-prefix explain /tmp/pilsmer-demo/db pi:first4
```

The standalone compactor path is available too:

```sh
cargo run -q --bin pilsmer -- compact /tmp/pilsmer-demo/db --into-nonexistence --run-ms 1000
```

`compact` runs SlateDB's foreground compactor for a bounded window with the
PiLSMer compaction filter installed. It still uses SlateDB's size-tiered
scheduler, so it is workload-dependent. The default `--min-compaction-sources`
is 4; use 2 for toy workloads with multiple flushed L0 files. `plan-key` is the
clearer single-key demo. After the bounded run, `compact` prints aggregate
compaction-filter stats from that process.

For maximum embarrassment:

```sh
cargo run -q --bin pilsmer -- compact /tmp/pilsmer-demo/db --humiliation maximum
```

That forces pure raw-to-plan compaction, sets `max_k = 1`, and uses the
ceremonial plan codec; it fails if the value cannot be represented without
literal bytes.

## CLI

```sh
pilsmer init <db>
pilsmer put <db> <key> <file>
pilsmer get <db> <key>
pilsmer explain <db> <key>
pilsmer plan-key <db> <key>
pilsmer vacuum-meaning <db> <key>|--all [--budget 10s]
pilsmer metrics <db>
pilsmer compact <db> [--mode normal|force-raw-to-plan|vacuum-meaning|disabled]
pilsmer bench <db> [--workload sha256-stream|tiny-json|json-4k|random-64k|repeated-64k|uuid-heavy|all-bytes|tiny-png] [--suite]
```

Global planning options:

```sh
--prefix-bytes <bytes>
--max-k <k>
--plan-codec compact-binary|ceremonial-cbor
--allow-literals
--stream sha256-counter|pi-prefix
```

`--allow-literals` is for development and tests. Forced compaction into plans
rejects it because literal chunks are still user bytes.

`bench` writes into a new directory and refuses to reuse an existing path. It
compares plain SlateDB values, PiLSMer raw envelopes, compact binary plans,
ceremonial plans, and post-vacuum plans. `--suite` runs the named workload
matrix; without it, `--values` and `--size` apply to the selected workload. Rows
include aggregate times, flush time, put p50/p95, and read p50/p95/p99.

`metrics` combines a scan-derived storage snapshot with runtime counters for
planner, reconstruction, and app-level vacuum activity in the current process.

## Correctness Contract

All reads and writes must go through the PiLSMer wrapper. Direct SlateDB reads
return envelopes, not user values.

Normal `put` stores a raw envelope. `put_with_options` can create an immediate
plan only for development and tests.
`vacuum_meaning` accepts `VacuumOptions` for per-run planning and reconstruction
limits.

After any successful `put`, `plan-key`, `vacuum-meaning`, or compaction-filter
rewrite, `get` should return the original logical bytes unless the key was
deleted. Planned values carry logical hashes and reconstruction verifies them.
The wrapper enforces `max_reconstruct_bytes` for planned reads and scans. Use
`scan_with_options(..., reconstruct: false)` or `scan_envelopes` for raw stored
envelopes without reconstructing planned values.

## Why

Because exact lookup is too honest.

What is stored?

Only metadata.

And logical hashes.

And chunk offsets.

And stream identifiers.

And enough regret to reconstruct the original value.
