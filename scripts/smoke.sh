#!/usr/bin/env bash
# Smoke test for the storeboard foundation: every CLI verb against a real
# daemon on a temp socket. Needs `nix build` (./result) and jq on PATH.
set -euo pipefail

cd "$(dirname "$0")/.."

STOREBOARD_BIN="${STOREBOARD_BIN:-$(pwd)/result/bin}"
CLI="$STOREBOARD_BIN/storeboard"
DAEMON="$STOREBOARD_BIN/storeboardd"

work="$(mktemp -d)"
socket="$work/storeboard.sock"
config="$work/config.json"
pid=""
cleanup() {
	if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; fi
	rm -rf "$work"
}
trap cleanup EXIT

echo "== version"
"$CLI" version
"$CLI" version --json | jq -e '.name == "storeboard" and .apiVersion == 1' >/dev/null

echo "== config check: valid example"
"$CLI" --config config.example.json config check | grep -q "config OK"
"$CLI" --config config.example.json config check --json |
	jq -e '.valid and (.folders | length == 2)' >/dev/null

echo "== config check: typo rejected, reported as JSON"
echo '{"configVersion": 1, "folars": []}' >"$work/bad.json"
if "$CLI" --config "$work/bad.json" config check --json >"$work/bad-report.json" 2>/dev/null; then
	echo "FAIL: typo accepted" >&2
	exit 1
fi
jq -e '.valid == false and (.error | contains("folars"))' "$work/bad-report.json" >/dev/null

echo "== config check: missing file is a failure"
if "$CLI" --config "$work/absent.json" config check >/dev/null 2>&1; then
	echo "FAIL: missing config accepted" >&2
	exit 1
fi

echo "== daemon up"
cat >"$config" <<EOF
{
  "configVersion": 1,
  "dataDir": "$work/data",
  "socket": "$socket",
  "peer": "laptop",
  "peers": {
    "laptop": { "endpoints": ["127.0.0.1:4242"] },
    "desktop": { "endpoints": [] }
  },
  "groups": {
    "family": {
      "peers": ["laptop", "desktop"],
      "folders": {
        "photos": { "mount": "$work/Pictures" },
        "documents": {}
      }
    }
  },
  "mounts": { "documents": "$work/docs" }
}
EOF

"$DAEMON" --config "$config" &
pid=$!

for _ in $(seq 1 100); do
	[ -S "$socket" ] && break
	sleep 0.05
done
[ -S "$socket" ] || { echo "FAIL: daemon socket never appeared" >&2; exit 1; }

echo "== status, both modes"
"$CLI" --config "$config" status | grep -q "photos"
# Group model end to end: identity, membership, and the local mount
# override winning over the group's default.
"$CLI" --config "$config" status --json |
	jq -e --arg s "$socket" --arg w "$work" '
		.apiVersion == 1 and .socket == $s and .peer == "laptop" and
		(.groups | length == 1) and (.folders | length == 2) and
		([.folders[] | select(.name == "documents")][0].mount == ($w + "/docs"))' >/dev/null

echo "== second daemon refused on a live socket"
if "$DAEMON" --config "$config" 2>/dev/null; then
	echo "FAIL: second daemon started" >&2
	exit 1
fi

echo "== clean shutdown removes the socket"
kill "$pid"
if wait "$pid"; then pid=""; else
	echo "FAIL: daemon exited nonzero on SIGTERM" >&2
	exit 1
fi
[ ! -e "$socket" ] || { echo "FAIL: socket left behind" >&2; exit 1; }

echo "smoke OK"
