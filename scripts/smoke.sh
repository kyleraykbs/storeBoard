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
  "folders": [
    { "name": "photos", "path": "$work/Pictures" },
    { "name": "documents", "path": "$work/Documents" }
  ]
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
"$CLI" --config "$config" status --json |
	jq -e --arg s "$socket" '.apiVersion == 1 and .socket == $s and (.folders | length == 2)' >/dev/null

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
