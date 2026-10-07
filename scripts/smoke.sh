#!/usr/bin/env bash
# Smoke test for the storeboard foundation: every CLI verb, two daemons
# connecting over the wire, a replica folder syncing, and a streamed folder
# mounting, detaching, and merging offline work back in. Needs `nix build`
# (./result) and jq on PATH.
set -euo pipefail

cd "$(dirname "$0")/.."

STOREBOARD_BIN="${STOREBOARD_BIN:-$(pwd)/result/bin}"
CLI="$STOREBOARD_BIN/storeboard"
DAEMON="$STOREBOARD_BIN/storeboardd"
export STOREBOARD_SCAN_INTERVAL=200ms

work="$(mktemp -d)"
socket="$work/storeboard.sock"
socket2="$work/s2.sock"
config="$work/config.json"
config2="$work/config2.json"
pid=""
pid2=""
cleanup() {
	if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; fi
	if [ -n "$pid2" ]; then kill "$pid2" 2>/dev/null || true; fi
	rm -rf "$work"
}
trap cleanup EXIT

# wait_for <seconds> <description> <command...>
wait_for() {
	local deadline=$(( $(date +%s) + $1 )) desc="$2"
	shift 2
	while [ "$(date +%s)" -lt "$deadline" ]; do
		if "$@"; then return 0; fi
		sleep 0.1
	done
	echo "FAIL: timed out waiting for $desc" >&2
	exit 1
}

echo "== version"
"$CLI" version
"$CLI" version --json | jq -e '.name == "storeboard" and .apiVersion == 1' >/dev/null

echo "== config check: valid example"
"$CLI" --config config.example.json config check | grep -q "config OK"
"$CLI" --config config.example.json config check --json |
	jq -e '.valid and (.folders | length == 3)' >/dev/null

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
mkdir -p "$work/Pictures"
cat >"$config" <<EOF
{
  "configVersion": 1,
  "dataDir": "$work/data",
  "socket": "$socket",
  "peer": "laptop",
  "peers": {
    "laptop": { "endpoints": ["127.0.0.1:42421"] },
    "desktop": { "endpoints": ["127.0.0.1:42422"] }
  },
  "groups": {
    "family": {
      "peers": ["laptop", "desktop"],
      "folders": {
        "photos": { "mount": "$work/Pictures" },
        "documents": { "mount": "$work/docs" }
      }
    }
  },
  "mounts": { "photos": "$work/Pictures", "documents": "$work/docs" }
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
	jq -e --arg s "$socket" --arg w "$work" '
		.apiVersion == 1 and .socket == $s and .peer == "laptop" and
		(.groups | length == 1) and (.folders | length == 2) and
		([.folders[] | select(.name == "documents")][0].mount == ($w + "/docs")) and
		([.folders[].keepOffline] | all) and
		([.sync[].state] | all(. == "replica"))' >/dev/null

echo "== second daemon refused on a live socket"
if "$DAEMON" --config "$config" 2>/dev/null; then
	echo "FAIL: second daemon started" >&2
	exit 1
fi

echo "== two daemons connect over the wire"
mkdir -p "$work/PicturesB"
cat >"$config2" <<EOF
{
  "configVersion": 1,
  "dataDir": "$work/data2",
  "socket": "$socket2",
  "peer": "desktop",
  "peers": {
    "laptop": { "endpoints": ["127.0.0.1:42421"] },
    "desktop": { "endpoints": ["127.0.0.1:42422"] }
  },
  "groups": {
    "family": {
      "peers": ["laptop", "desktop"],
      "folders": {
        "photos": { "mount": "$work/PicturesB" },
        "documents": { "mount": "$work/remoteDocs" }
      }
    }
  },
  "mounts": { "photos": "$work/PicturesB", "documents": "$work/remoteDocs" },
  "keepOffline": { "documents": false }
}
EOF

"$DAEMON" --config "$config2" &
pid2=$!

for _ in $(seq 1 20); do
	[ -S "$socket2" ] && break
	sleep 0.05
done
[ -S "$socket2" ] || { echo "FAIL: second daemon socket never appeared" >&2; exit 1; }

connected() {
	"$CLI" --config "$1" status --json |
		jq -e '[.connections[] | select(.state == "connected")] | length >= 1' >/dev/null 2>&1
}
wait_for 10 "laptop sees desktop" connected "$config"
wait_for 10 "desktop sees laptop" connected "$config2"

echo "== replica folder syncs"
echo "hello from laptop" >"$work/Pictures/a.txt"
wait_for 10 "file replicated to desktop (replica)" test -f "$work/PicturesB/a.txt"
grep -q "hello from laptop" "$work/PicturesB/a.txt"

echo "== streamed folder attaches"
wait_for 10 "desktop mount attached" bash -c "[ -L '$work/remoteDocs' ]"
"$CLI" --config "$config2" status --json |
	jq -e '[.sync[] | select(.folder == "documents")][0].state == "attached"' >/dev/null
echo "holder content" >"$work/docs/from-laptop.txt"
wait_for 10 "holder content visible through the mount" test -f "$work/remoteDocs/from-laptop.txt"
grep -q "holder content" "$work/remoteDocs/from-laptop.txt"

echo "== streamed folder detaches when the holder is gone"
kill "$pid"
wait "$pid" 2>/dev/null || true
pid=""

stream_state() {
	"$CLI" --config "$config2" status --json |
		jq -r '[.sync[] | select(.folder == "documents")][0].state' 2>/dev/null
}
detached() { [ "$(stream_state)" = "detached" ]; }
mount_empty() { [ -z "$(ls -A "$work/remoteDocs" 2>/dev/null)" ]; }

wait_for 20 "detached state" detached
wait_for 10 "mount replaced by an empty dir" mount_empty

echo "== offline work merges back in"
echo "written offline" >"$work/remoteDocs/offline.txt"

"$DAEMON" --config "$config" &
pid=$!
for _ in $(seq 1 100); do
	[ -S "$socket" ] && break
	sleep 0.05
done

wait_for 20 "holder received the offline file" test -f "$work/docs/offline.txt"
grep -q "written offline" "$work/docs/offline.txt"
wait_for 10 "mount reattached with both files" test -f "$work/remoteDocs/from-laptop.txt"
test -f "$work/remoteDocs/offline.txt"

echo "== clean shutdown removes the socket"
kill "$pid" "$pid2"
if wait "$pid" && wait "$pid2"; then pid=""; pid2=""; else
	echo "FAIL: daemon exited nonzero on SIGTERM" >&2
	exit 1
fi
[ ! -e "$socket" ] || { echo "FAIL: socket left behind" >&2; exit 1; }
[ ! -e "$socket2" ] || { echo "FAIL: second socket left behind" >&2; exit 1; }

echo "smoke OK"
