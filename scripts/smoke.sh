#!/usr/bin/env bash
# Smoke test for storeboard: every CLI verb, two daemons connecting over the
# wire, a replica store syncing, a remote store staying visible while its
# holder is gone, the tree's symlinks, and takeover of a path that already
# holds files. Needs `nix build` (./result) and jq on PATH.
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
"$CLI" version --json | jq -e '.name == "storeboard" and .apiVersion == 2' >/dev/null

echo "== config check: valid example"
"$CLI" --config config.example.json config check | grep -q "config OK"
"$CLI" --config config.example.json config check --json |
	jq -e '.valid and (.stores | length == 4)' >/dev/null

echo "== config check: typo rejected, reported as JSON"
echo '{"configVersion": 2, "strs": []}' >"$work/bad.json"
if "$CLI" --config "$work/bad.json" config check --json >"$work/bad-report.json" 2>/dev/null; then
	echo "FAIL: typo accepted" >&2
	exit 1
fi
jq -e '.valid == false and (.error | contains("strs"))' "$work/bad-report.json" >/dev/null

echo "== config check: missing file is a failure"
if "$CLI" --config "$work/absent.json" config check >/dev/null 2>&1; then
	echo "FAIL: missing config accepted" >&2
	exit 1
fi

echo "== daemon up"
cat >"$config" <<EOF
{
  "configVersion": 2,
  "root": "$work/laptop",
  "dataDir": "$work/data",
  "socket": "$socket",
  "peer": "laptop",
  "peers": {
    "laptop": { "endpoints": ["127.0.0.1:42421"] },
    "desktop": { "endpoints": ["127.0.0.1:42422"] }
  },
  "groups": {
    "family": { "peers": ["laptop", "desktop"] }
  },
  "stores": {
    "photos": {
      "group": "family",
      "symlinks": ["$work/Pictures"],
      "stores": { "bulk": { "group": "family", "path": "Bulk" } }
    },
    "documents": { "group": "family", "symlinks": ["$work/docs"] },
    "archive": { "group": "family", "symlinks": ["$work/archive"] }
  },
  "backup": { "enable": true, "keep": "7d" }
}
EOF

"$DAEMON" --config "$config" &
pid=$!

for _ in $(seq 1 100); do
	[ -S "$socket" ] && break
	sleep 0.05
done
[ -S "$socket" ] || { echo "FAIL: daemon socket never appeared" >&2; exit 1; }

echo "== status, the tree, and the canonical dirs"
wait_for 10 "every store is prepared" bash -c '
  "$0" --config "$1" status --json 2>/dev/null |
    jq -e "(.stores | length == 4) and ([.sync[].state] | all(. != \"\"))" >/dev/null
' "$CLI" "$config"
"$CLI" --config "$config" status | grep -q "photos"
status_json=$("$CLI" --config "$config" status --json)
echo "$status_json" |
	jq -e --arg s "$socket" --arg w "$work" '
		.apiVersion == 2 and .socket == $s and .peer == "laptop" and
		.root == ($w + "/laptop") and
		(.groups | length == 1) and (.stores | length == 4) and
		([.stores[] | select(.name == "documents")][0].root == ($w + "/laptop/documents")) and
		([.stores[] | select(.name == "bulk")][0].parent == "photos") and
		([.sync[].state] | all(. == "replica"))' >/dev/null || {
	echo "FAIL: the status document is not what this test expects:" >&2
	echo "$status_json" | jq -c '{
		apiVersion, socket, peer, root,
		groups: (.groups | length), stores: (.stores | length),
		states: [.sync[] | {store, state}]
	}' >&2
	exit 1
}

# The tree is symlinks: a declared path points at the canonical dir, and a
# child store appears inside its parent without the parent owning its bytes.
[ -L "$work/Pictures" ] || { echo "FAIL: declared symlink missing" >&2; exit 1; }
[ "$(readlink "$work/Pictures")" = "$work/laptop/photos" ] || {
	echo "FAIL: symlink target is $(readlink "$work/Pictures")" >&2
	exit 1
}
[ -L "$work/laptop/photos/Bulk" ] || { echo "FAIL: child store link missing" >&2; exit 1; }
[ "$(readlink "$work/laptop/photos/Bulk")" = "$work/laptop/bulk" ] || {
	echo "FAIL: child link target is $(readlink "$work/laptop/photos/Bulk")" >&2
	exit 1
}

# Nothing declared: the store root is permissioned like the root's container.
container_mode=$(stat -c %a "$work")
store_mode=$(stat -c %a "$work/laptop/photos")
[ "$store_mode" = "$container_mode" ] || {
	echo "FAIL: store root mode $store_mode, want the container's $container_mode" >&2
	exit 1
}

echo "== second daemon refused on a live socket"
if "$DAEMON" --config "$config" 2>/dev/null; then
	echo "FAIL: second daemon started" >&2
	exit 1
fi

echo "== two daemons connect over the wire"
mkdir -p "$work/remoteArchive"
echo "local file, kept by takeover" >"$work/remoteArchive/local.txt"
cat >"$config2" <<EOF
{
  "configVersion": 2,
  "root": "$work/desktop",
  "dataDir": "$work/data2",
  "socket": "$socket2",
  "peer": "desktop",
  "peers": {
    "laptop": { "endpoints": ["127.0.0.1:42421"] },
    "desktop": { "endpoints": ["127.0.0.1:42422"] }
  },
  "groups": {
    "family": { "peers": ["laptop", "desktop"] }
  },
  "stores": {
    "photos": {
      "group": "family",
      "symlinks": ["$work/PicturesB"],
      "stores": { "bulk": { "group": "family", "sync": "remote", "path": "Bulk" } }
    },
    "documents": { "group": "family", "sync": "remote", "symlinks": ["$work/remoteDocs"] },
    "archive": { "group": "family", "sync": "remote", "symlinks": ["$work/remoteArchive"] }
  }
}
EOF

"$DAEMON" --config "$config2" &
pid2=$!

for _ in $(seq 1 100); do
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

echo "== replica store syncs"
echo "hello from laptop" >"$work/Pictures/a.txt"
wait_for 10 "file replicated to desktop" test -f "$work/desktop/photos/a.txt"
grep -q "hello from laptop" "$work/desktop/photos/a.txt"

echo "== backup lists versions and restores one"
wait_for 10 "the first version is recorded" bash -c '
  "$0" --config "$1" backup list photos a.txt 2>/dev/null | grep -q "1 recorded"
' "$CLI" "$config"
echo "edited later" >"$work/Pictures/a.txt"
wait_for 10 "the edit is recorded" bash -c '
  "$0" --config "$1" backup list photos a.txt 2>/dev/null | grep -q "2 recorded"
' "$CLI" "$config"
first=$("$CLI" --config "$config" backup list photos a.txt --json | jq -r '.versions[-1].time')
"$CLI" --config "$config" backup restore photos a.txt --at "$first" >/dev/null
grep -q "hello from laptop" "$work/Pictures/a.txt" || {
	echo "FAIL: restore did not bring the first version back" >&2
	exit 1
}

echo "== a deleted file comes back from its history"
rm "$work/Pictures/a.txt"
wait_for 10 "the delete is recorded" bash -c '
  "$0" --config "$1" backup list photos a.txt --json 2>/dev/null | jq -e "[.versions[] | select(.deleted)] | length >= 1" >/dev/null
' "$CLI" "$config"
"$CLI" --config "$config" backup restore photos a.txt >/dev/null
test -f "$work/Pictures/a.txt" || { echo "FAIL: deleted file not restored" >&2; exit 1; }

echo "== remote store is live and visible"
wait_for 10 "desktop documents attached" bash -c '
  "$0" --config "$1" status --json |
    jq -e "[.sync[] | select(.store == \"documents\")][0].state == \"attached\"" >/dev/null
' "$CLI" "$config2"
echo "holder content" >"$work/docs/from-laptop.txt"
wait_for 10 "holder content visible on desktop" test -f "$work/desktop/documents/from-laptop.txt"
grep -q "holder content" "$work/remoteDocs/from-laptop.txt"

echo "== send puts a file into a store, and a taken name is parked, not lost"
echo "from the share sheet" >"$work/send-me.txt"
"$CLI" --config "$config" send photos "$work/send-me.txt" --as "sent/holiday.txt" --json |
	jq -e '.store == "photos" and .path == "sent/holiday.txt" and (.parked // "") == ""' >/dev/null
grep -q "from the share sheet" "$work/Pictures/sent/holiday.txt"
wait_for 10 "the sent file reached the peer" test -f "$work/desktop/photos/sent/holiday.txt"
grep -q "from the share sheet" "$work/desktop/photos/sent/holiday.txt"
echo "second take" >"$work/send-me2.txt"
parked=$("$CLI" --config "$config" send photos "$work/send-me2.txt" --as "sent/holiday.txt" --json | jq -r '.parked')
[ -n "$parked" ] && [ "$parked" != "null" ] || {
	echo "FAIL: a taken name was not parked" >&2
	exit 1
}
grep -q "from the share sheet" "$work/Pictures/$parked"
grep -q "second take" "$work/Pictures/sent/holiday.txt"

echo "== a path with content says why, and takeover links it without losing anything"
"$CLI" --config "$config2" status | grep -q "(path has content)"
"$CLI" --config "$config2" status --json |
	jq -e '[.sync[] | select(.store == "archive")][0].symlinks[0].state == "busy"' >/dev/null
"$CLI" --config "$config2" store takeover archive --path "$work/remoteArchive" --json |
	jq -e '.state == "attached" and .moved == 1' >/dev/null
[ -L "$work/remoteArchive" ] || { echo "FAIL: takeover did not link the path" >&2; exit 1; }
grep -q "local file, kept by takeover" "$work/remoteArchive/local.txt"
wait_for 10 "taken-over content reached the holder" test -f "$work/laptop/archive/local.txt"
grep -q "local file, kept by takeover" "$work/laptop/archive/local.txt"

echo "== a remote store keeps its content when the holder goes away"
kill "$pid"
wait "$pid" 2>/dev/null || true
pid=""

detached() {
	"$CLI" --config "$config2" status --json |
		jq -e '[.sync[] | select(.store == "documents")][0].state == "detached"' >/dev/null 2>&1
}
wait_for 20 "detached state" detached
grep -q "holder content" "$work/remoteDocs/from-laptop.txt" || {
	echo "FAIL: the remote store lost its content when the holder left" >&2
	exit 1
}

echo "== offline work syncs when the holder returns"
echo "written offline" >"$work/remoteDocs/offline.txt"

"$DAEMON" --config "$config" &
pid=$!
for _ in $(seq 1 100); do
	[ -S "$socket" ] && break
	sleep 0.05
done
[ -S "$socket" ] || { echo "FAIL: holder socket never came back" >&2; exit 1; }

wait_for 20 "holder received the offline file" test -f "$work/docs/offline.txt"
grep -q "written offline" "$work/docs/offline.txt"
wait_for 10 "the remote link shows both files" test -f "$work/remoteDocs/offline.txt"
test -f "$work/remoteDocs/from-laptop.txt"

echo "== clean shutdown removes the socket"
kill "$pid" "$pid2"
if wait "$pid" && wait "$pid2"; then pid=""; pid2=""; else
	echo "FAIL: a daemon exited non-zero" >&2
	exit 1
fi
[ ! -e "$socket" ] || { echo "FAIL: socket left behind" >&2; exit 1; }
[ ! -e "$socket2" ] || { echo "FAIL: second socket left behind" >&2; exit 1; }

echo "smoke OK"