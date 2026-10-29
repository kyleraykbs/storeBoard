#!/usr/bin/env bash
# Verify one streamed store against its holder, on real hosts.
#
# Run this ON the machine that carries the store streamed. It exercises the
# four paths that matter before any irreplaceable data is moved:
#
#   1. read   - a file written on the holder is readable through the mount
#   2. write  - a file written through the mount reaches the holder
#   3. offline- with the holder's daemon stopped, a write is accepted and queued
#   4. flush  - when the holder returns, the queued write lands unchanged
#
# Every comparison is by sha256 on both sides, so "it synced" is a fact and not
# a guess. Test files are named verify-*.txt and deleted at the end (which also
# exercises delete propagation when a holder is reachable).
#
# Usage:
#   scripts/verify-streamed.sh --store scratch --holder-root /mainpool/storeboard \
#       --holder-ssh amanita [--allow-restart]
#
#   --store NAME        the store id, as declared in `stores`
#   --holder-root PATH  the holder's `root` (so <root>/<store> is its copy)
#   --holder-ssh HOST   ssh target for the holder; omit when running the
#                       holder on this machine (tests/dev)
#   --allow-restart     run the offline phase; it stops the holder's daemon,
#                       which pauses syncing for every store on that host
#   --holder-stop CMD   how to stop the holder's daemon (default:
#                       sudo systemctl stop storeboard, on the holder)
#   --holder-start CMD  how to start it again (default: sudo systemctl start
#                       storeboard)
#   --cli PATH          storeboard binary (default: storeboard on PATH)
#   --config PATH       config to use (default: $STOREBOARD_CONFIG)
set -euo pipefail

store="scratch"
holder_ssh=""
holder_root=""
allow_restart=0
holder_stop_cmd=""
holder_start_cmd=""
cli="storeboard"
config="${STOREBOARD_CONFIG:-}"

die() { printf 'verify-streamed: %s\n' "$*" >&2; exit 1; }
say() { printf '  %s\n' "$*"; }

while [ $# -gt 0 ]; do
	case "$1" in
	--store) store="$2"; shift 2 ;;
	--holder-ssh) holder_ssh="$2"; shift 2 ;;
	--holder-root) holder_root="$2"; shift 2 ;;
	--allow-restart) allow_restart=1; shift ;;
	--holder-stop) holder_stop_cmd="$2"; shift 2 ;;
	--holder-start) holder_start_cmd="$2"; shift 2 ;;
	--cli) cli="$2"; shift 2 ;;
	--config) config="$2"; shift 2 ;;
	-h|--help) sed -n '2,30p' "$0"; exit 0 ;;
	*) die "unknown argument: $1" ;;
	esac
done

[ -n "$holder_root" ] || die "--holder-root is required"
command -v jq >/dev/null || die "jq is required"
[ -x "$cli" ] || command -v "$cli" >/dev/null || die "storeboard binary not found"
[ -n "$config" ] && export STOREBOARD_CONFIG="$config"

# holder runs a command on the holder and returns its stdout. With no --holder-ssh
# the holder is this machine, which is how the script is tested.
holder() {
	if [ -n "$holder_ssh" ]; then
		ssh -o BatchMode=yes "$holder_ssh" "$@"
	else
		bash -c "$*"
	fi
}

holder_stop() {
	local cmd="${holder_stop_cmd:-sudo systemctl stop storeboard}"
	holder "$cmd"
}

holder_start() {
	local cmd="${holder_start_cmd:-sudo systemctl start storeboard}"
	holder "$cmd"
}

sum()  { sha256sum "$1" | cut -d' ' -f1; }
hsum() { holder "sha256sum '$1' | cut -d' ' -f1"; }

# wait_for NAME TIMEOUT_S TEST... - polls a test command until it succeeds.
wait_for() {
	local what="$1" limit="$2"
	shift 2
	local waited=0
	while ! "$@" >/dev/null 2>&1; do
		sleep 1
		waited=$((waited + 1))
		if [ "$waited" -ge "$limit" ]; then
			die "timed out after ${limit}s waiting for $what"
		fi
	done
}

echo "== storeboard streamed store: $store =="

status="$("$cli" status --json)" || die "cannot reach the daemon (is it running?)"
root="$(jq -r --arg s "$store" '.sync[] | select(.store == $s) | .root' <<<"$status")"
state="$(jq -r --arg s "$store" '.sync[] | select(.store == $s) | .state' <<<"$status")"
[ -n "$root" ] && [ "$root" != "null" ] || die "store $store is not in the daemon's status"
mountpoint -q "$root" || die "$root is not a mount: store $store is not streamed here"
[ "$state" = "attached" ] || die "store $store is $state: no holder reachable"
say "mount  $root (attached)"
holder "test -d '$holder_root/$store'" || die "holder has no $holder_root/$store"

# --- 1. read: the holder's bytes arrive through the mount ------------------
name="verify-read-$$.txt"
printf 'read path %s\n' "$(date -Is)" > "/tmp/$name"
holder "cp '/tmp/$name' '$holder_root/$store/$name' && rm '/tmp/$name'"
wait_for "the entry to appear in the mount" 120 test -f "$root/$name"
[ "$(sum "$root/$name")" = "$(hsum "$holder_root/$store/$name")" ] ||
	die "content differs between the mount and the holder"
say "read   ok (sha256 matches the holder)"

# --- 2. write: the mount's bytes reach the holder --------------------------
name="verify-write-$$.txt"
printf 'write path %s\n' "$(date -Is)" > "$root/$name"
wait_for "the write to reach the holder" 120 holder "test -f '$holder_root/$store/$name'"
[ "$(sum "$root/$name")" = "$(hsum "$holder_root/$store/$name")" ] ||
	die "the holder's copy of $name differs from what was written"
say "write  ok (holder has the exact bytes)"

# --- 3/4. offline queue, then flush ---------------------------------------
if [ "$allow_restart" = 1 ]; then
	name="verify-offline-$$.txt"
	say "offline stopping the holder's daemon..."
	holder_stop
	sleep 2
	printf 'offline path %s\n' "$(date -Is)" > "$root/$name" ||
		die "a write was refused while the holder was down"
	if holder "test -f '$holder_root/$store/$name'" 2>/dev/null; then
		die "the holder had $name while its daemon was stopped"
	fi
	say "queued $name (visible through the mount, absent on the holder)"
	say "offline starting the holder's daemon..."
	holder_start
	wait_for "the queued write to flush" 180 holder "test -f '$holder_root/$store/$name'"
	[ "$(sum "$root/$name")" = "$(hsum "$holder_root/$store/$name")" ] ||
		die "the flushed copy of $name differs"
	say "flush  ok (queued write landed after reconnect)"
else
	say "offline skipped (pass --allow-restart to stop the holder's daemon)"
fi

# --- cleanup --------------------------------------------------------------
for f in verify-read-$$.txt verify-write-$$.txt "verify-offline-$$.txt"; do
	rm -f "$root/$f"
done
wait_for "the test files to leave the holder" 120 \
	holder "test ! -e '$holder_root/$store/verify-read-$$.txt' && test ! -e '$holder_root/$store/verify-write-$$.txt'"

echo "== pass: reads, writes and deletes verified against $holder_root/$store =="