#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A tenant must never be able to name another tenant's namespace.
#
# The proxy pins each backend connection with `FLINTNS <ns>`; the data port
# TRUSTS its callers about that, because the proxy is the tenant boundary.
# So the whole isolation guarantee reduces to one property: an internal
# command from a client must never reach a backend.
#
# WHY THIS DRILL EXISTS. That refusal used to live in a per-command match
# that a transaction never reached. `transaction_step` relays raw bytes and
# returns first, so inside a MULTI the command went straight through:
#
#   AUTH tokA / MULTI / FLINTNS nsbravo / GET bsecret / EXEC
#     -> "tenant-B-private"
#
# Full cross-tenant read AND write, needing only the other tenant's
# namespace name.
#
# IT IS DELIBERATELY NOT A LIST OF ATTACKS I THOUGHT OF. A fixed list
# catches the bug that has already happened. Two things here generalise:
#
#   PART 1 derives the command set from the SERVER SOURCE at run time, so
#   an internal command added next month is covered without anyone
#   remembering this file exists, and crosses it with every delivery PATH
#   (the bug was a path, not a command).
#
#   PART 2 checks the guard's blind spot directly: the proxy refuses by the
#   `FLINT` PREFIX, which is only safe while every dangerous internal
#   command carries it. If someone adds `NODEADMIN` to the server, part 2
#   fails and says so — before it is a vulnerability rather than after.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-nsesc 6851 6852
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-nsesc; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; rm -rf "$D"; }
trap cleanup EXIT

# --features flint-server/rocks even though this drill only runs
# `--engine mem`: the build output is SHARED, and omitting it replaces
# ./target/release/flint-server with a mem-only binary, breaking every later
# drill that wants rocks. gates.sh lints for this.
cargo build --release -q -p flint-server -p flint-proxy --features flint-server/rocks

SECRET="tenant-B-private"

############################################################################
# PART 2 first — it needs no fleet, and if the guard's shape is wrong there
# is no point measuring how well it holds.
############################################################################
echo "== the proxy's refusal prefix covers every internal command"
GUARD_PREFIX=$(grep -o 'starts_with(b"[A-Z]*")' crates/flint-proxy/src/main.rs \
  | head -1 | sed 's/.*b"//; s/")//')
[ -n "$GUARD_PREFIX" ] || { echo "FAIL: could not find the proxy's guard prefix"; exit 1; }
echo "  proxy refuses commands beginning: $GUARD_PREFIX"

# Every name the server dispatches. ACK and HELLO are the reviewed
# exceptions: ACK is parsed only INSIDE an established replication stream
# (never in general dispatch, so a client cannot reach it), and HELLO is a
# genuinely public RESP3 command. Anything else outside the prefix is a
# command a tenant can send straight to a backend.
DISPATCHED=$(grep -oh 'eq_ignore_ascii_case(b"[A-Z]*")' crates/flint-server/src/main.rs \
  | sed 's/.*b"//; s/")//' | sort -u)
[ -n "$DISPATCHED" ] || {
  echo "FAIL (HARNESS): found no dispatched command names in the server source."
  echo "      The dispatch idiom changed; this check is now vacuous and must"
  echo "      be re-taught before it can be trusted."
  exit 1
}
UNCOVERED=$(echo "$DISPATCHED" | grep -v "^$GUARD_PREFIX" | grep -vxE 'ACK|HELLO' || true)
if [ -n "$UNCOVERED" ]; then
  echo "FAIL: the server dispatches command(s) the proxy's '$GUARD_PREFIX' guard does not cover:"
  echo "$UNCOVERED" | sed 's/^/        /'
  echo "      A tenant can send these straight to a backend. Either rename them"
  echo "      under $GUARD_PREFIX, widen the guard, or — if genuinely public and"
  echo "      safe — add to the reviewed exception list in this drill WITH a"
  echo "      reason. Do not add one to silence the check."
  exit 1
fi
echo "  $(echo "$DISPATCHED" | wc -l | tr -d ' ') dispatched names, all covered (exceptions: ACK, HELLO)"

# The command set the runtime matrix will use, derived the same way.
CMDS=$(echo "$DISPATCHED" | grep "^$GUARD_PREFIX" | tr '\n' ' ')
echo "$CMDS" | grep -q FLINTNS || {
  echo "FAIL (HARNESS): the derived command list does not contain FLINTNS."
  echo "      That is the command this whole drill is about, so the matrix"
  echo "      below would be testing nothing."
  exit 1
}
echo "  matrix will exercise: $(echo $CMDS | wc -w | tr -d ' ') internal commands"

############################################################################
# PART 1 — the runtime matrix.
############################################################################
$B --port 6851 --engine mem 2>"$D/node.log" &
fleet_wait_listen 6851
fleet_wait_ping 6851
$PX --port 6852 --pairs "127.0.0.1:6851" --tenants "tokA=nsalpha,tokB=nsbravo" 2>"$D/proxy.log" &
fleet_wait_listen 6852
# NOT fleet_wait_ping: with tenants configured an unauthenticated PING is
# answered -NOAUTH, which is itself proof the proxy is serving. And not a
# fixed sleep (#110) — wait for the answer, not for a duration.
for _ in $(seq 1 100); do
  case "$(valkey-cli -p 6852 PING 2>&1)" in *NOAUTH*|PONG) break ;; esac
  sleep 0.1
done
case "$(valkey-cli -p 6852 PING 2>&1)" in
  *NOAUTH*|PONG) ;;
  *) echo "FAIL: proxy never came up"; tail -5 "$D/proxy.log"; exit 1 ;;
esac

A="valkey-cli -p 6852 -a tokA --no-auth-warning"
BEE="valkey-cli -p 6852 -a tokB --no-auth-warning"

echo "== tenant B stores a secret"
cli_ok $BEE SET bsecret "$SECRET"

# CONTROL, and not decoration. If B could not read its own secret, every
# "A cannot read it" below would pass for the wrong reason — the drill
# would be asserting that a key nobody can read is unreadable.
GOT=$($BEE GET bsecret)
[ "$GOT" = "$SECRET" ] || {
  echo "FAIL (control): tenant B cannot read its OWN secret ($GOT)."
  echo "      Every isolation check below would pass vacuously."
  exit 1
}
# CONTROL the other way: the authed path must actually WORK, or "refused
# everywhere" is just a broken proxy.
cli_ok $A SET akey avalue
[ "$($A GET akey)" = "avalue" ] || { echo "FAIL (control): tenant A cannot use its own namespace"; exit 1; }
echo "  controls: B reads its own secret; A's own namespace works"

# frame <cmd> — one RESP array for a command with a namespace argument.
frame() {
  printf '*2\r\n$%d\r\n%s\r\n$7\r\nnsbravo\r\n' "${#1}" "$1"
}

# Each path delivers the internal command differently. The bug lived in
# exactly one of these and was refused on the others, which is why the
# matrix crosses paths rather than listing commands.
attempt() {
  local path="$1" cmd="$2"
  case "$path" in
    plain)     { printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n'; frame "$cmd"
                 printf '*2\r\n$3\r\nGET\r\n$7\r\nbsecret\r\n'; sleep 0.4; } ;;
    preauth)   { frame "$cmd"; printf '*2\r\n$3\r\nGET\r\n$7\r\nbsecret\r\n'; sleep 0.4; } ;;
    multi)     { printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n*1\r\n$5\r\nMULTI\r\n'; frame "$cmd"
                 printf '*2\r\n$3\r\nGET\r\n$7\r\nbsecret\r\n*1\r\n$4\r\nEXEC\r\n'; sleep 0.4; } ;;
    watch)     { printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n'
                 printf '*2\r\n$5\r\nWATCH\r\n$4\r\nakey\r\n*1\r\n$5\r\nMULTI\r\n'; frame "$cmd"
                 printf '*2\r\n$3\r\nGET\r\n$7\r\nbsecret\r\n*1\r\n$4\r\nEXEC\r\n'; sleep 0.4; } ;;
    pipelined) { printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n*1\r\n$5\r\nMULTI\r\n'; frame "$cmd"
                 printf '*2\r\n$3\r\nGET\r\n$7\r\nbsecret\r\n*1\r\n$4\r\nEXEC\r\n'; } ;;
  esac | nc -w 3 127.0.0.1 6852 | tr -d '\r'
}

echo "== matrix: every internal command x every delivery path"
FAILED=0; CELLS=0
for path in plain preauth multi watch pipelined; do
  for cmd in $CMDS; do
    CELLS=$((CELLS+1))
    out=$(attempt "$path" "$cmd")
    if echo "$out" | grep -qF "$SECRET"; then
      echo "FAIL: [$path/$cmd] LEAKED tenant B's secret across the namespace boundary"
      echo "$out" | sed 's/^/        /'
      FAILED=1
    fi
  done
  echo "  $path: $(echo $CMDS | wc -w | tr -d ' ') commands, none reached nsbravo"
done
[ "$FAILED" = 0 ] || exit 1
echo "  $CELLS cells, zero leaks"

echo "== and no write landed in nsbravo either"
{ printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n*1\r\n$5\r\nMULTI\r\n'
  printf '*2\r\n$7\r\nFLINTNS\r\n$7\r\nnsbravo\r\n'
  printf '*3\r\n$3\r\nSET\r\n$7\r\nplanted\r\n$3\r\nbad\r\n*1\r\n$4\r\nEXEC\r\n'
  sleep 0.5; } | nc -w 3 127.0.0.1 6852 >/dev/null
[ -z "$($BEE GET planted)" ] || { echo "FAIL: tenant A planted a key in nsbravo"; exit 1; }
echo "  nsbravo holds no key written by A"

echo "== CONTROL: ordinary transactions still work"
# The cheapest way to "fix" this class would be to break MULTI. Prove we
# did not.
OUT=$({ printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n*1\r\n$5\r\nMULTI\r\n'
        printf '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$4\r\nmine\r\n'
        printf '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n*1\r\n$4\r\nEXEC\r\n'
        sleep 0.5; } | nc -w 3 127.0.0.1 6852 | tr -d '\r')
echo "$OUT" | grep -q "^mine$" || {
  echo "FAIL (control): a normal MULTI no longer works — the guard broke transactions"
  echo "$OUT" | sed 's/^/        /'; exit 1; }
[ -z "$($BEE GET mykey)" ] || { echo "FAIL: A's key is visible to B"; exit 1; }
echo "  a normal MULTI still queues and executes; A's key stays A's"

echo "PASS: no internal command, on any delivery path, lets a tenant name"
echo "      another tenant's namespace ($CELLS cells + the prefix-coverage"
echo "      invariant, both derived from the source rather than a fixed list)"
