#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A CONTROLLED ROLL MUST NOT SHED WRITES — and until now nothing could tell.
#
# WHY THIS EXISTS. BUG-0035's open half. Rolling the playground to rc.59 on
# 2026-08-20 shed 210 writes on the demoted seat during a CONTROLLED failover,
# at shipped defaults, with the canary replica at 0 and nobody trying to
# provoke it. docs/slo.md's no-stall row says that cannot happen.
#
# That sighting matters more than the four laptop ones before it. It is not a
# synthetic firehose: it is the roll procedure this product runs on purpose,
# on a live fleet, at shipped defaults. It was also the FIRST time the number
# was countable at all — `writes_shed_lag` did not exist until 2026-08-20, so
# every earlier roll shed an unknown amount and reported nothing. "0 shed" in
# the historical record means NOT COUNTED, not none.
#
# THE COVERAGE GAP THIS CLOSES. `upgrade_drill.sh` rolls a fleet and asserts
# the build lands on every seat. It contains no reference to lag, to shed, or
# to any counter — checked, not assumed. So the only production defect this
# procedure has ever produced is invisible to the only drill that exercises
# the procedure, and a green gate has never once been evidence about it.
#
# WHAT IS ASSERTED, each a control rather than an observation:
#
#   1. The cap under test is the SHIPPED one. A roll measured against a cap
#      the product does not ship measures nothing an operator will meet, so
#      the value is pinned here and a default change must break this drill on
#      purpose rather than quietly retarget it.
#   2. NEGATIVE control: a roll under sustained write load sheds ZERO by the
#      LAG cause. This is slo.md's claim, restated as a test.
#   3. POSITIVE control: with a replica deliberately stalled behind a tight
#      cap, the master DOES shed, and sheds by the LAG cause specifically —
#      not the widowed gate, which refuses the same command for a different
#      reason. Without this, a green (2) is indistinguishable from a counter
#      wired to nothing, which is the exact failure BUG-0042 records for
#      controller_ha: four green runs that never once exercised the property
#      they asserted.
#   4. RECOVERABLE: restoring the cap restores writes. Backpressure that
#      cannot be released is an outage with a nicer message.
#
# WHY CONTROL 3 STALLS A REPLICA INSTEAD OF ROLLING AGAIN, and this is the
# most important comment in the file. The first version ramped the cap down —
# 200, 50, 10, 2, 1 — and re-rolled at each step, expecting the roll itself to
# manufacture lag. On a laptop that shed every time. On the 16-vCPU Linux gate
# box it shed NOTHING, even at a 1ms cap, and the drill failed on its own
# positive control.
#
# That failure was correct and worth keeping in mind. A roll only sheds while
# a replica is LIVE and BEHIND, and the width of that window is a property of
# the MACHINE, not of the product: a fast box restarts and re-syncs a replica
# faster than any lag can accumulate. So a control built on "roll and hope"
# arms on slow hardware and silently stops arming on fast hardware — where it
# would have gone green while testing nothing, in the same breath as
# asserting that control 2's zero was meaningful.
#
# SIGSTOP holds a replica live-but-not-acking for a bounded interval. That is
# the same condition a slow replica reaches, it does not require the machine
# to be slow, and it is released deterministically. The stall is kept well
# under `widowed_grace_ms`: past that the master sheds as WIDOWED, a different
# gate, and the control would arm the wrong counter while looking armed.
#
# CONTROL 2 IS STILL MACHINE-DEPENDENT, and honestly so. On a fast box a roll
# cannot build lag, so the zero it asserts is easy there and hard on a slow
# one. Its value is regression detection — if a roll ever starts shedding at
# the shipped cap, this goes red — and control 3 is what stops that zero from
# being mistaken for a measurement nobody took. The load rate is a knob
# (ROLL_SHED_BATCH, ROLL_SHED_GAP) because the open question in BUG-0035 is at
# WHICH rate a roll begins to shed.
#
# BOTH SEATS ARE READ, never just the master. The production shed was on the
# seat that was subsequently DEMOTED, and a roll swaps roles — so reading
# "the master" after the fact reads the wrong machine and would have missed
# the only sighting this drill exists to reproduce.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rollshed 6872 6873 7146 7846
fleet_guard

CTL=./target/release/flintctl
D=$FLINT_DRILL_ROOT/flint-rollshed; rm -rf "$D"; mkdir -p "$D"
A=6872; B=6873; PROXY=7846
SHIPPED_LAG_HARD_MS=1000   # docs/slo.md; see control 1
SHIPPED_LAG_SOFT_MS=500    # the soft cap the hard one is clamped against

cleanup() {
  touch "$D/stop" 2>/dev/null
  # A SIGSTOPped seat outlives the drill and would wedge the next one.
  [ -n "${STOPPED:-}" ] && fleet_signal_port "$STOPPED" -CONT 2>/dev/null
  [ -n "${WRITER_PID:-}" ] && kill "$WRITER_PID" 2>/dev/null
  $CTL -f "$D/cluster.flint" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  [ -n "${KEEP:-}" ] || rm -rf "$D"
}
trap cleanup EXIT
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }
cargo build --release -q -p flint-ctl -p flint-proxy -p flint-controlplane -p flint-controller \
  || { echo "FAIL: build"; exit 1; }

# NO lag flags here on purpose: control 1 asserts the seats came up on the
# shipped defaults, which is only meaningful if this file did not set them.
cat > "$D/cluster.flint" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7146
pair 127.0.0.1:$A,127.0.0.1:$B
proxy 127.0.0.1:$PROXY
controller on
EOF

echo "== bootstrap"
$CTL -f "$D/cluster.flint" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -8 "$D/bootstrap.log"; exit 1; }
$CTL -f "$D/cluster.flint" tenant add acme tok-acme acme 1 >/dev/null 2>&1 \
  || { echo "FAIL: tenant add"; exit 1; }

CLI=""
for c in valkey-cli redis-cli; do command -v "$c" >/dev/null 2>&1 && { CLI=$c; break; }; done
[ -n "$CLI" ] || { echo "SKIP: no valkey-cli or redis-cli"; exit 0; }

# THE SEATS SPEAK TLS; THE EDGE DOES NOT. `tls on` covers the internal mesh,
# so a plain valkey-cli reaches the proxy but never a pair port — the first
# run of this drill read an empty lag_hard_ms and blamed the default for
# moving. Same client-cert set flintctl's own mesh client uses.
[ -d "$D/state/certs" ] || { echo "FAIL: no mesh certs at $D/state/certs after bootstrap"; exit 1; }
mesh() {  # $1=port, rest=RESP args -> reply payload on stdout
  python3 - "$D/state/certs" "$@" <<'PY'
import socket, ssl, sys
certs, port, *args = sys.argv[1:]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.load_verify_locations(f"{certs}/ca.crt")
ctx.load_cert_chain(f"{certs}/int.crt", f"{certs}/int.key")
ctx.check_hostname = False
try:
    s = ctx.wrap_socket(socket.create_connection(("127.0.0.1", int(port)), timeout=5),
                        server_hostname="flint-internal")
    req = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        req += b"$%d\r\n" % len(b) + b + b"\r\n"
    s.sendall(req)
    f = s.makefile("rb")
    head = f.readline()
    if head[:1] == b"$":
        n = int(head[1:].strip())
        sys.stdout.write("" if n < 0 else f.read(n).decode(errors="replace"))
    else:
        sys.stdout.write(head.decode(errors="replace"))
except Exception as e:
    print(f"MESHERR {e}", file=sys.stderr)
PY
}
info_field() {  # $1=port $2=field
  mesh "$1" FLINTINFO 2>/dev/null | tr '\r' '\n' | grep "^$2:" | cut -d: -f2 | tr -d ' ' | head -1
}
shed_lag() { local v; v=$(info_field "$1" writes_shed_lag); echo "${v:-0}"; }
# The SOFT gate is reported beside the hard one because the pair is a graded
# response and only reading the hard end hides whether the grading works. Past
# lag_soft_ms every write sleeps 2ms (main.rs:2277); past lag_hard_ms it is
# refused. A hard shed with NO soft delays beneath it means the band was
# crossed faster than a flat 2ms step can matter — a different problem from
# "backpressure engaged and was not enough", and indistinguishable from it if
# only the shed is counted.
delayed_soft() { local v; v=$(info_field "$1" writes_delayed_soft); echo "${v:-0}"; }

# ---------------------------------------------------------------- control 1
echo "== control 1: the seats are on the SHIPPED lag cap"
for p in $A $B; do
  got=$(info_field $p lag_hard_ms)
  if [ "${got:-}" != "$SHIPPED_LAG_HARD_MS" ]; then
    echo "FAIL: seat :$p reports lag_hard_ms=${got:-(none)}, expected the shipped $SHIPPED_LAG_HARD_MS"
    echo "      Either this drill set a cap it should not have, or the shipped default moved."
    echo "      If the default moved, change SHIPPED_LAG_HARD_MS here and in docs/slo.md"
    echo "      together — a roll measured against a cap nobody ships proves nothing."
    exit 1
  fi
done
echo "  both seats: lag_hard_ms=$SHIPPED_LAG_HARD_MS"

# Sustained write pressure through the PROXY, because that is the path a
# client takes and the one that chases a promotion. Individual writes are
# allowed to fail during the roll — the proxy restarts too — which is why the
# loop swallows errors. What must not happen is a seat REFUSING a write with
# the lag cause, and that is read from the seats, not from here.
# THE LOAD IS PACED, AND THAT IS THE WHOLE DIFFICULTY OF THIS DRILL.
#
# An unpaced `--pipe` firehose sheds by construction: BUG-0035 records that a
# replay at firehose rate recreates the very lag that caused the shed, and
# never converges. A first cut of this drill did exactly that and reported
# 20651 shed at the shipped cap — a true number about a load slo.md never
# claims anything about, and therefore not evidence for or against the claim.
#
# The production sighting was ORDINARY playground traffic: 210 writes. So the
# rate here is paced to something an operator would recognise, and it is a
# knob rather than a constant because the interesting question is at WHICH
# rate a roll starts shedding. BATCH writes then sleep GAP: ~200 writes/sec by
# default, which is modest for this product and well inside what one seat
# absorbs when nothing is rolling.
BATCH="${ROLL_SHED_BATCH:-50}"
GAP="${ROLL_SHED_GAP:-0.25}"
_gen() {
  awk -v s="$1" -v n="$BATCH" 'BEGIN{for(i=0;i<n;i++){k=sprintf("roll:%s:%06d",s,i);v=sprintf("v-%06d",i);
    printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}'
}
# DELIVERED THROUGHPUT IS MEASURED, not inferred from BATCH and GAP.
#
# Two earlier conclusions in this investigation rested on a rate that was
# computed rather than observed. A sweep labelled its axis BATCH/GAP and was
# read as a rate sweep, when each cycle also pays a process spawn, a TCP
# connect and an auth that the arithmetic ignores. And "Linux does not shed at
# any burst" could not be separated from "this client cannot push hard enough
# to make it", because nobody knew what the client actually delivered.
#
# `--pipe` reports `replies: N` per invocation. Summing those over a measured
# wall-clock interval gives the number both claims needed and neither had.
_now_s() { python3 -c 'import time; print(time.time())'; }
writer_start() {
  rm -f "$D/stop" "$D/replies"
  _now_s > "$D/wstart"
  ( n=0
    while [ ! -f "$D/stop" ]; do
      n=$((n+1))
      _gen "$1-$n" | $CLI -p $PROXY -a tok-acme --no-auth-warning --pipe 2>/dev/null \
        | sed -nE 's/.*replies: ([0-9]+).*/\1/p' >> "$D/replies" || true
      [ "$GAP" = "0" ] || sleep "$GAP"
    done ) &
  WRITER_PID=$!
}
writer_stop() {
  touch "$D/stop"; wait "$WRITER_PID" 2>/dev/null; WRITER_PID=""
  local end start delivered
  end=$(_now_s); start=$(cat "$D/wstart" 2>/dev/null || echo "$end")
  delivered=$(awk '{s+=$1} END {print s+0}' "$D/replies" 2>/dev/null)
  DELIVERED=$delivered
  RATE_OBS=$(awk -v d="$delivered" -v e="$end" -v s="$start" \
    'BEGIN { t = e - s; if (t <= 0) print "n/a"; else printf "%.0f", d / t }')
}

roll_under_load() {  # $1=tag $2=load-prefix -> echoes the lag-shed delta
  local tag="$1" pre_a pre_b post_a post_b pre_sa pre_sb post_sa post_sb
  pre_a=$(shed_lag $A); pre_b=$(shed_lag $B)
  pre_sa=$(delayed_soft $A); pre_sb=$(delayed_soft $B)
  writer_start "$2"
  sleep 2   # let the firehose actually build lag before the roll starts
  $CTL -f "$D/cluster.flint" upgrade --version-tag "$tag" --soak-ms 1500 \
    >"$D/upgrade-$tag.log" 2>&1 || { writer_stop; echo "FAIL: upgrade $tag exited non-zero" >&2
      tail -15 "$D/upgrade-$tag.log" >&2; exit 1; }
  writer_stop
  sleep 1
  post_a=$(shed_lag $A); post_b=$(shed_lag $B)
  post_sa=$(delayed_soft $A); post_sb=$(delayed_soft $B)
  echo "  seat :$A writes_shed_lag ${pre_a} -> ${post_a} | seat :$B ${pre_b} -> ${post_b}" >&2
  echo "  soft delays across the roll: :$A +$(( post_sa - pre_sa )) | :$B +$(( post_sb - pre_sb ))" >&2
  echo "  load actually DELIVERED: ${DELIVERED:-?} writes at ${RATE_OBS:-?}/s observed (batch=$BATCH gap=$GAP)" >&2
  echo $(( (post_a - pre_a) + (post_b - pre_b) ))
}

# ---------------------------------------------------------------- control 2
echo "== control 2: a roll at the shipped cap, under load, sheds nothing by lag"
DELTA=$(roll_under_load roll-a shipped)
if [ "$DELTA" -ne 0 ]; then
  echo "FAIL: a CONTROLLED roll shed $DELTA write(s) with the lag cause at the shipped"
  echo "      ${SHIPPED_LAG_HARD_MS}ms cap. docs/slo.md's no-stall row says this cannot"
  echo "      happen. This is BUG-0035's production sighting, reproduced."
  exit 1
fi
echo "  0 shed by lag across the roll"

# ---------------------------------------------------------------- control 3
# THE CONDITION IS FORCED, NOT HOPED FOR.
#
# A roll only sheds while a replica is LIVE and BEHIND, and how long that
# window lasts is a property of the machine, not of the product. The first
# version of this control ramped the cap down to 1ms and rolled again at each
# step: on a slow laptop that shed every time, and on the 16-vCPU Linux gate
# box it shed NOTHING even at 1ms, because the replica there restarts and
# catches up faster than any lag can accumulate. The drill failed, correctly —
# a control that cannot arm makes control 2's zero worthless, and saying so is
# the whole reason it exists.
#
# SIGSTOP holds the replica live-but-not-acking for a bounded interval. That
# is the same condition a slow replica reaches, it does not depend on the
# machine being slow, and it is released deterministically. The stall is kept
# well under `widowed_grace_ms` on purpose: past that the master sheds as
# WIDOWED instead, which is a different gate and would arm the wrong counter.
echo "== control 3: the lag gate and its counter are live on this machine"
MASTER=""; REPLICA=""
for p in $A $B; do
  if [ "$(info_field $p role)" = "master" ]; then MASTER=$p; else REPLICA=$p; fi
done
[ -n "$MASTER" ] && [ -n "$REPLICA" ] \
  || { echo "FAIL: could not identify master/replica after the roll (A=$(info_field $A role) B=$(info_field $B role))"; exit 1; }
echo "  master :$MASTER, replica :$REPLICA"

WG=$(info_field $MASTER widowed_grace_ms); WG=${WG:-5000}
STALL_MS=$(( WG / 3 )); [ "$STALL_MS" -gt 1500 ] && STALL_MS=1500
[ "$STALL_MS" -lt 300 ] && STALL_MS=300
# Overridable so the soft..hard band can be widened for a soft-gate study
# without editing the drill. Defaults keep the band narrow, which is right for
# arming the HARD gate quickly; a wide band is a different experiment and says
# so at the call site.
TIGHT="${ROLL_SHED_TIGHT:-50}"
TIGHT_SOFT="${ROLL_SHED_TIGHT_SOFT:-$(( TIGHT / 2 ))}"
[ "$STALL_MS" -gt "$TIGHT" ] \
  || { echo "FAIL: stall ${STALL_MS}ms is not longer than the ${TIGHT}ms cap; the control cannot arm"; exit 1; }
# Print the variable that is APPLIED, never a second computation of it. This
# line recomputed TIGHT/2 and so reported 700ms soft on a run that applied
# 25ms from the environment — a log disagreeing with the config it describes,
# in the one line an operator would trust to tell them what was tested.
echo "  widowed_grace_ms=$WG, stalling the replica for ${STALL_MS}ms against a ${TIGHT}ms hard cap (${TIGHT_SOFT}ms soft)"

# THE BAND MUST HAVE WIDTH OR THE SOFT GATE IS DEAD CODE.
#
# These were both set to $TIGHT, which collapsed soft..hard to zero width. The
# write path checks hard FIRST and returns (main.rs:2271), so with soft == hard
# the `lag >= lag_soft_ms` branch is unreachable and writes_delayed_soft can
# only ever read 0. The drill then printed that 0 next to a hard-shed count and
# a note reasoning about why the soft gate had not helped — a statement about
# the product, produced by a setting of this script.
#
# Half the hard cap gives the soft gate a real band to act in, so the number
# beside the shed means what it appears to mean.
mesh $MASTER FLINTCONFIG lag-soft-ms $TIGHT_SOFT >/dev/null 2>&1
mesh $MASTER FLINTCONFIG lag-hard-ms $TIGHT >/dev/null 2>&1
gots=$(info_field $MASTER lag_soft_ms)
[ "$gots" = "$TIGHT_SOFT" ] \
  || { echo "FAIL: lag-soft-ms $TIGHT_SOFT did not take (seat reports ${gots:-none})"; exit 1; }
got=$(info_field $MASTER lag_hard_ms)
[ "$got" = "$TIGHT" ] \
  || { echo "FAIL: FLINTCONFIG lag-hard-ms $TIGHT did not take (seat reports ${got:-none}) — see BUG-0043"; exit 1; }

PRE_LAG=$(shed_lag $MASTER)
PRE_SOFT=$(delayed_soft $MASTER)
PRE_WID=$(info_field $MASTER writes_shed_widowed); PRE_WID=${PRE_WID:-0}
fleet_signal_port $REPLICA -STOP || { echo "FAIL: could not SIGSTOP the replica on :$REPLICA"; exit 1; }
STOPPED=$REPLICA
writer_start forcelag
sleep "$(awk -v m=$STALL_MS 'BEGIN{print m/1000}')"
writer_stop
fleet_signal_port $STOPPED -CONT; STOPPED=""
sleep 1
POST_LAG=$(shed_lag $MASTER)
POST_WID=$(info_field $MASTER writes_shed_widowed); POST_WID=${POST_WID:-0}
POST_SOFT=$(delayed_soft $MASTER)
D_LAG=$(( POST_LAG - PRE_LAG )); D_WID=$(( POST_WID - PRE_WID ))
D_SOFT=$(( POST_SOFT - PRE_SOFT ))
echo "  writes_shed_lag ${PRE_LAG} -> ${POST_LAG} (+$D_LAG) | writes_delayed_soft +$D_SOFT | writes_shed_widowed +$D_WID"
# Reported, not asserted. The soft gate is a flat 2ms step across the whole
# soft..hard band, so how many writes land in it depends on how fast the band
# is crossed — a real property, but not one with a threshold worth failing on
# until it has been characterised on more than one machine.
#
# This only means anything because TIGHT_SOFT < TIGHT above. When the two were
# equal the count was structurally zero and the note below was a conclusion
# about the product drawn from a setting of this script.
if [ "$D_SOFT" -eq 0 ]; then
  echo "  NOTE: the hard gate fired with NO soft delays beneath it, across a real"
  echo "        ${TIGHT_SOFT}..${TIGHT}ms band — the band was crossed faster than a flat 2ms step"
  echo "        could act. Worth knowing before anyone tunes the soft cap expecting"
  echo "        it to smooth this."
else
  echo "  the soft gate engaged $D_SOFT time(s) before the hard gate refused — the"
  echo "        graded response is doing something, not merely present"
fi
if [ "$D_LAG" -le 0 ]; then
  echo "FAIL: with the replica stalled ${STALL_MS}ms behind a ${TIGHT}ms cap, the master shed"
  echo "      NOTHING by the lag cause. That is not a pass — it means control 2 above proves"
  echo "      nothing, because the counter it reads never moves. Either writes_shed_lag is not"
  echo "      wired to the write path, or the stall never reached the master's view of lag."
  [ "$D_WID" -gt 0 ] && echo "      NOTE: writes_shed_widowed moved by $D_WID — the stall outran widowed_grace_ms=$WG"
  echo "      and armed the WRONG gate. Shorten the stall or raise the grace."
  exit 1
fi
echo "  $D_LAG write(s) shed by LAG — the gate and its counter are live"

# ---------------------------------------------------------------- control 4
echo "== control 4: restoring the cap restores writes"
# HARD FIRST ON THE WAY UP, soft first on the way down. The pair must stay
# coherent (`soft <= hard`) and only one end moves per command, so the order
# reverses with the direction. Since BUG-0043 the server REFUSES the
# incoherent order instead of silently fixing it, which is why this is
# written out rather than left to luck.
for p in $A $B; do
  mesh $p FLINTCONFIG lag-hard-ms $SHIPPED_LAG_HARD_MS >/dev/null 2>&1
  mesh $p FLINTCONFIG lag-soft-ms $SHIPPED_LAG_SOFT_MS >/dev/null 2>&1
done
sleep 1
for p in $A $B; do
  got=$(info_field $p lag_hard_ms)
  [ "$got" = "$SHIPPED_LAG_HARD_MS" ] \
    || { echo "FAIL: seat :$p did not return to the shipped cap (reports ${got:-none})"; exit 1; }
done
OUT=$($CLI -p $PROXY -a tok-acme --no-auth-warning SET after-restore ok 2>&1 | tr -d '\r')
[ "$OUT" = "OK" ] || { echo "FAIL: after restoring the cap a write still failed: ${OUT:-(no reply)}"; exit 1; }
echo "  writes accepted again at the shipped cap"

# ---------------------------------------------------------------- control 5
# BUG-0043 REGRESSION. The knob used to arm control 3 must report what it
# applied. It used to accept a hard cap below the soft one, answer OK, and
# store the soft value — so a ramp tested one threshold while reporting five.
echo "== control 5: an incoherent lag pair is REFUSED, not silently clamped"
BAD=$(( SHIPPED_LAG_SOFT_MS - 1 ))
REPLY=$(mesh $A FLINTCONFIG lag-hard-ms $BAD 2>&1 | tr -d '\r')
case "$REPLY" in
  *ERR*lag-soft-ms*) echo "  refused as it should: ${REPLY%%$'\n'*}" ;;
  *) echo "FAIL: FLINTCONFIG lag-hard-ms $BAD (below the ${SHIPPED_LAG_SOFT_MS}ms soft cap)"
     echo "      answered ${REPLY:-(nothing)} instead of refusing. BUG-0043 has regressed:"
     echo "      the knob reports success and applies a different number, which makes every"
     echo "      threshold ramp built on it test one value while reporting several."
     exit 1 ;;
esac
NOW=$(info_field $A lag_hard_ms)
[ "$NOW" = "$SHIPPED_LAG_HARD_MS" ] \
  || { echo "FAIL: the refused set still moved the cap to ${NOW:-none}"; exit 1; }
echo "  and the cap is unchanged at ${NOW}ms"

echo "PASS: a controlled roll under sustained load sheds nothing by the lag cause at the"
echo "      shipped ${SHIPPED_LAG_HARD_MS}ms cap, and on this same machine a stalled replica"
echo "      behind a ${TIGHT}ms cap sheds $D_LAG — so the zero above was measured, not merely"
echo "      unobserved."
