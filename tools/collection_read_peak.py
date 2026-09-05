#!/usr/bin/env python3
# SPDX-License-Identifier: Elastic-2.0
#
# Measure the peak memory a single collection read costs, as a multiple of the
# collection's own size. BUG-0060's admission fix refuses a read that would push
# the node past a memory budget, and the only thing knowable BEFORE serving is
# `ComplexMeta.bytes` -- so the whole design rests on `peak = k x bytes`, and k
# has to come from a measurement rather than an estimate.
#
# THREE THINGS THIS GETS RIGHT THAT AN AD-HOC SCRIPT DID NOT (all cost a day):
#
#   1. ONE FRESH SERVER PER TRIAL. A harness that sweeps sizes in one process
#      measures each size against a base the previous read already warmed. RSS
#      does not fall back, so the bias grows with size -- which is exactly the
#      direction that flattens a climbing curve. The contaminated sweep reported
#      "spread 6%, k is flat"; k actually climbs 2.19 -> 3.15.
#
#   2. THE DENOMINATOR IS ComplexMeta.bytes, which counts `field.len() +
#      value.len()`, not the payload alone. Dividing by the payload understates
#      k, and admission divides by this quantity.
#
#   3. UNITS. `ps -o rss=` reports KiB. Dividing that by 1024 and then by a
#      denominator computed in decimal MB understates every ratio by 4.9%.
#
# AND THE INSTRUMENT CAN LIE. An RSS peak is a LOWER BOUND on demand: under
# macOS memory compression, pages leave the resident set and the peak reads low
# -- two trials of one shape disagreed by 40% until the compressor was sampled.
# So every trial records whether the host was reclaiming, and results are
# reported as the MAX across trials, never the median: compression can only
# lower an observed peak, so the largest trial is the best estimate.
#
# TYPES. `--type hash|set|zset`, because the multiplier is a RATIO and each
# type divides by a different denominator: a hash counts field+value, a set
# counts members only (they are stored as keys with empty values), and a zset
# counts member+8 for the score. Measuring one type and applying its k to the
# others is an approximation, and this exists so it does not have to be.
#
# Usage:
#   tools/collection_read_peak.py --bin target/release/flint-server
#   tools/collection_read_peak.py --type zset --shapes 131072x4096
#
# The server must be built with --features rocks; the harness asserts it can
# actually serve before it measures, so a mem-only binary fails loudly.
import argparse, os, platform, shutil, statistics, subprocess, sys, threading, time

FIELD_FMT = "f%07d"          # fixed width so field bytes are exactly known
BUILD = {"hash": "HSET", "set": "SADD", "zset": "ZADD"}
LEN = {"hash": "HLEN", "set": "SCARD", "zset": "ZCARD"}
READ = {"hash": ("HGETALL", "h"), "set": ("SMEMBERS", "h"), "zset": ("ZRANGE", "h", "0", "-1")}


def sh(*a):
    return subprocess.run(a, capture_output=True, text=True).stdout


class Reclaim:
    """Whether the host was reclaiming memory around a measurement.

    Linux has no compressor, so a faithful RSS is the normal case there and the
    analogue is swap-out. Either way the point is the same: a peak taken while
    the host was reclaiming is an UNDERESTIMATE, and must say so rather than be
    averaged in with clean ones.
    """

    def __init__(self):
        self.mac = platform.system() == "Darwin"

    def sample(self):
        if self.mac:
            out = sh("vm_stat")
            pg = int(sh("sysctl", "-n", "hw.pagesize") or 16384)
            for line in out.splitlines():
                if "occupied by compressor" in line:
                    return int(line.split()[-1].rstrip(".")) * pg / 2**30
            return 0.0
        try:
            with open("/proc/vmstat") as f:
                for line in f:
                    if line.startswith("pswpout "):
                        return int(line.split()[1]) * 4096 / 2**30
        except OSError:
            return None          # cannot look != nothing happened
        return 0.0

    def label(self):
        return "compressor" if self.mac else "swap-out"


def rss_bytes(pid):
    out = sh("ps", "-o", "rss=", "-p", str(pid)).strip()
    return int(out) * 1024 if out else 0


def build_and_measure(binary, port, root, n_fields, vsize, reclaim, ctype="hash"):
    """One trial in its OWN server. Returns (bytes, peak_delta, reclaim_delta)."""
    d = os.path.join(root, "collection-read-peak-%d" % port)
    shutil.rmtree(d, ignore_errors=True)
    srv = subprocess.Popen(
        [binary, "--port", str(port), "--engine", "rocks", "--data-dir", d],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def cli(*a):
        return subprocess.run(["valkey-cli", "-p", str(port)] + list(a),
                              capture_output=True, text=True).stdout.strip()

    try:
        for _ in range(60):
            if cli("PING") == "PONG":
                break
            time.sleep(0.5)
        else:
            raise SystemExit("no PONG on %d -- built without --features rocks?" % port)

        payload = "x" * vsize
        per_call = max(1, 1_000_000 // max(vsize, 1))
        den, args = 0, []
        # Each branch mirrors ITS OWN accumulator. Getting this wrong does not
        # fail loudly -- it silently scales the ratio.
        for i in range(n_fields):
            f = FIELD_FMT % i
            if ctype == "hash":
                args += [f, payload]
                den += len(f) + vsize
            elif ctype == "set":
                m = f + payload                    # unique: a set deduplicates
                args.append(m)
                den += len(m)
            else:                                  # zset
                m = f + payload
                args += [str(i), m]
                den += len(m) + 8                  # member_cost: member + score
            if len(args) >= (per_call if ctype == "set" else 2 * per_call):
                cli(BUILD[ctype], "h", *args)
                args = []
        if args:
            cli(BUILD[ctype], "h", *args)

        got_len = cli(LEN[ctype], "h")
        if got_len != str(n_fields):
            raise SystemExit("%s %s != %d -- the collection was not built (a set or "
                             "zset silently dedupes, so this is the assert that "
                             "catches a repeating member pattern)"
                             % (LEN[ctype], got_len, n_fields))

        time.sleep(1)
        rc_before = reclaim.sample()
        base = rss_bytes(srv.pid)
        peak = [base]
        stop = threading.Event()

        def sampler():
            while not stop.is_set():
                peak[0] = max(peak[0], rss_bytes(srv.pid))
                time.sleep(0.005)

        t = threading.Thread(target=sampler)
        t.start()
        got = cli(*READ[ctype])
        stop.set()
        t.join()
        rc_after = reclaim.sample()

        if len(got) < vsize:
            raise SystemExit("%s returned %d bytes -- the read did not happen"
                             % (READ[ctype][0], len(got)))
        rc = None if (rc_before is None or rc_after is None) else rc_after - rc_before
        return den, peak[0] - base, rc
    finally:
        srv.terminate()
        srv.wait(timeout=10)
        shutil.rmtree(d, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="target/release/flint-server")
    ap.add_argument("--port", type=int, default=6499)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--root", default=os.environ.get("FLINT_DRILL_ROOT", "/tmp"))
    ap.add_argument("--type", default="hash", choices=("hash", "set", "zset"))
    ap.add_argument("--shapes", default="500x100000,1000x100000,2000x100000,"
                                        "4000x100000,5368x100000,53644x10000,"
                                        "532610x1000",
                    help="comma-separated FIELDSxVALUEBYTES")
    a = ap.parse_args()

    if not os.access(a.bin, os.X_OK):
        raise SystemExit("no executable at %s -- build with --features rocks first" % a.bin)
    reclaim = Reclaim()
    print("host: %s %s | type: %s | reclaim signal: %s | reps: %d"
          % (platform.system(), platform.machine(), a.type, reclaim.label(), a.reps))
    print("%-9s %-10s %-13s %-15s %-8s %s"
          % ("fields", "value B", "bytes(den)", "peak delta B", "ratio", reclaim.label()))

    rows = []
    for shape in a.shapes.split(","):
        n_fields, vsize = (int(x) for x in shape.lower().split("x"))
        ks, dirty = [], 0
        for _ in range(a.reps):
            den, delta, rc = build_and_measure(a.bin, a.port, a.root, n_fields,
                                               vsize, reclaim, a.type)
            k = delta / den
            ks.append(k)
            if rc is None:
                note, dirty = "UNREADABLE", dirty + 1
            elif rc > 0.01:
                note, dirty = "%+.2f GiB  <-- underestimate" % rc, dirty + 1
            else:
                note = "%+.2f GiB" % rc
            print("%-9d %-10d %-13d %-15d %-8.3f %s"
                  % (n_fields, vsize, den, delta, k, note))
        rows.append((n_fields, vsize, max(ks), dirty))

    print()
    print("k per shape (MAX across reps -- an RSS peak is a lower bound, so the")
    print("largest trial is the best estimate; a median would understate it):")
    for n_fields, vsize, k, dirty in rows:
        flag = "   [%d/%d trial(s) ran while the host reclaimed]" % (dirty, a.reps) if dirty else ""
        print("  %8d fields x %-7d B  k=%.3f%s" % (n_fields, vsize, k, flag))
    kmax = max(r[2] for r in rows)
    print()
    print("  MAXIMUM OBSERVED k = %.3f" % kmax)
    print("  Admission multiplier must exceed this: k rises with both collection")
    print("  size and field count before saturating, so a mid-range sample is not")
    print("  a bound. Every figure here is a floor on true demand.")


if __name__ == "__main__":
    main()
