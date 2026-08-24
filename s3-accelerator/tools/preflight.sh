#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Answer, on YOUR cluster and before you change anything: can flint-accel be
# adopted here, and with which configuration?
#
# Refusing to start on a bad classpath is a backstop, not a plan. This runs
# first, needs no JVM and installs nothing -- it reads the jars you already
# have. Point it at a Spark jars directory, or let it use `hadoop classpath`.
#
#   tools/preflight.sh                       # uses `hadoop classpath`
#   tools/preflight.sh /opt/spark/jars       # or a directory of jars
#   tools/preflight.sh "$(hadoop classpath)" # or an explicit classpath
set -uo pipefail

RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; DIM=$'\033[2m'; OFF=$'\033[0m'
say() { printf "%s%s%s\n" "${2:-}" "$1" "$OFF"; }

# ---- self-test ------------------------------------------------------------
# A warning nobody has watched fire is not a check. These run the version
# predicates against synthetic classpaths, with the negative controls that
# distinguish "correctly silent" from "dead".
if [ "${1:-}" = "--self-test" ]; then
  P=0; F=0
  t() { # description, expected-hits, classpath
    n=$("$0" "$3" 2>&1 | grep -c "$4")
    if [ "$n" = "$2" ]; then P=$((P+1)); printf "[ok] %s\n" "$1"
    else F=$((F+1)); printf "[FAIL] %s (matched %s, want %s)\n" "$1" "$n" "$2"; fi
  }
  t "iceberg 1.9 + avro 1.11.4 WARNS"        1 "/x/iceberg-core-1.9.0.jar:/x/avro-1.11.4.jar" "WARN.*iceberg"
  t "negative control: avro 1.12 is silent"  0 "/x/iceberg-core-1.9.0.jar:/x/avro-1.12.0.jar" "WARN.*iceberg"
  t "negative control: iceberg 1.8 is silent" 0 "/x/iceberg-core-1.8.1.jar:/x/avro-1.11.4.jar" "WARN.*iceberg"
  t "negative control: no avro at all is silent" 0 "/x/iceberg-core-1.9.0.jar" "WARN.*iceberg"
  printf -- "--- %d passed, %d failed\n" "$P" "$F"
  [ "$F" -eq 0 ]; exit $?
fi

# ---- locate jars ----------------------------------------------------------
ARG="${1:-}"
JARS=""
if [ -z "$ARG" ]; then
  command -v hadoop >/dev/null 2>&1 && JARS=$(hadoop classpath 2>/dev/null)
  [ -z "$JARS" ] && { say "No argument and no 'hadoop' on PATH." "$RED"
    echo "Usage: $0 [<spark-jars-dir> | <classpath>]"; exit 2; }
elif [ -d "$ARG" ]; then
  JARS=$(find "$ARG" -name '*.jar' | paste -sd: -)
else
  JARS="$ARG"
fi

find_jar() { tr ':' '\n' <<<"$JARS" | grep -E "/$1[^/]*\.jar$" | head -1; }
ver_of()  { basename "$1" .jar | sed -E "s/^$2-//"; }

HADOOP_AWS=$(find_jar "hadoop-aws")
AAL=$(find_jar "analyticsaccelerator-s3")
ICEBERG=$(find_jar "iceberg-aws")
ICEBERG_RT=$(find_jar "iceberg-spark-runtime")
ICEBERG_CORE=$(find_jar "iceberg-core")
AVRO=$(find_jar "avro")

echo
say "flint-accel preflight" "$DIM"
echo "----------------------------------------------------------------"

# ---- hadoop-aws -----------------------------------------------------------
S3A_OK=no; NEEDS_SHIM=no
if [ -z "$HADOOP_AWS" ]; then
  say "hadoop-aws        not found" "$YEL"
else
  HV=$(ver_of "$HADOOP_AWS" hadoop-aws)
  HAS_CUSTOM=$(unzip -p "$HADOOP_AWS" org/apache/hadoop/fs/s3a/impl/streams/InputStreamType.class 2>/dev/null | strings | grep -cx "custom" || true)
  AUDIT_REF=$(unzip -p "$HADOOP_AWS" org/apache/hadoop/fs/s3a/audit/AWSRequestAnalyzer.class 2>/dev/null | strings | grep -c "analyticsaccelerator" || true)
  printf "hadoop-aws        %s\n" "$HV"
  if [ "${HAS_CUSTOM:-0}" -gt 0 ]; then
    printf "  custom stream   %syes%s\n" "$GRN" "$OFF"; S3A_OK=yes
  else
    printf "  custom stream   %sNO -- needs 3.4.2 or newer%s\n" "$RED" "$OFF"
  fi
  if [ "${AUDIT_REF:-0}" -gt 0 ]; then
    printf "  audit defect    %spresent -- needs the shim%s\n" "$YEL" "$OFF"; NEEDS_SHIM=yes
  else
    printf "  audit defect    %sabsent%s\n" "$GRN" "$OFF"
  fi
fi

# ---- the class the shim would supply --------------------------------------
COPIES=0
for j in $(tr ':' '\n' <<<"$JARS"); do
  [ -f "$j" ] || continue
  if unzip -l "$j" 2>/dev/null | grep -q "analyticsaccelerator/request/Constants.class"; then
    COPIES=$((COPIES+1)); FOUND_IN="$j"
  fi
done
if [ "$COPIES" -eq 0 ]; then
  printf "AAL Constants     %snot present%s\n" "$DIM" "$OFF"
elif [ "$COPIES" -eq 1 ]; then
  printf "AAL Constants     %salready provided%s by %s\n" "$GRN" "$OFF" "$(basename "$FOUND_IN")"
  NEEDS_SHIM=already
else
  printf "AAL Constants     %s%d copies -- collision%s\n" "$RED" "$COPIES" "$OFF"
  NEEDS_SHIM=collision
fi

[ -n "$AAL" ]     && printf "AAL               %s\n" "$(ver_of "$AAL" analyticsaccelerator-s3)"
[ -n "$ICEBERG" ] && printf "iceberg-aws       %s\n" "$(ver_of "$ICEBERG" iceberg-aws)"
[ -n "$ICEBERG_RT" ] && printf "iceberg runtime   %s\n" "$(basename "$ICEBERG_RT")"
[ -n "$AVRO" ]    && printf "avro              %s\n" "$(ver_of "$AVRO" avro)"

# ---- two version clashes found the hard way, neither of them ours ----------
# Both were found by building a real Iceberg table rather than by reading
# release notes, and a customer would meet them the same way: as a stack trace
# in somebody else's code, hours after adopting us.
ICE_V="$(ver_of "${ICEBERG_CORE:-${ICEBERG:-}}" iceberg-core 2>/dev/null)"
[ -z "$ICE_V" ] && ICE_V="$(ver_of "${ICEBERG:-}" iceberg-aws 2>/dev/null)"
AVRO_V="$(ver_of "${AVRO:-}" avro 2>/dev/null)"
if [ -n "$ICE_V" ] && [ -n "$AVRO_V" ]; then
  # Iceberg >= 1.9 calls LogicalTypes.timestampNanos(), added in Avro 1.12.0.
  ice_major=${ICE_V%%.*}; ice_rest=${ICE_V#*.}; ice_minor=${ice_rest%%.*}
  avro_major=${AVRO_V%%.*}; avro_rest=${AVRO_V#*.}; avro_minor=${avro_rest%%.*}
  if [ "${ice_major:-0}" -ge 1 ] 2>/dev/null && [ "${ice_minor:-0}" -ge 9 ] 2>/dev/null \
     && [ "${avro_major:-0}" -eq 1 ] 2>/dev/null && [ "${avro_minor:-99}" -lt 12 ] 2>/dev/null; then
    say "WARN  iceberg $ICE_V with avro $AVRO_V: writing Avro-format Iceberg" "$YEL"
    echo "      tables throws NoSuchMethodError on LogicalTypes.timestampNanos()."
    echo "      Pre-existing on this cluster, not caused by flint-accel, and it"
    echo "      does not affect Parquet tables or any read path. Fix: avro 1.12+."
  fi
fi
if [ -n "$AAL" ] && [ -n "$HADOOP_AWS" ]; then
  if ! unzip -l "$AAL" 2>/dev/null | grep -q "util/RequestCallback"; then
    say "WARN  this AAL predates util.RequestCallback, which hadoop-aws's own" "$YEL"
    echo "      AnalyticsStreamFactory references. If S3A selects the analytics"
    echo "      stream it will NoClassDefFoundError inside its own open(), before"
    echo "      flint-accel runs. Our shaded jar avoids this by relocating AAL;"
    echo "      an unshaded AAL on the cluster classpath does not."
    echo "      Fix: -Dfs.s3a.input.stream.type=classic, or upgrade AAL."
  fi
fi

# ---- verdict --------------------------------------------------------------
echo "----------------------------------------------------------------"
# ---- the encryption disclosure --------------------------------------------
# Printed unconditionally, before any configuration. ADR-0023 D13 commits to
# the principle that the encryption trade is STATED to the customer rather than
# discovered by them, and a security review that finds we cached their
# protected data unannounced ends the deal, rightly. Printing this only when we
# detect encryption would be worse than useless: preflight reads JARS, it
# cannot see a bucket policy, so absence of a warning would read as an
# all-clear it is not entitled to give.
say "Encryption, before you configure anything:" "$DIM"
cat <<'ENC'
  SSE-C     never cached. The tier would hold plaintext readable without the
            key, defeating the control outright. No acceleration; not tunable.
  SSE-KMS   NOT cached by default. S3 decrypts server-side, so caching is
            lawful -- but anyone who can read the tier would read the plaintext
            WITHOUT holding kms:Decrypt, and cache hits produce no CloudTrail
            decrypt record. If that audit trail is your compliance requirement,
            leave this off.
            To accelerate KMS buckets after your own review:
              --conf spark.hadoop.fs.s3a.flint.cache.sse-kms=true
              (python: cache_sse_kms=True)
  SSE-S3    cached normally; no grant or audit trail is bypassed.
ENC
echo "----------------------------------------------------------------"
if [ "$NEEDS_SHIM" = collision ]; then
  say "STOP -- two copies of AAL Constants are already on this classpath." "$RED"
  echo "Adding our shim would make it three. Resolve the existing duplicate first."
  exit 1
fi

if [ "$S3A_OK" = yes ]; then
  say "RECOMMENDED: the S3A path (works for every s3a:// read)" "$GRN"
  cat <<CFG

  --conf spark.hadoop.fs.s3a.input.stream.type=custom
  --conf spark.hadoop.fs.s3a.input.stream.custom.factory=ai.crestway.flintaccel.s3a.FlintStreamFactory
  --conf spark.hadoop.fs.s3a.flint.tier.uri=redis://<your-flint-endpoint>:6379

  jars: flint-accel.jar
CFG
  case "$NEEDS_SHIM" in
    yes)     echo "        + flint-accel-hadoop-shim.jar  ${YEL}(this hadoop-aws needs it)${OFF}" ;;
    already) echo "        ${GRN}do NOT add flint-accel-hadoop-shim.jar${OFF} -- already provided here" ;;
    no)      echo "        ${DIM}flint-accel-hadoop-shim.jar not needed${OFF}" ;;
  esac
else
  say "No Custom stream type on this hadoop-aws (needs 3.4.2+)." "$YEL"
  if [ -n "$ICEBERG" ] || [ -n "$ICEBERG_RT" ]; then
    echo
    say "OPTION A -- Iceberg tables (preferred: smallest blast radius)" "$GRN"
    cat <<CFG

  --conf spark.sql.catalog.<catalog>.io-impl=ai.crestway.flintaccel.iceberg.FlintFileIO
  --conf spark.sql.catalog.<catalog>.flint.tier.uri=redis://<your-flint-endpoint>:6379

  Accelerates Iceberg table reads only. Plain s3a:// paths are untouched,
  which is also why nothing else in the job can be affected by it.
CFG
  fi
  echo
  say "OPTION B -- any s3a:// read, on any Hadoop version" "$YEL"
  cat <<CFG

  --conf spark.hadoop.fs.s3a.impl=ai.crestway.flintaccel.s3a.FlintS3AFileSystem
  --conf spark.hadoop.fs.s3a.flint.tier.uri=redis://<your-flint-endpoint>:6379

  A subclass of S3AFileSystem overriding only the two read entry points.
  Works from 3.3.x up, needs no shim, one config line.

  The trade is blast radius: we become the FileSystem for every s3a://
  operation in the job, not just reads. Writes, listings and deletes are
  inherited untouched, but the class is ours. Prefer OPTION A where it
  applies, and prefer upgrading to hadoop-aws 3.4.2+ over either.
CFG
  [ -z "$ICEBERG$ICEBERG_RT" ] && echo "  ${DIM}(no Iceberg on this classpath, so OPTION A does not apply)${OFF}"
fi
echo
