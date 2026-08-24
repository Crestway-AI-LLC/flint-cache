#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Prove the shaded jar cannot impose our dependency versions on a customer.
#
# ADR-0023 D12.16 made this a requirement rather than a packaging afterthought:
# getting this project to build needed a Netty BOM and an AWS SDK bump, and a
# customer's Spark cluster has its own versions of both. Relocation is the
# promise; this is the check that it was kept.
set -uo pipefail
cd "$(dirname "$0")/../jvm-spike"

# Select by EXCLUSION, not by sort order. An earlier version took the first
# match and silently graded the hadoop-shim jar as if it were the main one --
# every count was wrong in a way that still produced a plausible verdict.
MAIN=$(ls target/*.jar 2>/dev/null | grep -v original | grep -v hadoop-shim | head -1)
SHIM=$(ls target/*hadoop-shim.jar 2>/dev/null | head -1)
[ -f "$MAIN" ] || { echo "no shaded jar; run: mvn package"; exit 2; }

ok=0
c() { # label, zero|some, actual
  if   [ "$2" = zero ] && [ "$3" -eq 0 ]; then printf "[ok]   %-46s %s\n" "$1" "$3"
  elif [ "$2" = some ] && [ "$3" -gt 0 ]; then printf "[ok]   %-46s %s\n" "$1" "$3"
  else printf "[FAIL] %-46s %s (wanted %s)\n" "$1" "$3" "$2"; ok=1; fi
}
n() { unzip -l "$MAIN" 2>/dev/null | grep -c "$1" || true; }

echo "main jar: $MAIN ($(du -h "$MAIN" | cut -f1 | tr -d ' '))"
echo "-------------------------------------------------------------------"
c "un-relocated io/netty"              zero "$(n ' io/netty/')"
c "un-relocated io/lettuce"            zero "$(n ' io/lettuce/')"
c "un-relocated reactor/"              zero "$(n ' reactor/')"
c "relocated netty"                    some "$(n 'shaded/netty/')"
c "relocated lettuce"                  some "$(n 'shaded/lettuce/')"
c "hadoop not bundled"                 zero "$(n ' org/apache/hadoop/')"
c "iceberg not bundled"                zero "$(n ' org/apache/iceberg/')"
c "aws sdk not bundled"                zero "$(n ' software/amazon/awssdk/')"
c "shim NOT in the main jar"           zero "$(n 'analyticsaccelerator/request/Constants.class')"
# Positive controls: every count above is also zero for an empty jar.
c "our client classes present"         some "$(n 'flintaccel/client/')"
c "our adoption entry points present"  some "$(n 'flintaccel/s3a/FlintS3AFileSystem')"

echo "-------------------------------------------------------------------"
if [ -f "$SHIM" ]; then
  echo "shim jar: $SHIM"
  SC=$(unzip -l "$SHIM" 2>/dev/null | grep -c '\.class$' || true)
  SK=$(unzip -l "$SHIM" 2>/dev/null | grep -c 'analyticsaccelerator/request/Constants.class' || true)
  if [ "$SC" -eq 1 ]; then printf "[ok]   %-46s %s\n" "shim jar holds exactly one class" "$SC"
  else printf "[FAIL] %-46s %s (wanted 1)\n" "shim jar holds exactly one class" "$SC"
       echo "       ^ anything more means we ship code into someone else's namespace"
       ok=1; fi
  c "and it is the Constants shim"     some "$SK"
else
  printf "[FAIL] %-46s\n" "no hadoop-shim artifact built"; ok=1
fi
echo "-------------------------------------------------------------------"
[ $ok -eq 0 ] && echo "SHADING VERIFIED" || echo "SHADING FAILED"
exit $ok
