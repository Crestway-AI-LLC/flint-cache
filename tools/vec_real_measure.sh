#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0049 verifications 1 and 2 on a real corpus, on a measurement box: run
# through packaging/aws/gate-box/run.sh (ops) with FLINT_GATE_TYPE=i4i.* and
# FLINT_GATE_CMD='bash tools/vec_real_measure.sh', pulling VEC_OUT. Fetches
# Qdrant's DBpedia set of 1M OpenAI text-embedding-3-large vectors at 1536
# dimensions (MIT), and the two wheels that convert it, onto the box only;
# converts; computes the exact neighbours once; then runs one
# `bench --data` process per arm, in parallel. Not a drill and in no gate.
#
#   VEC_N     base vectors to index (default: all 999,000)
#   VEC_ARMS  arms to run (default "plain sq8-disk bin-disk pq-disk")
#   VEC_DATA  where the data goes (default /mnt/d/vecdata, the instance store)
#   VEC_OUT   where the results go (default /tmp/flint-vecreal)
set -euo pipefail
cd "$(dirname "$0")/.."
D="${VEC_DATA:-/mnt/d/vecdata}"
OUT="${VEC_OUT:-/tmp/flint-vecreal}"
N="${VEC_N:-}"
ARMS="${VEC_ARMS:-plain sq8-disk bin-disk pq-disk}"
mkdir -p "$D/parquet" "$D/py" "$OUT"

# The wheels are cp39: say so now rather than fail at an import.
case "$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')" in
  3.9) ;;
  *) echo "FAIL: the converter's wheels are for Python 3.9; python3 here is $(python3 --version)"; exit 1 ;;
esac

HF=https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-large-1536-1M/resolve/main/data
for i in $(seq 0 25); do
  f=$(printf 'train-%05d-of-00026.parquet' "$i")
  [ -s "$D/parquet/$f" ] || curl -fsSL --retry 3 -o "$D/parquet/$f" "$HF/$f"
done
echo "== parquet: $(ls "$D/parquet" | wc -l) files, $(du -sh "$D/parquet" | cut -f1)"

# Exactly the two wheels approved, checked against PyPI's sha256, unpacked
# onto PYTHONPATH: no pip, and nothing else fetched.
wheel() { # url sha256 file
  [ -s "$D/$3" ] || curl -fsSL --retry 3 -o "$D/$3" "$1"
  echo "$2  $D/$3" | sha256sum -c --quiet - || { echo "FAIL: $3 does not match PyPI's sha256"; exit 1; }
  (cd "$D/py" && python3 -m zipfile -e "$D/$3" .)
}
PY=https://files.pythonhosted.org/packages
wheel "$PY/39/f4/90258b4de753df7cc61cefb0312f8abcf226672e96cc64996e66afce817a/pyarrow-17.0.0-cp39-cp39-manylinux_2_28_x86_64.whl" \
  a48ddf5c3c6a6c505904545c25a4ae13646ae1f8ba703c4df4a1bfe4f4006bda pyarrow-17.0.0-cp39-cp39-manylinux_2_28_x86_64.whl
wheel "$PY/54/30/c2a907b9443cf42b90c17ad10c1e8fa801975f01cb9764f3f8eb8aea638b/numpy-1.26.4-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" \
  f870204a840a60da0b12273ef34f7051e98c3b5961b61b0c2c1be6dfd64fbcd3 numpy-1.26.4-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

[ -s "$D/base.fbin" ] || PYTHONPATH="$D/py" python3 tools/vec_fbin_from_parquet.py "$D/parquet" "$D" 1000

cargo build --release -q -p flint-vec --bin bench
B=./target/release/bench
"$B" --data "$D" ${N:+--n "$N"} --queries 200 --gt-only 2>&1 | tee "$OUT/gt.log"
for arm in $ARMS; do
  "$B" --data "$D" --arm "$arm" ${N:+--n "$N"} --queries 200 --vec-dir "/mnt/d/vecdir-$arm" \
    > "$OUT/$arm.txt" 2> "$OUT/$arm.log" &
done
wait
cat "$OUT"/*.txt
