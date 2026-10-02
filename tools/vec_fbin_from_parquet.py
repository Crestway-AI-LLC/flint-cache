#!/usr/bin/env python3
# SPDX-License-Identifier: Elastic-2.0
"""ADR-0049 verification 2: a corpus's embedding Parquet files to the float32
.fbin files `bench --data` reads. The last QUERIES rows are held out as queries
and the rest are the base set. COLUMN names the embedding column; the default
is Qdrant's DBpedia OpenAI set
(huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-large-1536-1M,
MIT), and tools/vec_real_measure.sh names the others.

Usage: vec_fbin_from_parquet.py PARQUET_DIR OUT_DIR [QUERIES [COLUMN]]
Needs pyarrow and numpy; tools/vec_real_measure.sh supplies both.
"""
import glob
import struct
import sys

import numpy as np
import pyarrow.parquet as pq

COL = "text-embedding-3-large-1536-embedding"


def main():
    src, out = sys.argv[1], sys.argv[2]
    nq = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    col_name = sys.argv[4] if len(sys.argv) > 4 else COL
    files = sorted(glob.glob(f"{src}/*.parquet"))
    if not files:
        sys.exit(f"no parquet files in {src}")
    total = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    nb = total - nq
    dim = None
    done = 0
    with open(f"{out}/base.fbin", "wb") as base, open(f"{out}/query.fbin", "wb") as query:
        for f in files:
            for batch in pq.ParquetFile(f).iter_batches(columns=[col_name], batch_size=10000):
                col = batch.column(0)
                if col.null_count:
                    sys.exit(f"{f}: {col.null_count} rows have no embedding")
                rows = col.flatten().to_numpy(zero_copy_only=False).astype("<f4").reshape(len(col), -1)
                if dim is None:
                    dim = rows.shape[1]
                    base.write(struct.pack("<II", nb, dim))
                    query.write(struct.pack("<II", nq, dim))
                if rows.shape[1] != dim:
                    sys.exit(f"{f}: a row of {rows.shape[1]} dimensions among {dim}")
                to_base = max(0, min(len(rows), nb - done))
                base.write(rows[:to_base].tobytes())
                query.write(rows[to_base:].tobytes())
                done += len(rows)
    print(f"base {nb} x {dim}, queries {nq} x {dim}, from {len(files)} files")


if __name__ == "__main__":
    main()
