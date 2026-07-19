# Flint docs

- `adr/` — architecture decision records for the open stack (numbered
  globally; gaps in the sequence are Crestway-internal records about the
  managed-service plane, referenced from code comments by number).
- `bugs/` — notable root-caused bugs, kept as regression documentation.
- `retry-safety.md` — which commands are safe to retry, and why.

Some code comments also reference `docs/design.md` and the raw benchmark
data under `docs/bench/` — both are Crestway-internal working documents;
the public conclusions live in the ADRs (e.g. ADR-0003 for the storage
baseline numbers).
