# Eight pairs under chaos

What an eight-pair cluster does when you bootstrap it, load it, and start
killing masters. Run on one AWS **i4i.2xlarge** (8 vCPU, 61 GB RAM, 1.7 TB
NVMe), Amazon Linux 2023.

**What this proves and what it does not.** `flintctl` spawns nodes as local
processes — the multi-machine remote runner is still roadmap — so eight
pairs is 16 `flint-server` processes plus a control plane, controller and
proxy on ONE host. That exercises slot routing at width, proxy fan-out, the
controller polling 16 nodes, control-plane snapshot cost, and concurrent
failover. It does **not** exercise real network partitions, cross-AZ
latency, host loss, or NIC saturation between nodes: chaos here kills
processes, not machines.

## Bootstrap

Sixteen nodes, a control plane and a proxy, with full mTLS certificate
minting: **6.1 seconds**. Every pair came up master/replica at epoch (0,1)
with `seq_lag 0`.

## Slot spread

200,000 keys written through the single proxy endpoint, then each pair's
master counted directly:

| keys | distribution across 8 pairs |
|---|---|
| 200,000 sequential (`k:%06d`) | exactly 25,000 each |
| 80,000 random (`r:%08x`) | 10013, 9844, 10106, 10080, 10037, 10123, 9963, 9833 |

The exact split for sequential keys is real, not an artifact — and it was
worth confirming, because eight identical numbers is what a *broken* hash
would also look like. CRC16 is linear over GF(2), so a contiguous run of
fixed-width keys distributes exactly evenly across power-of-two slot
buckets. Random keys show proper binomial variance (σ ≈ 105 expected, ≈ 110
observed), which is what rules out the degenerate explanation.

Sequential key spaces — user IDs, order IDs — therefore balance perfectly
rather than merely well.

`SCAN` of all 200,000 keys through the proxy, as one cursor stream across
eight pairs: **1.1 s**.

## Chaos: six masters killed

Six masters across different pairs, killed 8 s apart, with the controller
running (`poll-ms 200`, `confirm 3`):

Every one had its replica promoted to epoch **(0,2)**. The two untouched
pairs stayed at (0,1). No manual intervention.

The first attempt at this showed no promotions at all — because the
inventory omitted `controller on`, so nothing was watching. Worth recording
as an operational footgun: a cluster with no controller fails *stop* rather
than failing over, and the proxy correctly answered `ERR a pair has no
reachable master` rather than serving from a stale replica. Correct
behaviour, but not the behaviour anyone wants to discover during an
incident.

## Two bugs this run found

Both were found by doing ordinary things — bulk-loading data and killing
masters — and **every existing drill stayed green through both**, because a
drill asserts the path it was written for.

**Fan-out never rediscovered after a promotion.** Keyed traffic healed
itself; `DBSIZE`, `FLUSHALL` and `SCAN` stayed pointed at the dead node
indefinitely — still broken after keyed traffic had recovered and after
another 20 seconds. Failover is the headline feature and this bricked
keyspace iteration until the proxy was restarted.

**The proxy rejected inline commands and dropped the connection.** The
server has always parsed them; the proxy did not, and all tenant traffic
goes through the proxy. That breaks `redis-cli --pipe` — the bulk-import
path Redis's own mass-insert documentation recommends — and it failed
*silently*: a 200,000-key load printed "All data transferred" and left
`DBSIZE` at zero.

Both fixed in the same session and verified against the cluster that
exhibited them: `DBSIZE` and `SCAN` back to 100,001 with six masters dead,
`--pipe` reporting `errors: 0`.

They are also the reason `flintctl verify` exists. Reconciling the control
plane's view, each node's manifest, and the proxy's actual behaviour would
have caught the first the moment the cluster failed over.
