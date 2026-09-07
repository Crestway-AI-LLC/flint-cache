// SPDX-License-Identifier: Elastic-2.0
//! flint-chaos (KV workload): random writes with a checksummed ledger while
//! master/replica instances are killed randomly. Oracle: no corruption ever;
//! zero acked-write loss on replica kills and on steady-state master
//! failover; no time-travel or cross-key bleed.
//!
//! Usage: flint-chaos [--iterations 12] [--keys 400] [--mode mixed|replica|master] [--seed N]
//!
//! `--quiesce-file <path>` + `--converge-s N`: the pre-kill gate needs the
//! pair at seq_lag == 0, and this harness can only park its OWN writer. When
//! something else is also writing (the durability soak runs four feeders),
//! pass a quiesce path that load agrees to honour, or the gate is a race.
//!
//! `--inventory <path>` attaches to a REAL flintctl-managed fleet instead of
//! spawning a local pair, so the same oracle runs against seats that may be
//! on different machines. Faults go through `flintctl kill-node` /
//! `restart-node`, which know where each seat lives.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use flint_chaos::cluster::{Attached, Cluster, Target, arg, sweep_stale_dirs};

/// Wall clock in ms. The RPO bound is a statement about TIME — "acked longer
/// ago than the cap must have replicated" — so the ledger needs a real clock,
/// not the monotonic Instant used for pacing.
use flint_chaos::oracle::{Served, classify_send, parse_value};
use flint_chaos::writer::{self, Edge, Shared, now_ms};
use flint_resp::Value;
use rand::{Rng, SeedableRng, rngs::SmallRng};

/// Ask the rest of the world to stop writing, for as long as this lives.
///
/// The harness parks its own writer with `shared.pause`, which is a complete
/// quiesce only when the harness is the only writer. The durability soak runs
/// four external feeder loops against the same edge, so parking one writer in
/// five left the pre-kill convergence gate depending on a coin flip: soak run
/// 20 won it and measured a kill, run 21 lost and died in the gate having
/// killed nothing (#183). The file is the contract — while it exists,
/// cooperating load holds off.
///
/// Dropped on EVERY path including a panic, because a quiesce left armed is a
/// soak that silently stops ingesting and still reports its feeders alive.
struct Quiesce(Option<std::path::PathBuf>);

impl Quiesce {
    fn begin(path: &str) -> Self {
        if path.is_empty() {
            return Self(None);
        }
        let p = std::path::PathBuf::from(path);
        match std::fs::write(&p, b"flint-chaos: hold off writes\n") {
            Ok(()) => Self(Some(p)),
            Err(e) => {
                // Loud, and NOT fatal: the gate that follows will fail on its
                // own if the load really did keep writing, and it names this
                // file when it does.
                eprintln!("chaos: could not arm quiesce file {path}: {e}");
                Self(None)
            }
        }
    }
}

impl Drop for Quiesce {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Why an edge dial might have failed, in the words of the thing most
/// likely to be wrong.
///
/// An undialable edge surfaces in at least two places — the post-kill stall
/// detector and the final walk's `connect()` — and which one you hit is a
/// timing race. Before this, one blamed the proxy ("the proxy never
/// recovered") and the other said `edge client` and nothing else. Both sent
/// the reader to the fleet. The dial is the cheaper thing to rule out, so it
/// gets named first, and the hint lives here rather than at each site so the
/// two cannot drift apart.
fn edge_hint(edge_addr: &str, edge_ca: &str) -> String {
    if edge_ca.is_empty() {
        format!(
            "dialling {edge_addr} in PLAINTEXT (no --edge-ca). Against a \
             client-TLS proxy this looks exactly like a fleet that never \
             recovered: the TCP connect succeeds, the handshake never \
             happens, and every write times out. Suspect the dial before \
             the fleet."
        )
    } else {
        format!(
            "dialling {edge_addr} with TLS trusting {edge_ca}. If the proxy \
             is up, check that the edge cert's SANs cover the dialled host — \
             the edge uses the dialled name, not the mesh's fixed SNI. \
             Suspect the dial before the fleet."
        )
    }
}

fn main() {
    let iterations: u32 = arg("--iterations", 12);
    let key_count: u64 = arg("--keys", 400);
    let mode: String = arg("--mode", "mixed".to_string());
    let controller_driven = arg("--driver", "harness".to_string()) == "controller";
    let min_replicas: u32 = arg("--min-replicas", 0);
    if min_replicas > 0 {
        // Gate every node the harness spawns; workloads must retry -THROTTLED.
        unsafe { std::env::set_var("FLINT_CHAOS_MIN_REPLICAS", min_replicas.to_string()) };
    }
    let inventory: String = arg("--inventory", String::new());
    // The cluster's ports, as a base: master, master+1, and master+2 for the
    // proxy when --mode proxy is used. Default 6460 keeps the historical
    // master port; the proxy moves off 7690, which tenant_quota's control
    // plane also binds. Give concurrent or neighbouring drills distinct bases
    // and each can DECLARE its block, which the port guards need.
    let port_base: u16 = arg("--port-base", 6460u16);
    // A "randomized" gate that runs ONE path per topology explores nothing
    // after its first run. run.sh pinned --seed 22 and the attached drill
    // --seed 7, forever (#118 item 5). The seed now defaults to the clock and
    // is PRINTED, so every run is a fresh draw and any failure is replayable
    // by pasting the number back as --seed.
    let seed: u64 = arg(
        "--seed",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(42),
    );
    // OPS-0058. Every key this RUN writes carries this prefix, so a cluster
    // that hosts more than one run cannot hand our final walk someone else's
    // value at one of our key names. Derived from the seed, so `--seed N`
    // replays the keyspace as well as the sequence, and printed in the banner
    // so the operator can find this run's keys on the box afterwards. It goes
    // OUTSIDE the hash tag -- see Shared::run_nonce.
    let run_nonce = format!("r{seed:x}-");
    // The server's own default (repl_hub::DEFAULT_LAG_HARD_MS). Writes acked
    // longer ago than this MUST have replicated, because past it the master
    // sheds instead of acking — so losing one is a breach of the published
    // bound, not the async contract.
    let lag_hard_ms: u64 = arg("--lag-hard-ms", 1_000);
    // The oracle's bound and the server's enforced cap must be the SAME
    // number, or the check compares against a cap nobody applied.
    if lag_hard_ms != 1_000 {
        unsafe { std::env::set_var("FLINT_CHAOS_LAG_HARD_MS", lag_hard_ms.to_string()) };
    }
    // Freeze the replica for this long before each master kill, so the master
    // acks writes the replica has not taken. Without it the unreplicated
    // suffix is empty on loopback and the RPO bound has nothing to measure —
    // every run to date reported a loss depth of 0ms.
    //
    // Calibration matters. A stall well under the cap produces real,
    // in-bounds loss depth (700ms stall -> 543ms deepest loss, oracle green;
    // the same seed with no stall reads 0ms, so the stall is demonstrably
    // what creates the window). A stall LONGER than the cap trips the RPO
    // assertion — not because a gate failed, but because the published bound
    // is stated as a wall-clock age and no gate bounds the age of an
    // already-acked write. See the assertion's own note.
    let stall_replica_ms: u64 = arg("--stall-replica-ms", 0);
    // TEST-ONLY fault injector, off unless asked for. Reproduces on demand the
    // pair of conditions that once turned a master-kill artefact into a
    // data-loss verdict on the NEXT replica kill:
    //
    //   1. the write really is gone from the survivor, and
    //   2. the oracle cannot read it to find that out.
    //
    // Both co-occur in the wild — a key lost in the failover, read back while
    // the proxy is still chasing the promotion and answering -TRYAGAIN — but
    // only under load and only sometimes, so the retire path that handles it
    // shipped unexercised: every run reported `unreadable: 0`. A fix nothing
    // has ever executed is a belief. This makes it a test.
    let inject_unreadable: u64 = arg("--inject-unreadable", 0);
    // Slack for measurement, not for the engine: the master samples lag
    // slightly before it decides to ack, and our ack clock is taken after the
    // reply arrives. Small next to the cap it qualifies.
    let rpo_margin_ms: u64 = arg("--rpo-margin-ms", 500);
    // docs/slo.md publishes RTO <= 10 s.
    let rto_budget_ms: u64 = arg("--rto-budget-ms", 10_000);
    // Client-path mode: drive the workload through the proxy edge instead of
    // dialling each pair's master (#118 item 3, and what the #99 plan asked
    // for). Needs a tenant credential because a CP-fed proxy is gated.
    let edge_addr: String = arg("--edge", String::new());
    let edge_auth: String = arg("--auth", String::new());
    // The CA that signs the proxy's EDGE certificate — the tenant's trust
    // root, not the mesh's. Omitted means a plaintext edge, which is what
    // every chaos run before ADR-0018 item 9 silently assumed.
    let edge_ca: String = arg("--edge-ca", String::new());
    // --quiesce-file: the pre-kill convergence gate needs the pair to reach
    // seq_lag == 0, and this harness can only park ITS OWN writer. Any load
    // outside it keeps the replica behind and the gate becomes a coin flip
    // (#183). While this file exists, cooperating load holds off; see the
    // Quiesce guard. Empty = no external load to coordinate with, the
    // historical behaviour.
    let quiesce_file: String = arg("--quiesce-file", String::new());
    // How long to wait for that convergence. A soak's replacement replica is
    // still full-syncing from an earlier kill, so 10 s is not always enough
    // once a dataset is real.
    let converge_s: u64 = arg("--converge-s", 10);
    // A quiesce left armed by a crashed run is a soak that silently stops
    // ingesting, so start from a known-unquiesced state.
    if !quiesce_file.is_empty() {
        let _ = std::fs::remove_file(&quiesce_file);
    }
    // Clear the corpses of runs whose process is gone before allocating any
    // of our own. Drop handles this run; only a sweep can handle the ones
    // that were SIGKILLed or ended via process::exit, and only a sweep drains
    // a backlog that already exists on the box.
    sweep_stale_dirs();
    println!(
        "chaos-kv: {iterations} kills, {key_count} keys, mode={mode}, driver={}, min_replicas={min_replicas}, seed={seed} (replay with --seed {seed}); keyspace {run_nonce}key*",
        if !inventory.is_empty() {
            // An attached fleet HAS a controller; promotion was never ours
            // to make, so saying "harness" here would misreport what was
            // actually under test.
            "fleet-controller"
        } else if controller_driven {
            "controller"
        } else {
            "harness"
        }
    );

    // One Target PER PAIR. The 7-host runs reported "16 cross-host kills on
    // 7 hosts" while every kill landed on pair 0 — Attached::open took a
    // single pair index defaulting to 0 and run.sh never passed another, so
    // pair 1 was scenery (#118 item 2). Each pair also gets its own writer
    // and ledger: a hammer on pair 0 while pair 1's master dies would test
    // nothing about pair 1's loss window. --pair N still pins one pair for
    // debugging.
    let mut targets: Vec<Target> = if inventory.is_empty() {
        vec![Target::Local {
            cluster: if controller_driven {
                Cluster::bootstrap_controlled_at(port_base, 150, 3)
            } else {
                Cluster::bootstrap_at(port_base)
            },
            controller_driven,
        }]
    } else {
        // A real fleet already has a controller; promotion is its job.
        let pin: i64 = arg("--pair", -1);
        if pin >= 0 {
            vec![Target::Attached(Attached::open(&inventory, pin as usize))]
        } else {
            (0..Attached::pair_count(&inventory))
                .map(|i| Target::Attached(Attached::open(&inventory, i)))
                .collect()
        }
    };
    println!("  pairs under test: {}", targets.len());
    // Before a single write: this oracle cannot mean anything against a
    // namespace that is allowed to evict. See Target::refuse_if_evictable.
    for t in &targets {
        if let Err(e) = t.refuse_if_evictable() {
            eprintln!("{e}");
            std::process::exit(2);
        }
    }
    // The workload runs on its own thread and does not stop for kills —
    // that is the entire point (#118 item 1): with the writer parked at the
    // moment a master dies, the unreplicated suffix is empty and the RPO
    // bound has nothing to test. See writer.rs for the full argument.
    let writer_seed: u64 = seed;
    let edge = if edge_addr.is_empty() {
        None
    } else {
        let (tenant, token) = edge_auth
            .split_once(':')
            .unwrap_or_else(|| panic!("--edge needs --auth <tenant>:<token>"));
        // Built here rather than lazily in the writer so a bad path fails
        // the run at startup with the reason, instead of turning into an
        // endless retry loop against a healthy fleet forty minutes in.
        let etls = if edge_ca.is_empty() {
            None
        } else {
            Some(
                flint_tls::edge_client_config(&edge_ca)
                    .unwrap_or_else(|e| panic!("--edge-ca {edge_ca}: {e}")),
            )
        };
        println!(
            "  client path: proxy edge {edge_addr} as tenant {tenant} ({})",
            if etls.is_some() {
                format!("TLS, trusting {edge_ca}")
            } else {
                // Said out loud, because it is the posture that used to be
                // the only one and the one a release note would misdescribe.
                "PLAINTEXT — not the posture a TLS customer runs".to_string()
            }
        );
        Some(Edge {
            addr: edge_addr.clone(),
            tenant: tenant.to_string(),
            token: token.to_string(),
            tls: etls,
        })
    };
    let pair_count = targets.len();
    let shareds: Vec<std::sync::Arc<Shared>> = (0..pair_count)
        .map(|i| {
            let t = &targets[i];
            // Through the edge the PROXY routes, so each writer pins its keys
            // behind a hash tag landing in its own pair's slots — otherwise
            // writer 1's keys scatter onto pair 0 and its verdict after a
            // pair-1 kill would judge the wrong nodes.
            let tag = if edge.is_some() {
                flint_chaos::cluster::pair_tag(i, pair_count)
            } else {
                String::new()
            };
            std::sync::Arc::new(
                Shared::new(t.endpoints(), t.tls(), key_count)
                    .with_edge(edge.clone(), tag)
                    .with_run_nonce(run_nonce.clone()),
            )
        })
        .collect();
    let writer_handles: Vec<_> = shareds
        .iter()
        .enumerate()
        .map(|(i, sh)| {
            let sh = sh.clone();
            // Distinct stream per pair, derived from the run seed so a replay
            // reproduces every writer, not just the first.
            std::thread::spawn(move || writer::run(&sh, writer_seed.wrapping_add(i as u64)))
        })
        .collect();

    let mut rng = SmallRng::seed_from_u64(seed);

    // COVERAGE FIRST, then randomness.
    //
    // Picking uniformly at random every iteration leaves a real chance that a
    // short run never touches one of the pairs: with 2 pairs and 6 kills that
    // is (1/2)^6 on each side, about 3% of runs. attached_chaos_drill.sh then
    // reports "no kill ever landed on pair 0 / the harness is single-pair
    // again" — which is precisely the regression that assertion exists to
    // catch (the 7-host runs once reported "16 cross-host kills" while every
    // one hit pair 0). A check that cries wolf on its own headline finding is
    // worse than no check, because it teaches you to wave the real one
    // through.
    //
    // So deal every pair exactly once, in a shuffled order, before any pair
    // takes a second kill. Which pair goes first, master versus replica, and
    // all the timing stay random — only the coverage becomes certain. Note
    // this changes the sequence a given --seed produces, so seeds recorded
    // before this change no longer replay to the same run.
    let mut deal: Vec<usize> = (0..targets.len()).collect();
    for i in (1..deal.len()).rev() {
        deal.swap(i, rng.random_range(0..=i));
    }

    let mut acked_lost_total = 0u64;
    let mut unverifiable_total = 0u64;
    let mut injected = 0u64;
    let mut rtos: Vec<u64> = Vec::new();
    // THE TWO NUMBERS THE RTO WAS HIDING (BUG-0121).
    //
    // On the direct path the RTO is `first post-outage ack - kill_ms`, and
    // `kill_ms` is stamped BEFORE the kill is dispatched. On a multi-host
    // fleet that dispatch is a master-discovery round trip plus an SSH hop,
    // so the reported figure is dispatch PLUS recovery with nothing
    // separating them. Measured over 401 master kills: dispatch p50 712ms
    // against a reported RTO p50 of 572ms -- the number was mostly SSH.
    //
    // `stalls` is the same quantity the EDGE path already reports as its RTO:
    // the worst gap between consecutive ACKS, which needs no kill stamp at
    // all and is what a client actually experienced. It has been computed on
    // the direct path all along (the writer updates it whenever a kill is
    // armed) and never surfaced.
    let mut stalls: Vec<u64> = Vec::new();
    let mut dispatches: Vec<u64> = Vec::new();
    // Worst single unanswered request across every master kill — the figure
    // the write deadline bounds, reported next to the ack-gap so the two are
    // never confused for each other (#186).
    let mut worst_hold_ms: u64 = 0;
    let mut deepest_loss_ms: u64 = 0;
    // Acked writes lost that were older than the cap: the AGE reading of the
    // RPO, reported because it is interesting, not asserted because it is not
    // promised. See the note at the check site.
    let mut beyond_cap: u64 = 0;
    // BUG-0014: acks whose SEND stamp equals the death stamp exactly. Neither
    // master can be ruled out for these, so they are excluded from every
    // measurement and surfaced on their own. A run that reports a durability
    // finding resting on these has reported nothing; that is the whole point
    // of counting them apart.
    let mut ambiguous_at_boundary: u64 = 0;
    // BUG-0120, second half: `lost_here` is decided by `got < last_acked`, and
    // `last_acked` is the max seq over ALL acks — including ones SENT after the
    // master was already dead, which `classify_send` returns as
    // `Served::NewMaster` and the loop skips. So the loss COUNT and the loss
    // DEPTH were computed over different populations, and a key whose whole
    // surplus was post-death sends counted toward the regression total while
    // contributing to no depth measure and appearing in no counter.
    //
    // That case is not the async tail. It is the new master failing to hold a
    // write it acked, which is the more serious reading, and it was being
    // folded silently into a number the run then annotated as within the cap.
    let mut lost_unattributed: u64 = 0;
    // THE BOUND THE PRODUCT ACTUALLY PROMISES, finally measured.
    //
    // docs/failover.md and slo.md both say the RPO bounds the VOLUME of
    // at-risk writes, not their age: past the lag cap the master stops
    // ACCEPTING, so at most one cap-window's worth of acked writes can ever be
    // unreplicated. The age assertion that used to stand here was removed for
    // being a promise nothing makes, and the comment recording that said the
    // volume bound "SHOULD be asserted" and "needs the observed write rate".
    // This is that. The M2 exit depends on it: "measured RPO <= 1 s worth of
    // writes (a volume, not an age)".
    //
    // Per pair, the window is [previous recovery, this kill] — healthy
    // operation only, so an outage cannot drag the rate down and tighten the
    // budget it produces. Budget is rate x (lag_hard_ms + rpo_margin_ms): one
    // cap-window's worth plus the same margin the cap check already uses.
    let mut volume_breaches: u64 = 0;
    let mut volume_unjudged: u64 = 0;
    let mut lost_writes_total: u64 = 0;
    // (window start ms, writer seq at that instant) per pair; filled once the
    // writers are up, just before the kill loop.
    let mut post_death_surplus: u64 = 0;

    // BUG-0014: the ledger boundary from the PREVIOUS master kill, carried
    // across iterations. The replica-kill assertion needs it to say whether a
    // failing ack predates that kill (never retired -> harness, BUG-0007
    // class) or postdates it (served by the current master -> real loss).
    let mut last_dead_us: u64 = 0;
    // Mixed mode flips a coin per iteration, so a run can land on a replica
    // every time — six tails is 1/64 — and `attached_chaos_drill.sh` then
    // correctly refuses to pass a failover path it never exercised. That
    // turns a coverage gap into an intermittent red on commits that changed
    // nothing; it reddened public main on a docs-only commit on 2026-08-22.
    // Counted so the flip can stop once coverage is actually at risk.
    let mut master_kills: u32 = 0;

    // One window per pair, opened at run start; each master kill closes the
    // current one and the recovery opens the next.
    let mut vol_window: Vec<(u64, u64)> = shareds
        .iter()
        .map(|sh| (now_ms(), sh.seq.load(Ordering::SeqCst)))
        .collect();

    for iteration in 1..=iterations {
        // Let the writer run for a spell BETWEEN kills; it keeps writing
        // through what follows.
        std::thread::sleep(Duration::from_millis(rng.random_range(400..900)));

        // Which pair takes this kill. Every pair's writer keeps hammering
        // regardless; only the chosen pair's ledger is judged afterwards.
        // The first pass walks `deal` so every pair is hit once; after that it
        // is uniform again.
        let pair_idx = if (iteration as usize) <= deal.len() {
            deal[iteration as usize - 1]
        } else {
            rng.random_range(0..targets.len())
        };
        let cluster = &mut targets[pair_idx];
        let shared = &shareds[pair_idx];

        let want_master = match mode.as_str() {
            "replica" => false,
            "master" => true,
            // Past halfway with no master killed yet, stop flipping and ask
            // for one every remaining iteration. The run stays random where
            // randomness is the point — which pair, and in what order — and
            // becomes deterministic only about covering the thing the drill
            // is named for. A master kill can still be declined below when
            // the pair is re-seeding, so this raises the floor rather than
            // guaranteeing it, and the drill's own assert remains the check.
            _ if master_kills == 0 && iteration * 2 > iterations => true,
            _ => rng.random_bool(0.5),
        };
        // Kill a master that HAS a live replica but is NOT required to be
        // caught up, with writes in flight AT the kill. That is the regime
        // the RPO number describes; requiring seq_lag==0 first (the old
        // guard) left nothing unreplicated to lose, so the oracle's verdict
        // was a property of this harness.
        // ARM, RESUME, THEN KILL. The controller arms auto-failover only after
        // observing the pair converged, and under a live hammer it never does
        // — the first run of this loop killed an unarmed pair and died on
        // "controller did not promote within 20s". So: park the writer until
        // convergence has been visible long enough for the controller's
        // confirm*poll window (the hotkey drill's precedent), resume the
        // hammer, give it a beat so the replica is genuinely behind again, and
        // only then kill. Writes are in flight AT the kill, which is the point.
        //
        // A pair that is NOT converged costs this iteration's master kill, not
        // the run. Soak run 22 aborted outright: iteration 3 killed pair 3's
        // replica, iteration 8 killed the same pair's master while the
        // replacement was still re-seeding, and the controller then correctly
        // refused to promote a survivor it had never seen hold the lineage.
        // The run died on a promotion timeout wearing #171's signature, for a
        // kill it should never have made. A re-seeding pair is a NORMAL state
        // mid-soak; the harness picks a different victim and carries on.
        //
        // Not silent: the skip prints, and run.sh separately asserts at least
        // one master was killed, so a run that skipped every one cannot pass.
        // NO SILENT DOWNGRADE. A master kill that becomes a replica kill
        // because no live replica appeared used to leave no trace at all --
        // the SKIPPED line below covers only the CONVERGED check, and this
        // one short-circuits before it. A run that reports "1 master kill"
        // out of 16 should say what happened to the other 15.
        let have_live = cluster.wait_replica_live(Duration::from_secs(8));
        if want_master && !have_live {
            println!(
                "iter {iteration}: pair {pair_idx}: master kill DOWNGRADED — no live replica \
                 within 8s, so there is nothing to fail over to; killing a replica instead"
            );
        }
        let kill_master = want_master && have_live && {
            // Quiesce EVERYONE, not just ourselves — see Quiesce. Resumed
            // together with our own writer, so the kill still lands with
            // writes in flight from every source.
            let quiesce = Quiesce::begin(&quiesce_file);
            shared.pause.store(true, Ordering::SeqCst);
            let converged = cluster.wait_healthy(Duration::from_secs(converge_s));
            std::thread::sleep(Duration::from_millis(1_500)); // controller confirm*poll
            shared.pause.store(false, Ordering::SeqCst);
            drop(quiesce);
            if !converged {
                println!(
                    "iter {iteration}: pair {pair_idx}: master kill SKIPPED — the replica \
                         has not taken the lineage within {converge_s}s (still re-seeding from \
                         an earlier kill?); killing a REPLICA this iteration instead{}",
                    if quiesce_file.is_empty() {
                        " [no --quiesce-file: load outside this harness keeps seq_lag \
                             above 0, see #183]"
                    } else {
                        ""
                    }
                );
            }
            converged
        };
        if kill_master {
            master_kills += 1;
            let harness_promoted = cluster.promotion_is_harness();
            std::thread::sleep(Duration::from_millis(300)); // hammer re-established

            // Deliberately push the replica behind, so the master is acking
            // writes that have not replicated when it dies. Kept under the
            // 2s liveness window so the pair still looks failover-worthy —
            // this is the bounded-loss regime, not the widowed one.
            if stall_replica_ms > 0 && cluster.stall_replica(true) {
                std::thread::sleep(Duration::from_millis(stall_replica_ms));
                // Unfreeze BEFORE the kill, not after. The survivor has to be
                // running to be promoted and to answer the oracle, but it is
                // still carrying the whole backlog, so the master dies with
                // acked writes the replica has not taken. Resuming afterwards
                // instead meant sending FLINTPROMOTE to a stopped process.
                cluster.stall_replica(false);
            }
            // Arm the writer's RTO clock, then kill. The kill blocks through
            // the promotion, so any timestamp taken after it would include
            // the recovery it is meant to measure. The writer closes the
            // measurement with its FIRST POST-KILL ACK — the vantage of the
            // thing actually trying to write.
            shared.recovered_ms.store(0, Ordering::SeqCst);
            shared.outage_seen.store(false, Ordering::SeqCst);
            shared.max_stall_ms.store(0, Ordering::SeqCst);
            shared.max_stall_at_ms.store(0, Ordering::SeqCst);
            shared.max_hold_ms.store(0, Ordering::SeqCst);
            shared.max_hold_at_ms.store(0, Ordering::SeqCst);
            shared.acks_after_kill.store(0, Ordering::SeqCst);
            let kill_ms = now_ms();
            shared.kill_ms.store(kill_ms, Ordering::SeqCst);
            // Two clocks on purpose. `kill_ms` is armed BEFORE the kill and
            // times the outage (RTO/stall) from the writer's vantage.
            // `dead_us` is stamped AFTER the SIGKILL landed, in MICROSECONDS,
            // and is the only boundary the LEDGER may use: in the gap between
            // the two — an
            // epoch read plus a pkill spawn, tens of ms on a busy box — the
            // old master is alive and still acking. Judging those acks as
            // "sent after the kill, so the new master's" left the ledger
            // claiming values the survivor never had, and the NEXT replica
            // kill reported them as data loss (seed 7: key270 216 < 239).
            let dead_us = cluster.kill_master_hot();
            last_dead_us = dead_us; // BUG-0014: carry it to the next iteration
            // Harness-mode replacement replicas get fresh ports; republish
            // so the writer can find the pair again. (Attached and
            // controlled-local endpoints are fixed; this is a no-op there.)
            shared.set_endpoints(cluster.endpoints());

            let deadline = Instant::now() + Duration::from_millis(rto_budget_ms.max(1) * 2);
            // Two different questions, because the two paths answer different
            // ones. DIRECT: how long from the error to the first success —
            // the client saw the outage. CLIENT PATH: how long was the worst
            // stall — the client saw no error at all, just one slow write,
            // because the proxy chased the promotion underneath. Waiting for
            // a direct-path outage on the client path hangs forever; the
            // first run of edge mode did exactly that.
            let rto = if shared.edge.is_some() {
                // Enough acks after the kill that the failover window is
                // certainly behind us, then take the worst gap.
                loop {
                    if shared.acks_after_kill.load(Ordering::SeqCst) >= 50 {
                        break shared.max_stall_ms.load(Ordering::SeqCst).max(1);
                    }
                    // NAMES THE POSTURE, not just the symptom. This fires
                    // identically whether the proxy really failed to recover
                    // or whether chaos never completed a single handshake
                    // with it — and the second case sends someone to debug a
                    // healthy proxy. It is exactly what a plaintext dial
                    // against a TLS edge looked like before --edge-ca
                    // existed: zero writes served, blamed on the fleet.
                    assert!(
                        Instant::now() < deadline,
                        "iter {iteration}: edge served fewer than 50 writes in \
                         {rto_budget_ms}ms x2 after the kill ({} since) — the \
                         proxy never recovered, OR this run never reached it: {}",
                        shared.acks_after_kill.load(Ordering::SeqCst),
                        edge_hint(&edge_addr, &edge_ca),
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            } else {
                loop {
                    let r = shared.recovered_ms.load(Ordering::SeqCst);
                    if r != 0 {
                        break r;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "iter {iteration}: writer saw no ack within {rto_budget_ms}ms x2 of the kill"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            };
            shared.kill_ms.store(0, Ordering::SeqCst);
            // The absolute window, not just the delta. A breach is only
            // actionable if it can be JOINED to the fleet journal's `at_ms`,
            // and for three runs it could not be: the message said "11.5s
            // exceeds 10s" and nothing else, so the time could not be
            // attributed to a leg (detect / fence / promote / re-route) and
            // the finding stayed open. These three numbers are what turn a
            // verdict into a diagnosis.
            // On the EDGE path `rto` is the worst inter-ack gap, not an
            // outage-to-first-ack, so "kill_ms + rto" would be a fiction.
            // Report the instant the gap actually closed; the two paths
            // measure different things and the window must say which.
            let stall_at = shared.max_stall_at_ms.load(Ordering::SeqCst);
            let recovered_at_ms = if shared.edge.is_some() && stall_at != 0 {
                stall_at
            } else {
                kill_ms.saturating_add(rto)
            };
            // WHAT WAS MEASURED, in the message that reports the breach. The
            // per-iteration line already distinguishes "client stall" from
            // "RTO", but the ASSERT said "kill to first post-kill ack" on both
            // paths — and the assert is the only line anyone reads, because it
            // is the one that ends the run. Every scale run drives through
            // --edge, so #181 ("failover blackout is 11.5s") was named from a
            // sentence describing the OTHER path. On the edge the number is
            // the worst gap between consecutive acks anywhere in the post-kill
            // window; a write stall on the new master produces it just as
            // readily as a slow promotion, and the two want opposite fixes.
            let measured = if shared.edge.is_some() {
                "worst gap between consecutive acks through the proxy edge. This is NOT \
                 necessarily the failover: ANY stall in the post-kill window sets it \
                 (a RocksDB write stall on the new master reads identically). Check \
                 rocks-stalls-*.txt in the evidence bundle before blaming promotion"
            } else {
                "kill to first post-kill ack"
            };
            let dispatch_ms = (dead_us / 1000).saturating_sub(kill_ms);
            let stall_ms = shared.max_stall_ms.load(Ordering::SeqCst);
            if !harness_promoted {
                rtos.push(rto);
                stalls.push(stall_ms);
                dispatches.push(dispatch_ms);
                // The HOLD belongs in the breach message, not only in the
                // summary. This assert fires before the per-iteration line
                // prints, so on soak run 26 a 43850ms breach arrived with no
                // hold figure at all — and the hold is the datum that says,
                // on sight, whether clients were HELD for the window or
                // refused promptly inside it. Those want opposite fixes, and
                // establishing which cost a separate FLINTINFO sweep against
                // a fleet that was already tearing down.
                let held = shared.max_hold_ms.load(Ordering::SeqCst);
                let held_at = shared.max_hold_at_ms.load(Ordering::SeqCst);
                let reading = if held * 4 < rto {
                    "the node FAST-FAILED: no single write waited anywhere near the gap, so \
                     clients were being refused (-THROTTLED) rather than held. Look at the \
                     ROUTING leg — promotion notices, the proxy's control-plane watch — not \
                     at storage"
                } else {
                    "writes were genuinely HELD for most of the gap, so the write path itself \
                     was blocked. Look at storage: fullsync_active, write_stopped, \
                     delayed_write_rate, writes_shed_deadline"
                };
                assert!(
                    rto <= rto_budget_ms,
                    "iter {iteration}: {rto}ms exceeds the published budget {rto_budget_ms}ms \
                     (docs/slo.md) — {measured}. \
                     Worst SINGLE write held: {held}ms (at {held_at}) — {reading}. \
                     WINDOW kill_ms={kill_ms} dead_us={dead_us} recovered_at_ms={recovered_at_ms} — \
                     slice the fleet journal to this window; the leg holding the \
                     time names itself"
                );
            }

            // Oracle pass over a SNAPSHOT of the ledger. The writer keeps
            // going, so keys examined here can gain acks afterwards — but a
            // new ack cannot un-lose an old one, and each key's verdict uses
            // only what was recorded at snapshot time.
            struct KeySnap {
                key: String,
                acked_at: Vec<(u64, u64, u64)>,
                last_acked: u64,
            }
            // BUG-0014 DIAGNOSTIC: the prune below runs INSIDE the loop over
            // this snapshot, so a key excluded here is never pruned. If such a
            // key later acquires an ack that predates the kill and was never
            // replicated, the NEXT replica kill asserts it against a seat that
            // never had it. That is the same class as the bug the comment at
            // the prune describes, one level out: that fix widened the prune's
            // CONDITION, this is a hole in its DOMAIN.
            //
            // Printed unconditionally, not only on failure: a zero here across
            // many runs falsifies the mechanism, and a count that only appears
            // when the bug fires could never do that.
            let (snapshot, excluded) = {
                let led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                let excluded = led.iter().filter(|(_, e)| e.last_acked == 0).count();
                let snap: Vec<KeySnap> = led
                    .iter()
                    .filter(|(_, e)| e.last_acked != 0)
                    .map(|(k, e)| KeySnap {
                        key: k.clone(),
                        acked_at: e.acked_at.clone(),
                        last_acked: e.last_acked,
                    })
                    .collect();
                (snap, excluded)
            };
            println!(
                "  | BUG-0014 DIAGNOSTIC: master-kill prune domain: {} key(s) in \
                 snapshot, {} ledger key(s) EXCLUDED (last_acked==0) and therefore \
                 never pruned",
                snapshot.len(),
                excluded
            );
            // BUG-0120: anchored to the DEATH, not to `kill_ms`. `kill_ms` is
            // stamped before `kill_master_hot()`, and on `Target::Attached`
            // that call makes a master-discovery round trip and then an SSH
            // kill before stamping `dead_us` — measured at 711ms, 720ms and
            // 3304ms on one 5-host soak. Every write acked inside that window
            // was served by a master still alive, so against `kill_ms` its
            // depth `saturating_sub`s to 0 and its age never reaches
            // `must_have_replicated_by`. Both measures read clean over exactly
            // the interval where the loss happens.
            //
            // `dead_us` is stamped just AFTER the SIGKILL returned, so death
            // <= dead_us and the depth is an OVER-estimate. That is the right
            // direction for a durability measure: it may call a loss deeper
            // than it was, it cannot call a deep loss shallow. `beyond_cap` is
            // counted rather than asserted, so a conservative over-count costs
            // nothing.
            let dead_ms = dead_us / 1000;
            let must_have_replicated_by = dead_ms.saturating_sub(lag_hard_ms + rpo_margin_ms);
            // Read back the way the CLIENT would: through the edge when
            // that is the path under test, so a proxy that has not chased
            // the promotion shows up as the data loss it would be for a
            // real client rather than being bypassed.
            let mut c = shared
                .connect()
                .or_else(|| cluster.master_client().ok())
                .expect("oracle connect");
            let mut lost_here = 0u64;
            let mut deepest_here = 0u64;
            let mut lost_writes_here = 0u64;
            // Keys the survivor would not answer for, even after retries. Not
            // loss, not health — an absence of evidence, which is reported
            // rather than swallowed so nobody mistakes a quiet run for a
            // clean one.
            let mut unverifiable = 0u64;
            for KeySnap {
                key,
                acked_at,
                last_acked,
            } in &snapshot
            {
                // A reply we cannot READ is not evidence of anything — but the
                // old `_ => continue` skipped the key while leaving it in the
                // ledger still claiming its pre-kill acks. The next REPLICA
                // kill then demanded that key, found it absent, and panicked
                // "REPLICA kill lost acked key" — a master-kill artefact
                // turned into a data-loss verdict on the one path that has no
                // async-contract excuse. Seen for real: iter 1 killed a master
                // and regressed 12 acked keys, iter 2 killed a replica and
                // blamed it for key9.
                //
                // So: retry first, because right after a promotion -TRYAGAIN
                // (the stale-read fence) and -MOVED are exactly what a correct
                // cluster says while the proxy catches up. Only if it stays
                // unreadable do we give up — and then RETIRE the key's
                // pre-kill acks so that nothing downstream judges it on
                // evidence we could not gather, and report the count.
                // The injector simulates the OUTCOME of an unreadable reply —
                // "still unreadable after the retries" — rather than the
                // transport error itself; reaching that outcome is all the
                // retry loop is for. It also DELs the key first, because
                // without that the key is still on the master and a later
                // replica-kill check would find it and pass whether or not the
                // retire worked, which would make this test prove nothing.
                //
                // It injects the FAULT ONLY. The retire that follows is the
                // real code path, untouched — if the injector did the retiring
                // itself the test would pass with the fix removed, which is
                // the difference between a regression test and a decoration.
                // Only inject into a key with NO post-kill ack. Post-kill acks
                // belong to the NEW master and the retire keeps them on
                // purpose, so deleting such a key manufactures a loss the
                // ledger is right to complain about — which is a bug in the
                // injector, not a finding. The real condition is a write lost
                // in the failover that nothing rewrote afterwards.
                let inject_now =
                    injected < inject_unreadable && acked_at.iter().all(|&(_, _, at)| at < kill_ms);
                if inject_now {
                    injected += 1;
                    let _ = c.call(&[b"DEL", key.as_bytes()]);
                }
                let got: Option<u64> = if inject_now {
                    None
                } else {
                    let mut attempt = 0;
                    loop {
                        match c.call(&[b"GET", key.as_bytes()]) {
                            Ok(Value::Bulk(Some(raw))) => {
                                let (owner, seq_got) = parse_value(&raw)
                                    .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                                assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                                break Some(seq_got);
                            }
                            // Absent is a real, readable answer: the survivor
                            // holds nothing for this key. That is sequence 0,
                            // not "unverifiable".
                            Ok(Value::Bulk(None)) => break Some(0),
                            _ if attempt < 5 => {
                                attempt += 1;
                                std::thread::sleep(Duration::from_millis(100));
                            }
                            _ => break None,
                        }
                    }
                };
                let Some(got) = got else {
                    unverifiable += 1;
                    let mut led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(entry) = led.get_mut(key) {
                        entry.acked_at.retain(|&(_, sent_us, _)| sent_us >= dead_us);
                        entry.last_acked =
                            entry.acked_at.iter().map(|&(s, _, _)| s).max().unwrap_or(0);
                    }
                    continue;
                };
                // Acked before the cap's window? Then replication carried it,
                // or the bound is broken. Acked inside the window? Losing it
                // is the async contract — track the depth, not a failure.
                //
                // WHAT THIS ASSERTION ENCODES, and why it can fire on
                // behaviour that is not a code bug. docs/failover.md states
                // the RPO as a WALL-CLOCK age: "a crash loses at most the
                // async tail below --lag-hard-ms (default <= 1 s)". The
                // mechanisms actually implemented — the lag cap, the
                // min-replicas gate, the lease — all bound when the master
                // STOPS ACCEPTING NEW writes. None of them can retroactively
                // protect a write that was already acked. So if the replica
                // stalls right after an ack, that write's age grows for as
                // long as the stall lasts, without limit, and this assertion
                // fires while every gate did its job.
                //
                // Reproduced at --stall-replica-ms 1800 under BOTH drivers
                // and with --min-replicas 1: breach at ~1.7s every time. The
                // bound the product enforces is on the VOLUME of at-risk
                // writes (about one cap-window's worth), not on their age.
                // RECONCILED 2026-08-27, on the doc side: docs/failover.md
                // now says outright "it bounds VOLUME, not age" and spells out
                // that no mechanism here can reach back and protect a write
                // already acked. This comment used to say the two were still
                // unreconciled and the assertion stood on the published
                // promise; that sent a reader looking for a conflict that no
                // longer exists. The assertion stays because the VOLUME bound
                // is still worth asserting, not because an age bound is
                // promised anywhere.
                // Per KEY: did any ack this loop actually judged sit above the
                // survivor's value? If not, the regression counted below is
                // invisible to every depth measure (BUG-0120).
                let mut measured_surplus = 0u64;
                for &(seq, sent_us, at) in acked_at {
                    // The writer may have re-acked this key AFTER the kill;
                    // those acks belong to the new master and say nothing
                    // about what the old one lost. Judged by SEND time
                    // against the post-SIGKILL clock: a request sent strictly
                    // after `dead_us` cannot have been served by the dead
                    // master, whereas one sent in the arming gap — or in
                    // flight at the kill and acked afterwards — can.
                    let served = classify_send(sent_us, dead_us);
                    if served == Served::NewMaster {
                        // Counted now (BUG-0120) rather than silently dropped:
                        // this is the surplus that `lost_here` sees and the
                        // depth measures cannot.
                        if seq > got {
                            post_death_surplus += 1;
                        }
                        continue;
                    }
                    // THE BOUNDARY ITSELF IS NEITHER, AND SAYS SO.
                    //
                    // `dead_us` is stamped just AFTER the SIGKILL returned, so
                    // the death instant lies at or before it. A send sharing
                    // that exact stamp may have preceded the death and been
                    // served by the old master, or followed it and been served
                    // by the new one, and nothing recorded distinguishes them.
                    //
                    // This used to be folded into the `>=` above, which
                    // silently assigned every tie to the new master. At
                    // millisecond resolution ties were common enough that one
                    // carried an entire durability verdict (BUG-0014): the run
                    // reported a regression whose whole evidence was a single
                    // entry at the one point the clock could not resolve.
                    //
                    // Microseconds make this rare rather than impossible, so
                    // it is counted and reported instead of being decided. An
                    // ambiguous entry feeds NEITHER `beyond_cap` nor
                    // `deepest_loss_ms`: a measurement taken on an
                    // unattributable write is not a conservative estimate, it
                    // is a number with no referent.
                    if served == Served::Ambiguous {
                        ambiguous_at_boundary += 1;
                        continue;
                    }
                    // NO LONGER AN ASSERTION — see the note above. This used
                    // to fail the run when an acked write older than the cap
                    // was lost, i.e. it enforced the WALL-CLOCK AGE reading of
                    // the RPO. docs/failover.md and slo.md now state the bound
                    // the product actually provides — a VOLUME: past the cap
                    // the master stops accepting, so at most one cap-window's
                    // worth is ever at risk, and an already-acked write ages
                    // without limit while replication is stalled.
                    //
                    // Keeping the age assertion after correcting the claim
                    // would fail honest runs for a promise nothing makes. It
                    // just did: seed 42, no --stall-replica-ms, a natural
                    // 3160ms stall under load on a busy box. A gate that red-
                    // lights on behaviour the docs explicitly permit teaches
                    // people to re-run it, which is worse than not having it.
                    //
                    // The depth is still MEASURED and reported every run, so a
                    // real regression remains visible; what is gone is the
                    // false verdict attached to it.
                    //
                    // THE VOLUME BOUND IS NOW ASSERTED (2026-09-07). This
                    // comment used to end "the volume bound that SHOULD be
                    // asserted needs the observed write rate and is tracked
                    // separately" — it has the rate now: per pair, over the
                    // healthy window from the previous recovery to the kill,
                    // and `rpo_volume_budget` turns it into one cap-window's
                    // arrivals. A kill that loses more than that fails the run.
                    // The age reading stays unasserted and that is still
                    // right; what changed is that the bound the product DOES
                    // promise is no longer merely reported.
                    if seq > got && at <= must_have_replicated_by {
                        beyond_cap += 1;
                    }
                    if seq > got {
                        measured_surplus += 1;
                        deepest_here = deepest_here.max(loss_depth_ms(dead_us, at));
                    }
                }
                // The exit says "worth of writes". `lost_here` counts KEYS,
                // which is the wrong unit for a volume bound — one key can
                // lose many acked writes. `measured_surplus` is the count of
                // acked WRITES this loop actually judged and found missing.
                lost_writes_here += measured_surplus;
                if got < *last_acked {
                    lost_here += 1;
                    // No pre-death ack above the survivor's value means every
                    // measure that judges this loss skipped it. Say so rather
                    // than letting it inherit the reassurance the others earn.
                    if measured_surplus == 0 {
                        lost_unattributed += 1;
                    }
                }
                // Retire what this failover lost, ALWAYS. Entries acked
                // before the kill and above what the survivor holds are gone;
                // leaving them in the ledger makes the NEXT kill re-judge
                // them, and their age keeps growing — which is how a run
                // reported "acked 7628ms before the kill" against a 1000ms
                // cap on a fleet that had lost nothing of the sort.
                //
                // The previous version only pruned when the key's last_acked
                // still matched the snapshot, i.e. when the writer had not
                // acked anything new mid-pass. Against a concurrent writer
                // that guard fails constantly, so the pruning silently did
                // not happen. Post-kill acks are kept: they belong to the new
                // master and are not this failover's business.
                let mut led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(entry) = led.get_mut(key) {
                    entry
                        .acked_at
                        .retain(|&(s, sent_us, _)| s <= got || sent_us >= dead_us);
                    entry.last_acked = entry.acked_at.iter().map(|&(s, _, _)| s).max().unwrap_or(0);
                }
            }
            acked_lost_total += lost_here;
            deepest_loss_ms = deepest_loss_ms.max(deepest_here);
            lost_writes_total += lost_writes_here;

            // THE VOLUME BOUND, judged per kill.
            //
            // Rate over the healthy window that just ended: from this pair's
            // previous recovery (or run start) to this kill. Outages are
            // excluded by construction, because the window closes AT the kill
            // and reopens after recovery — an outage would otherwise depress
            // the rate and hand the next kill a budget tighter than the engine
            // was ever asked to honour.
            let (win_start_ms, win_start_seq) = vol_window[pair_idx];
            let win_ms = kill_ms.saturating_sub(win_start_ms);
            let win_writes = shared
                .seq
                .load(Ordering::SeqCst)
                .saturating_sub(win_start_seq);
            // A window too short or too idle to estimate a rate is NOT a pass.
            // Counted and reported, the same discipline as the boundary ties:
            // "no verdict" and "no breach" must not print the same way.
            let vol_budget = rpo_volume_budget(win_writes, win_ms, lag_hard_ms + rpo_margin_ms);
            if vol_budget.is_none() {
                volume_unjudged += 1;
            }
            let vol_note = match vol_budget {
                Some(b) if lost_writes_here > b => {
                    volume_breaches += 1;
                    format!(
                        "; VOLUME BREACH: {lost_writes_here} acked write(s) lost against a budget of                          {b} ({win_writes} writes in {win_ms}ms => one {}ms window's worth)",
                        lag_hard_ms + rpo_margin_ms
                    )
                }
                Some(b) => format!(
                    "; volume {lost_writes_here}/{b} acked write(s) ({win_writes} in {win_ms}ms)"
                ),
                None => format!(
                    "; volume UNJUDGED ({win_writes} writes in {win_ms}ms is too little to rate)"
                ),
            };
            unverifiable_total += unverifiable;
            // The pair, always together. `{rto}` is the widest gap between
            // ACKS; `max_hold_ms` is the longest a single request went
            // unanswered. A window where the node fast-failed shows a large
            // gap and a SMALL hold — writes were refused promptly, which is
            // the deadline working, not a stall (#186). A window where the
            // node held on shows both large. Reporting only the first cannot
            // tell those apart, and for three runs it did not.
            // Reopen this pair's window at the recovery, so the next kill's
            // rate is measured over healthy operation only.
            vol_window[pair_idx] = (
                recovered_at_ms.max(kill_ms),
                shared.seq.load(Ordering::SeqCst),
            );

            let held = shared.max_hold_ms.load(Ordering::SeqCst);
            worst_hold_ms = worst_hold_ms.max(held);
            println!(
                "iter {iteration}: pair {pair_idx}: killed MASTER (writes in flight); {} {rto}ms{} \
                 [kill_ms={kill_ms} dead_us={dead_us} recovered_at_ms={recovered_at_ms} \
                 max_hold_ms={held} dispatch={dispatch_ms}ms client_stall={stall_ms}ms]; acked keys \
                 regressed: {lost_here} (deepest {deepest_here}ms before the death; cap {lag_hard_ms}ms){vol_note}{}",
                if shared.edge.is_some() {
                    "client stall"
                } else {
                    "RTO"
                },
                if harness_promoted {
                    " harness-promoted, not RTO"
                } else {
                    ""
                },
                if unverifiable > 0 {
                    format!(
                        "; {unverifiable} key(s) unreadable after retries — retired from the ledger, \
                         NOT judged as loss"
                    )
                } else {
                    String::new()
                }
            );
        } else {
            cluster.kill_replica();
            shared.set_endpoints(cluster.endpoints());
            // A replica kill must not disturb the write path at all: every
            // ack recorded BEFORE the kill still stands on the master.
            let snapshot: Vec<(String, u64)> = {
                let led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                led.iter()
                    .filter(|(_, e)| e.last_acked != 0)
                    .map(|(k, e)| (k.clone(), e.last_acked))
                    .collect()
            };
            // ASK THE CLUSTER WHO THE MASTER IS; do not go looking.
            //
            // This used to try shared.connect() first, which scans endpoints
            // for whoever answers `role:master`. kill_replica() spawns the
            // REPLACEMENT on a fresh port with an empty data dir, and a node
            // with no durable manifest can report `role:master` in the window
            // before its --replica-of handshake demotes it. The scan then
            // reads keys from a node that is still full-syncing and finds a
            // sequence behind the ledger — which the assertion below reports
            // as "REPLICA kill lost acked write", i.e. as data loss on the one
            // path that has no async-contract excuse.
            //
            // cluster.master_client() uses the port the harness KNOWS is
            // master, so the check can no longer be answered by the wrong
            // node. If this assertion ever fires again it is about the
            // master, which is what it always claimed to be about.
            // EDGE MODE MUST STAY ON THE EDGE. Keys written through the
            // proxy live in the TENANT's namespace and carry its hash tag;
            // a direct master dial has no namespace, so every one of them
            // reads back as nil. Forcing master_client() here did exactly
            // that — "REPLICA kill lost acked key {p1x2}key170: Bulk(None)",
            // which is the harness looking in the wrong place, reported as
            // data loss.
            //
            // Direct mode is where the role-scan is unsafe: shared.connect()
            // there picks whoever answers `role:master`, and kill_replica()
            // spawns the replacement with an empty data dir, which can claim
            // master until --replica-of demotes it. Reading a node mid
            // full-sync yields a sequence just behind the ledger — the
            // original #126 symptom. So: edge through the edge, direct via
            // the port the harness KNOWS is master.
            // BUG-0014: record WHICH BRANCH was taken, not only what it
            // resolved to. If --edge is ever added to this drill, a
            // diagnostic that printed only the resolution would measure
            // nothing and read as "ruled out".
            let read_via = if shared.edge.is_some() {
                "edge (shared.connect)"
            } else {
                "direct (cluster.master_client)"
            };
            let mut c = if shared.edge.is_some() {
                shared.connect().unwrap_or_else(|| {
                    panic!(
                        "no edge client for the post-kill snapshot check — {}",
                        edge_hint(&edge_addr, &edge_ca)
                    )
                })
            } else {
                cluster.master_client().expect("master client")
            };
            for (key, last_acked) in &snapshot {
                match c.call(&[b"GET", key.as_bytes()]) {
                    Ok(Value::Bulk(Some(raw))) => {
                        let (owner, got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                        if got < *last_acked {
                            // BUG-0014: three candidate causes, and the
                            // ledger entry separates them. Dump before dying.
                            let dump = {
                                let led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                                match led.get(key) {
                                    Some(e) => {
                                        let mut v: Vec<String> = e
                                            .acked_at
                                            .iter()
                                            .filter(|&&(sq, _, _)| sq > got)
                                            .map(|&(sq, sent_us, at)| {
                                                // Three states, because two
                                                // cannot express "the stamps
                                                // are equal and the order is
                                                // therefore unrecorded". The
                                                // boolean this replaces read
                                                // `sent < last_dead_us`, so a
                                                // tie printed as `false` —
                                                // indistinguishable from a
                                                // write provably sent after
                                                // the kill, which is the
                                                // reading that made BUG-0014
                                                // look decided.
                                                let rel = match classify_send(sent_us, last_dead_us)
                                                {
                                                    Served::MaybeOldMaster => "before_prev_kill",
                                                    Served::Ambiguous => {
                                                        "AMBIGUOUS_ON_PREV_KILL_BOUNDARY"
                                                    }
                                                    Served::NewMaster => "after_prev_kill",
                                                };
                                                format!(
                                                    "(seq={sq} sent_us={sent_us} at_ms={at} \
                                                     vs_prev_kill={rel})"
                                                )
                                            })
                                            .collect();
                                        if v.len() > 10 {
                                            v.truncate(10);
                                            v.push("...".to_string());
                                        }
                                        format!(
                                            "ledger last_acked={} entries_above_got=[{}]",
                                            e.last_acked,
                                            v.join(" ")
                                        )
                                    }
                                    None => "<no ledger entry>".to_string(),
                                }
                            };
                            panic!(
                                "iter {iteration}: REPLICA kill lost acked write at {key}: \
                                 {got} < {last_acked}\n\
                                 == BUG-0014 DIAGNOSTIC ==\n\
                                 read_via: {read_via}\n\
                                 master:   {}\n\
                                 prev_master_kill dead_us: {last_dead_us}\n\
                                 {dump}\n\
                                 READ IT SO: any offending seq with \
                                 vs_prev_kill=before_prev_kill was acked BEFORE the \
                                 previous master kill and never retired -> harness \
                                 ledger bug, BUG-0007 class, NOT a durability \
                                 regression. All after_prev_kill -> the write was \
                                 served by the CURRENT master and lost -> real. \
                                 Any AMBIGUOUS_ON_PREV_KILL_BOUNDARY entry is \
                                 NEITHER: its send stamp equals the death stamp to \
                                 the microsecond, so the order is not recorded and \
                                 no verdict may rest on it. If the offending set is \
                                 only those, this run has found nothing and the \
                                 report says so rather than picking a side.",
                                cluster.master_diagnostic()
                            );
                        }
                    }
                    other => {
                        panic!("iter {iteration}: REPLICA kill lost acked key {key}: {other:?}")
                    }
                }
            }
            println!("iter {iteration}: pair {pair_idx}: killed REPLICA; zero acked loss verified");
        }
    }

    // Stop every writer before the final walk, or the walk races live SETs
    // and "TIME TRAVEL" would fire on a value newer than the snapshot
    // ceiling.
    for sh in &shareds {
        sh.stop.store(true, Ordering::SeqCst);
    }
    for h in writer_handles {
        h.join().expect("writer thread");
    }
    let seq: u64 = shareds.iter().map(|s| s.seq.load(Ordering::SeqCst)).sum();
    let throttled_total: u64 = shareds
        .iter()
        .map(|s| s.throttled.load(Ordering::SeqCst))
        .sum();

    // Final full-keyspace walk, per pair against that pair's own ledger.
    let (mut present, mut missing) = (0u64, 0u64);
    for (t, sh) in targets.iter_mut().zip(&shareds) {
        let ledger = std::mem::take(&mut *sh.ledger.lock().unwrap_or_else(|e| e.into_inner()));
        // NO FALLBACK OFF THE EDGE. This used to be
        // `.connect().or_else(|| t.master_client().ok())`, so when the edge
        // could not be dialled the final walk quietly verified the ledger
        // against the pair masters instead — and printed PASS. A run whose
        // whole claim is "the client path holds" would then have checked a
        // different path than the one under test, and said nothing. That is
        // the same defect as dialling the edge in plaintext (ADR-0018 item
        // 9), one layer further in.
        //
        // The fallback stays for DIRECT mode, where there is no edge and
        // master_client is the only path there ever was.
        let mut c = if sh.edge.is_some() {
            sh.connect().unwrap_or_else(|| {
                panic!(
                    "final walk cannot reach the edge, and will NOT fall back to \
                     the masters — that would verify a path this run is not \
                     testing: {}",
                    edge_hint(&edge_addr, &edge_ca)
                )
            })
        } else {
            sh.connect()
                .or_else(|| t.master_client().ok())
                .expect("final connect")
        };
        for (key, entry) in &ledger {
            match c.call(&[b"GET", key.as_bytes()]) {
                Ok(Value::Bulk(Some(raw))) => {
                    let (owner, got) =
                        parse_value(&raw).unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                    assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                    assert!(
                        entry.written.contains(&got),
                        "PHANTOM at {key}: seq {got} never written"
                    );
                    assert!(got <= entry.last_written, "TIME TRAVEL at {key}");
                    present += 1;
                }
                Ok(Value::Bulk(None)) => missing += 1,
                other => panic!("final walk failed at {key}: {other:?}"),
            }
        }
    }

    let (mk, rk) = targets.iter().fold((0u32, 0u32), |(m, r), t| {
        let (tm, tr) = t.kills();
        (m + tm, r + tr)
    });
    println!("---");
    // A volume breach is a FAILED run, and the first word has to say so. The
    // summary still prints in full underneath — an operator needs the numbers
    // more, not less, when the verdict went the wrong way — and the process
    // exits non-zero at the end.
    let verdict = if volume_breaches > 0 { "FAIL" } else { "PASS" };
    println!("{verdict}: {iterations} kills ({mk} master, {rk} replica), {seq} writes");
    println!("  corruption: 0  time-travel: 0  cross-key: 0");
    println!(
        "  acked keys regressed across master kills: {acked_lost_total} (async contract; replica kills: zero)"
    );
    // Printed even when zero: it is the denominator for the line above. A run
    // that could not read N keys judged fewer than it appears to have judged,
    // and silence about that is how "clean" and "unexamined" become the same
    // output.
    println!(
        "  keys unreadable at a kill, retired and NOT judged as loss: {unverifiable_total} \
         (transient -TRYAGAIN/-MOVED while the proxy chased a promotion; each retried 5x)"
    );
    println!(
        "  writes shed -THROTTLED (retried): {throttled_total} (widowed/lag gate exercised when > 0)"
    );
    if rtos.is_empty() {
        println!("  RTO: not measured (promotions were harness-issued, not the product's)");
    } else {
        let mut sorted = rtos.clone();
        sorted.sort_unstable();
        let p50 = sorted[sorted.len() / 2];
        let worst = *sorted.last().expect("non-empty");
        let label = if edge.is_some() {
            // Named for what it is. Through the edge the client never saw a
            // failure, so calling this RTO would overstate the outage and
            // understate the product: the proxy absorbed it.
            "client-visible stall (no errors seen; the proxy chased the promotion)"
        } else {
            "RTO kill->writable"
        };
        println!(
            "  {label} over {} promotion(s): p50 {p50}ms, worst {worst}ms (budget {rto_budget_ms}ms, docs/slo.md)",
            sorted.len()
        );
        // WHAT THAT NUMBER IS MADE OF, on the direct path (BUG-0121). The
        // edge path's figure is already the inter-ack gap, so these two would
        // restate it; there they are printed for the dispatch alone.
        let mut sd = dispatches.clone();
        sd.sort_unstable();
        let (dp50, dworst) = (sd[sd.len() / 2], *sd.last().expect("non-empty"));
        let mut ss = stalls.clone();
        ss.sort_unstable();
        let (sp50, sworst) = (ss[ss.len() / 2], *ss.last().expect("non-empty"));
        if edge.is_some() {
            println!(
                "  kill dispatch (stamp to SIGKILL returning): p50 {dp50}ms, worst {dworst}ms"
            );
        } else {
            println!(
                "  of which kill DISPATCH (kill_ms to the SIGKILL returning): p50 {dp50}ms, \
                 worst {dworst}ms — the master is alive and serving for this, so it is in the \
                 figure above and is not outage"
            );
            println!(
                "  client-observed outage (worst gap between ACKS, no kill stamp): p50 {sp50}ms, \
                 worst {sworst}ms — this is what a client experienced, and what the edge path \
                 already reports as its RTO"
            );
            // NEITHER STAMP IS THE DEATH, and the run must say so rather than
            // let a reader pick whichever suits. `kill_ms` precedes dispatch;
            // `dead_us` is taken when the SIGKILL RETURNS, which is after the
            // signal landed -- anchoring to it yields negative recovery times
            // (measured: p50 -144ms over 401 kills). The death is inside the
            // dispatch window and these stamps cannot resolve it further.
            println!(
                "  NOTE: the death is somewhere inside that dispatch window. kill_ms is before \
                 it and dead_us is after it, so neither bounds the failover tightly; the \
                 outage line above is the one that needs no death stamp."
            );
        }
    }
    // The figure above is a gap between ACKS, so it cannot separate "one write
    // was held for the whole window" from "writes were refused promptly for
    // the whole window". This one can: it is the longest any single request
    // went unanswered. Small here with a large gap above means the node
    // fast-failed — the write deadline doing its job, not an outage the
    // client absorbed (#186).
    println!(
        "  worst single write held: {worst_hold_ms}ms before any answer (ack or -THROTTLED); \
         --write-deadline-ms is what bounds this, docs/slo.md"
    );
    println!(
        "  deepest acked-write loss: {deepest_loss_ms}ms before the DEATH (post-SIGKILL stamp, BUG-0120 — not the pre-kill stamp in the iteration lines; cap {lag_hard_ms}ms + {rpo_margin_ms}ms margin; older-than-cap losses are COUNTED, not failed — the cap bounds volume, not age)"
    );
    // Say plainly when a run proved nothing about the bound. A zero here is
    // not a pass on its own: with nothing regressed it means no acked write
    // was ever at risk, so the RPO check had nothing to judge; with losses
    // present it means they were all acked at or after the death (BUG-0120).
    // The branches below distinguish the two rather than printing one line.
    if beyond_cap > 0 {
        println!(
            "  NOTE: {beyond_cap} lost acked write(s) were older than the {lag_hard_ms}ms cap. \
             That is permitted: the cap bounds the VOLUME at risk (past it the master stops \
             accepting), not the AGE of a write already acked while replication was healthy."
        );
    }
    // ALWAYS PRINT THE COUNT, INCLUDING ZERO.
    //
    // The NOTE below fires only when there are ties, so a run with none and a
    // run whose classification never executed printed exactly the same thing:
    // nothing. Reading 100 gate runs backwards to see whether this bug still
    // fires, the absence of this line had to be interpreted by opening the
    // source -- and "absent" versus "zero" is the distinction this whole file
    // exists to keep. One unconditional line makes the log say which it is.
    println!("  boundary ties (send == death stamp): {ambiguous_at_boundary}");
    if ambiguous_at_boundary > 0 {
        println!(
            "  NOTE: {ambiguous_at_boundary} acked write(s) carried a send stamp equal to the \
             death stamp, to the microsecond. Which master served them is not recorded, so they \
             were excluded from the loss measurements rather than attributed to either. This is \
             expected to be rare; if it is not, the clock is coarser than the race."
        );
    }
    // ALWAYS PRINTED, INCLUDING ZERO — same reason as the boundary-tie count
    // above. A run where no regression escaped the depth measures and a run
    // whose accounting never executed must not look identical.
    println!(
        "  regressions no depth measure could judge: {lost_unattributed} \
         (post-death surplus acks seen: {post_death_surplus})"
    );
    if lost_unattributed > 0 {
        println!(
            "  NOTE: {lost_unattributed} of the regressed key(s) had NO ack above the survivor's \
             value that was sent before the death stamp. Every such ack was sent after the master \
             was already dead, so `classify_send` returned NewMaster and both the depth and the \
             cap check skipped it. The RPO numbers above say nothing about these keys, and this \
             is not the async tail: a write acked after the old master died and then absent from \
             the survivor is the NEW master failing to hold what it acknowledged. Judge it \
             separately (BUG-0120)."
        );
    }
    if deepest_loss_ms == 0 {
        // WHAT DEPTH 0 MEANS DEPENDS ON WHETHER LAG WAS FORCED.
        //
        // This printed one line regardless, advising --stall-replica-ms even
        // to a run that had just passed it. Worse than noise: under a stall,
        // depth 0 is the CORRECT outcome when shedding works, because the
        // master refuses writes rather than acking ones it cannot replicate
        // — so the line called a run deficient for demonstrating the thing it
        // set out to demonstrate. Seen on lag_cap, which passes
        // --stall-replica-ms 200 (BUG-0049) and shed 70 writes on the run
        // that printed it.
        if acked_lost_total > 0 {
            // BUG-0120: this branch used to be unreachable-in-practice noise
            // and instead printed "replication kept up throughout" on a run
            // that had just regressed 80 acked keys, because the depth was
            // measured from a stamp taken BEFORE the kill was sent and every
            // loss saturated to 0. With the anchor corrected, depth 0 next to
            // a non-zero loss count is a real and specific statement — every
            // lost write was acked at or after the death stamp — and it is
            // not evidence that replication kept up.
            println!(
                "  NOTE: loss depth 0 with {acked_lost_total} acked key(s) regressed: every lost \
                 write was acked at or after the death stamp, so none of them aged unreplicated \
                 for a measurable interval. This says nothing about whether replication kept up \
                 generally — it says the losses were writes in flight at the kill."
            );
        } else if stall_replica_ms == 0 {
            println!(
                "  NOTE: loss depth 0 and nothing regressed — replication kept up throughout, so \
                 the RPO bound was not exercised by this run (try --stall-replica-ms)"
            );
        } else if throttled_total > 0 {
            println!(
                "  NOTE: loss depth 0 under a {stall_replica_ms}ms replica stall, with \
                 {throttled_total} write(s) shed: the master refused writes it could not \
                 replicate rather than acking them. That is the bound holding, not an \
                 unexercised path."
            );
        } else {
            println!(
                "  NOTE: loss depth 0 under a {stall_replica_ms}ms replica stall AND nothing \
                 shed — the stall produced neither loss nor throttling, so neither mechanism \
                 was exercised. Check the stall actually reached the replica."
            );
        }
    }
    if throttled_total == 0 {
        println!(
            "  NOTE: nothing was shed — the lag cap never bit, so the mechanism the bound \
             RESTS on is unproven by this run (try a smaller --lag-hard-ms)"
        );
    }
    println!("  final walk: {present} present, {missing} missing-or-regressed");
    // THE VOLUME BOUND — the one the product actually promises, and the one
    // the M2 exit names ("measured RPO <= 1 s worth of writes, a volume, not
    // an age"). Always printed, including zero, so "no breach" and "never
    // judged" cannot look alike.
    println!(
        "  RPO volume: {lost_writes_total} acked write(s) lost across master kills; \
         {volume_breaches} kill(s) over budget, {volume_unjudged} unjudged"
    );
    if volume_unjudged > 0 {
        println!(
            "  NOTE: {volume_unjudged} kill(s) could not be judged — the healthy window before \
             them was under 200ms or carried no writes, so no rate could be estimated. Those \
             kills are NOT evidence the bound held."
        );
    }
    if volume_breaches > 0 {
        println!(
            "  FAIL: {volume_breaches} kill(s) lost more acked writes than one \
             {}ms window's worth at the rate observed just before them. That is the bound the \
             product does promise — past the cap the master stops ACCEPTING, so the at-risk set \
             cannot exceed one cap-window's arrivals. Each breaching kill printed its own rate, \
             window and budget above.",
            lag_hard_ms + rpo_margin_ms
        );
        std::process::exit(1);
    }
}

/// How many acked writes may be lost in ONE failover, at the rate observed
/// just before it.
///
/// The bound the product promises is a VOLUME, not an age (docs/failover.md,
/// slo.md): past the lag cap the master stops ACCEPTING, so the set of acked
/// but unreplicated writes cannot exceed one cap-window's arrivals. An already
/// acked write can then age without limit behind a stalled replica — which is
/// why the age assertion that used to live here was removed — but no NEW one
/// joins it.
///
/// `None` means the window could not support an estimate: too short, or no
/// writes in it. That is deliberately not a budget of zero and not a pass;
/// callers must count it as unjudged and say so.
///
/// Integer arithmetic rounding UP, because a rate under one write per cap
/// window would otherwise floor to a budget of 0 and turn a single ordinary
/// loss into a breach.
fn rpo_volume_budget(win_writes: u64, win_ms: u64, cap_ms: u64) -> Option<u64> {
    if win_ms < 200 || win_writes == 0 {
        return None;
    }
    Some(win_writes.saturating_mul(cap_ms).div_ceil(win_ms))
}

/// How long before the master's DEATH an acked write was acknowledged.
///
/// BUG-0120: the anchor is the whole content of this function. It was
/// `kill_ms`, stamped before `kill_master_hot()` is called; on
/// `Target::Attached` that call makes a master-discovery round trip and then
/// an SSH kill before stamping `dead_us`, so a write acked in between was
/// served by a master still alive and `saturating_sub` floored its depth to 0.
/// Measured windows of 711ms, 720ms and 3304ms on one 5-host soak, against a
/// 1000ms cap — so the depth read 0 over exactly the interval where the loss
/// happens.
///
/// `dead_us` is stamped just AFTER the SIGKILL returned, so death <= dead_us
/// and this over-estimates. That is the safe direction: it can call a loss
/// deeper than it was, it cannot call a deep loss shallow.
fn loss_depth_ms(dead_us: u64, acked_ms: u64) -> u64 {
    (dead_us / 1000).saturating_sub(acked_ms)
}

#[cfg(test)]
mod quiesce_tests {
    use super::*;

    fn scratch(tag: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("flint-quiesce-test-{}-{tag}", std::process::id()));
        p.to_string_lossy().into_owned()
    }

    /// The guard has to actually create the file, or the feeders never hear
    /// about it and the convergence gate stays the coin flip #183 describes.
    #[test]
    fn arming_creates_the_file_and_dropping_removes_it() {
        let path = scratch("basic");
        let _ = std::fs::remove_file(&path);
        {
            let _q = Quiesce::begin(&path);
            assert!(
                std::path::Path::new(&path).exists(),
                "quiesce file was not created, so nothing would hold off"
            );
        }
        assert!(
            !std::path::Path::new(&path).exists(),
            "quiesce file outlived the guard — ingestion would never resume"
        );
    }

    /// A panic is the path that matters. flint-chaos aborts the whole run on a
    /// failed gate, and if the file survived that, the next thing to look at
    /// the fleet would find it idle and conclude the load was broken.
    #[test]
    fn a_panic_while_quiesced_still_resumes_the_load() {
        let path = scratch("panic");
        let _ = std::fs::remove_file(&path);
        let p = path.clone();
        let caught = std::panic::catch_unwind(move || {
            let _q = Quiesce::begin(&p);
            panic!("the convergence gate failing, as it did in run 21");
        });
        assert!(caught.is_err(), "the test's own panic did not happen");
        assert!(
            !std::path::Path::new(&path).exists(),
            "a panic left the fleet quiesced: the soak would stop ingesting and \
             still report its feeders alive"
        );
    }

    /// No --quiesce-file is the historical behaviour (nothing external to
    /// coordinate with) and must not litter the filesystem.
    #[test]
    fn an_empty_path_arms_nothing() {
        let q = Quiesce::begin("");
        assert!(q.0.is_none(), "an empty path should arm no file at all");
    }
}

/// ADR-0018 item 9. The edge-dial hint, which is the only thing standing
/// between an undialable edge and a reader who goes to debug the fleet.
#[cfg(test)]
mod edge_hint_tests {
    use super::edge_hint;

    /// The plaintext case is the one that produced the bug: chaos dialled a
    /// client-TLS proxy without TLS, and the failure read as "the proxy never
    /// recovered".
    #[test]
    fn the_plaintext_hint_names_the_dial_and_the_flag() {
        let h = edge_hint("127.0.0.1:7193", "");
        assert!(h.contains("127.0.0.1:7193"), "{h}");
        assert!(
            h.contains("--edge-ca"),
            "the fix has to be in the message: {h}"
        );
        assert!(h.contains("PLAINTEXT"), "{h}");
        assert!(h.contains("Suspect the dial before the fleet"), "{h}");
    }

    /// With TLS configured the likely cause is different — a SAN that does
    /// not cover the dialled host — so pointing at --edge-ca would be
    /// misdirection of the same kind, one step along.
    #[test]
    fn the_tls_hint_points_at_sans_not_at_the_flag_already_set() {
        let h = edge_hint("10.0.0.1:7002", "/etc/flint/ca.crt");
        assert!(
            h.contains("10.0.0.1:7002") && h.contains("/etc/flint/ca.crt"),
            "{h}"
        );
        assert!(h.contains("SANs"), "{h}");
        assert!(
            !h.contains("PLAINTEXT"),
            "must not blame plaintext when TLS is configured: {h}"
        );
        assert!(h.contains("Suspect the dial before the fleet"), "{h}");
    }

    /// Both branches carry the phrase the drill's negative control greps
    /// for. Without this the drill could pass while the message it is
    /// asserting on had drifted to something else.
    #[test]
    fn both_branches_carry_the_phrase_the_drill_asserts_on() {
        for ca in ["", "/tmp/ca.crt"] {
            assert!(
                edge_hint("h:1", ca).contains("Suspect the dial before the fleet"),
                "ca={ca:?}"
            );
        }
    }
}

#[cfg(test)]
mod loss_depth_anchor {
    use super::loss_depth_ms;

    // The real stamps from soak-20260907T043640Z iter 603, the kill that lost
    // 24 acked keys. The harness stamped kill_ms 3304ms before the SSH kill
    // landed. A write acked anywhere in that window was served by a live
    // master and then lost.
    const KILL_MS: u64 = 1_788_758_279_425;
    const DEAD_US: u64 = 1_788_758_282_729_372;

    #[test]
    fn a_write_acked_between_the_stamp_and_the_death_has_a_depth() {
        let acked = KILL_MS + 4; // just after the harness stamped, long before death
        let depth = loss_depth_ms(DEAD_US, acked);
        assert_eq!(depth, 3300, "depth must be measured from the death");
        // THE CONTROL: this is what the old anchor returned for the same
        // write, and why the run reported `deepest acked-write loss: 0ms`
        // while regressing 80 keys. If loss_depth_ms is ever re-anchored to
        // kill_ms the assert above fails, because this is what it would give.
        assert_eq!(KILL_MS.saturating_sub(acked), 0);
    }

    #[test]
    fn the_window_bound_settles_the_two_smaller_kills_and_not_the_third() {
        // iters 610 and 645: windows of 720ms and 711ms, both inside the
        // 1000ms cap, so every write lost there is within it on the strict
        // age reading. iter 603's 3304ms window is not.
        let cap = 1000u64;
        for (dead_us, kill_ms, settled) in [
            (1_788_758_310_658_495u64, 1_788_758_309_938u64, true),
            (1_788_758_440_015_308, 1_788_758_439_304, true),
            (DEAD_US, KILL_MS, false),
        ] {
            // Deepest a write acked at-or-after `kill_ms` can possibly be.
            let worst = loss_depth_ms(dead_us, kill_ms);
            assert_eq!(worst < cap, settled, "window {worst}ms against cap {cap}ms");
        }
    }

    #[test]
    fn an_ack_after_the_death_floors_at_zero_rather_than_underflowing() {
        assert_eq!(loss_depth_ms(DEAD_US, DEAD_US / 1000 + 50), 0);
    }
}

#[cfg(test)]
mod rpo_volume {
    use super::rpo_volume_budget;

    const CAP: u64 = 1_500; // 1000ms hard cap + 500ms margin, the defaults

    #[test]
    fn budget_is_one_cap_window_at_the_observed_rate() {
        // 1000 writes/s for 4s: a 1.5s window is worth 1500 writes.
        assert_eq!(rpo_volume_budget(4_000, 4_000, CAP), Some(1_500));
        // Double the rate, double the budget.
        assert_eq!(rpo_volume_budget(8_000, 4_000, CAP), Some(3_000));
    }

    #[test]
    fn a_slow_writer_still_gets_a_budget_of_at_least_one() {
        // THE ROUNDING THAT MATTERS. One write in 10s is 0.15 writes per cap
        // window; flooring gives 0, and a budget of 0 makes any single loss a
        // breach — a harness that red-lights on the engine behaving normally,
        // which is exactly why the previous assertion here was deleted.
        assert_eq!(rpo_volume_budget(1, 10_000, CAP), Some(1));
        assert_eq!(rpo_volume_budget(3, 10_000, CAP), Some(1));
    }

    #[test]
    fn an_unusable_window_is_none_rather_than_zero() {
        assert_eq!(rpo_volume_budget(500, 199, CAP), None, "window too short");
        assert_eq!(rpo_volume_budget(0, 60_000, CAP), None, "no writes to rate");
        // The boundary is judged, not skipped.
        assert!(rpo_volume_budget(500, 200, CAP).is_some());
    }

    #[test]
    fn a_breach_is_a_loss_above_one_windows_arrivals() {
        // 10k writes in 5s = 2000/s, so a 1.5s window is worth 3000.
        let b = rpo_volume_budget(10_000, 5_000, CAP).expect("rateable");
        assert_eq!(b, 3_000);
        assert!(3_000 <= b, "one window's worth is within the bound");
        assert!(3_001 > b, "one more than a window's worth is a breach");
    }
}
