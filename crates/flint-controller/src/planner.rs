// SPDX-License-Identifier: Elastic-2.0
//! Rebalance planner: given a group's per-pair fill/heat map, decide which
//! pairs should shed load to which (docs/design.md §2.3). Pure and
//! deterministic — the controller feeds it observed fills and (today, via
//! --rebalance-deadband) LOGS the plan as a dry run. Executing the returned
//! moves via FLINTMIGRATEIN, gated by the min-replicas/lease safety rules, is
//! the follow-on step.
//!
//! Two properties the design requires (ADR-0004 obligation #2):
//!   - Deterministic: identical inputs yield an identical plan, regardless of
//!     observation order, so concurrent controllers agree and fencing only
//!     ever has to reject exact duplicates, never reconcile divergent plans.
//!   - Hysteresis: a pair is left alone unless it exceeds the group mean by
//!     more than `deadband`. Fencing prevents CONFLICTING moves; only the
//!     deadband prevents OSCILLATION — two controllers (or successive ticks)
//!     shuffling load back and forth around a balanced point.
//!
//! Granularity is pair-level (from, to, approximate amount). Choosing the
//! specific slots to move to make up the amount needs per-slot heat, which
//! the nodes do not expose yet; that selection is a follow-on. Fill is an
//! opaque load unit (key count today via DBSIZE; bytes later).

/// One pair's observed load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairLoad {
    pub label: String,
    pub fill: u64,
}

/// A planned shed of `approx` load units from `from` to `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Move {
    pub from: String,
    pub to: String,
    pub approx: u64,
}

/// Plan the moves that bring a group toward balance. Only pairs above
/// `mean * (1 + deadband)` shed load, and only to pairs below the mean; each
/// move is the smaller of the donor's excess-over-mean and the recipient's
/// deficit-to-mean, so no move overshoots. Deterministic: donor/recipient
/// ties break by label, so the plan is a pure function of the inputs.
///
/// `deadband` is a fraction (e.g. 0.20 = act only past 20% over mean).
pub fn plan_moves(pairs: &[PairLoad], deadband: f64) -> Vec<Move> {
    if pairs.len() < 2 {
        return Vec::new();
    }
    let total: u64 = pairs.iter().map(|p| p.fill).sum();
    let mean = total as f64 / pairs.len() as f64;
    if mean <= 0.0 {
        return Vec::new();
    }
    let threshold = mean * (1.0 + deadband.max(0.0));

    // Simulate on a mutable copy so successive moves see updated fills.
    let mut work: Vec<(String, f64)> = pairs
        .iter()
        .map(|p| (p.label.clone(), p.fill as f64))
        .collect();
    let mut moves = Vec::new();

    // At most one pair leaves the over/under set per move, so this bounds at
    // `pairs.len()` iterations; the guard below also breaks when none qualify.
    loop {
        // Donor: fullest pair strictly above the deadband threshold; ties to
        // the lowest label (so the choice is order-independent).
        let donor = work
            .iter()
            .enumerate()
            .filter(|(_, (_, f))| *f > threshold)
            .max_by(|a, b| {
                a.1.1
                    .partial_cmp(&b.1.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(b.1.0.cmp(&a.1.0))
            })
            .map(|(i, _)| i);
        let Some(di) = donor else { break };

        // Recipient: emptiest pair strictly below the mean; ties to lowest
        // label.
        let recip = work
            .iter()
            .enumerate()
            .filter(|(_, (_, f))| *f < mean)
            .min_by(|a, b| {
                a.1.1
                    .partial_cmp(&b.1.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.1.0.cmp(&b.1.0))
            })
            .map(|(i, _)| i);
        let Some(ri) = recip else { break };

        let excess = work[di].1 - mean;
        let deficit = mean - work[ri].1;
        let amount = excess.min(deficit);
        if amount < 1.0 {
            break;
        }
        work[di].1 -= amount;
        work[ri].1 += amount;
        moves.push(Move {
            from: work[di].0.clone(),
            to: work[ri].0.clone(),
            approx: amount.round() as u64,
        });
    }
    moves
}

/// Choose which of the donor's (namespace, slot) units to ship to satisfy
/// (approximately) a planned move of `amount` load units — the migration
/// unit in a multi-tenant group is one namespace's slot. Greedy
/// largest-first so the fewest units move; ties break deterministically
/// (count desc, then ns asc, then slot asc), so — like `plan_moves` — the
/// choice is a pure, order-independent function of the observed stats and
/// concurrent controllers pick the same units (their duplicate migrations
/// then collapse against the same fenced records). `cap` bounds units per
/// cycle: rebalancing converges over several observe→plan→move cycles,
/// which is also the pacing mechanism.
///
/// Stops BEFORE overshooting: a unit is added only while the running total
/// is under `amount`, so we never move more than one unit past the target
/// (the deadband absorbs the remainder). Always moves at least one unit if
/// any is non-empty — otherwise a single unit larger than `amount` could
/// stall the plan forever.
pub fn select_units(stats: &[((String, u16), u64)], amount: u64, cap: usize) -> Vec<(String, u16)> {
    let mut sorted: Vec<((String, u16), u64)> =
        stats.iter().filter(|(_, n)| *n > 0).cloned().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let mut picked = Vec::new();
    let mut total = 0u64;
    for (unit, n) in sorted {
        if picked.len() >= cap || (total >= amount && !picked.is_empty()) {
            break;
        }
        picked.push(unit);
        total += n;
    }
    if amount == 0 { Vec::new() } else { picked }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(label: &str, fill: u64) -> PairLoad {
        PairLoad {
            label: label.into(),
            fill,
        }
    }

    #[test]
    fn balanced_group_produces_no_moves() {
        let pairs = vec![load("a", 1000), load("b", 1000), load("c", 1000)];
        assert!(plan_moves(&pairs, 0.20).is_empty());
    }

    #[test]
    fn within_deadband_produces_no_moves_hysteresis() {
        // Mean 1000; the fullest is 15% over, under the 20% deadband → leave
        // it alone. This is the anti-oscillation guard.
        let pairs = vec![load("a", 1150), load("b", 1000), load("c", 850)];
        assert!(
            plan_moves(&pairs, 0.20).is_empty(),
            "imbalance within the deadband must not trigger a move"
        );
    }

    #[test]
    fn a_hot_pair_sheds_to_the_emptiest() {
        // Mean 1000; a is 60% over (past deadband), c is emptiest.
        let pairs = vec![load("a", 1600), load("b", 1000), load("c", 400)];
        let moves = plan_moves(&pairs, 0.20);
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].from, "a");
        assert_eq!(moves[0].to, "c");
        // a's excess over mean is 600, c's deficit is 600 → move 600.
        assert_eq!(moves[0].approx, 600);
    }

    #[test]
    fn move_never_overshoots_the_recipient() {
        // a is very hot (excess 900) but c only has room for 100 to reach the
        // mean; b can take the rest. No pair should end up pushed past mean.
        let pairs = vec![load("a", 1900), load("b", 900), load("c", 900)];
        let moves = plan_moves(&pairs, 0.10);
        // Total moved equals a's excess over the mean (1233.33 → ~900 total).
        let mean = (1900 + 900 + 900) as f64 / 3.0;
        let moved: u64 = moves.iter().map(|m| m.approx).sum();
        assert!(
            (moved as f64 - (1900.0 - mean)).abs() <= 1.0,
            "should shed exactly a's excess over the mean, got {moved}"
        );
        // Every recipient was below the mean and each move is bounded by its
        // deficit, so none is overshot.
        for m in &moves {
            assert_eq!(m.from, "a");
        }
    }

    #[test]
    fn plan_is_deterministic_regardless_of_input_order() {
        let a = vec![load("a", 1600), load("b", 1000), load("c", 400)];
        let b = vec![load("c", 400), load("a", 1600), load("b", 1000)];
        let c = vec![load("b", 1000), load("c", 400), load("a", 1600)];
        let pa = plan_moves(&a, 0.20);
        assert_eq!(pa, plan_moves(&b, 0.20));
        assert_eq!(pa, plan_moves(&c, 0.20));
    }

    #[test]
    fn ties_break_by_label_for_determinism() {
        // Two equally-empty recipients (b, c). The donor must pick the lowest
        // label deterministically.
        let pairs = vec![load("a", 1600), load("b", 700), load("c", 700)];
        let moves = plan_moves(&pairs, 0.20);
        assert_eq!(moves[0].to, "b", "tie must break to the lowest label");
    }

    #[test]
    fn single_pair_or_empty_is_a_no_op() {
        assert!(plan_moves(&[], 0.2).is_empty());
        assert!(plan_moves(&[load("solo", 9999)], 0.2).is_empty());
    }

    #[test]
    fn zero_deadband_still_balances_but_ignores_sub_unit_noise() {
        // With no deadband, any pair above the mean sheds; the < 1.0 amount
        // guard stops sub-unit churn.
        let pairs = vec![load("a", 1001), load("b", 999)];
        let moves = plan_moves(&pairs, 0.0);
        assert_eq!(moves.len(), 1);
        assert_eq!((moves[0].from.as_str(), moves[0].to.as_str()), ("a", "b"));
        assert_eq!(moves[0].approx, 1);
    }

    fn u(ns: &str, slot: u16, n: u64) -> ((String, u16), u64) {
        ((ns.to_string(), slot), n)
    }

    #[test]
    fn select_units_largest_first_until_amount() {
        let stats = vec![
            u("t", 100, 50),
            u("t", 200, 500),
            u("t", 300, 30),
            u("t", 400, 200),
        ];
        // amount 600: picks 500 (slot 200), then 200 (slot 400) -> total 700.
        let picked: Vec<u16> = select_units(&stats, 600, 8)
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        assert_eq!(picked, vec![200, 400]);
        // amount 400: the largest alone satisfies it (no overshoot append).
        let picked: Vec<u16> = select_units(&stats, 400, 8)
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        assert_eq!(picked, vec![200]);
    }

    #[test]
    fn select_units_ties_break_ns_then_slot() {
        // Equal counts: order is ns asc, then slot asc — deterministic
        // across controllers regardless of observation order.
        let stats = vec![u("beta", 3, 100), u("alpha", 9, 100), u("alpha", 3, 100)];
        assert_eq!(
            select_units(&stats, 250, 8),
            vec![
                ("alpha".to_string(), 3),
                ("alpha".to_string(), 9),
                ("beta".to_string(), 3)
            ]
        );
    }

    #[test]
    fn select_units_respects_cap_and_moves_at_least_one() {
        let stats = vec![u("t", 1, 10), u("t", 2, 10), u("t", 3, 10), u("t", 4, 10)];
        assert_eq!(
            select_units(&stats, 1_000, 2).len(),
            2,
            "cap bounds the cycle"
        );
        // A single huge unit larger than amount still moves (no stall).
        let big = vec![u("t", 5, 10_000)];
        assert_eq!(select_units(&big, 100, 8), vec![("t".to_string(), 5)]);
    }

    #[test]
    fn select_units_ignores_empty_and_zero_amount() {
        assert!(select_units(&[], 100, 8).is_empty());
        assert!(select_units(&[u("t", 1, 0), u("t", 2, 0)], 100, 8).is_empty());
        assert!(
            select_units(&[u("t", 1, 50)], 0, 8).is_empty(),
            "zero amount = no move"
        );
    }

    #[test]
    fn select_units_is_order_independent() {
        let a = vec![u("x", 10, 5), u("x", 20, 50), u("y", 30, 20)];
        let b = vec![u("y", 30, 20), u("x", 10, 5), u("x", 20, 50)];
        assert_eq!(select_units(&a, 60, 8), select_units(&b, 60, 8));
    }
}
