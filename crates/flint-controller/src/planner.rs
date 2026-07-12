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
}
