//! Node manifests: durable slot-ownership and role claims with fencing
//! epochs.
//!
//! CORRECTNESS-CRITICAL. The manifest is the durable truth the metadata
//! layer is rebuilt from (docs/design.md §2.3): each node persists what it
//! owns and what role it holds, stamped with epochs. Epochs are
//! `(generation, counter)` tuples compared lexicographically — the control
//! plane bumps `generation` on inter-group moves, the group trio bumps
//! `counter` locally; exactly one writer per component.
//!
//! Fencing rule: a role or claim write must carry a STRICTLY greater epoch
//! than the stored one. Equal is rejected — two actors deciding "epoch 7"
//! independently must not both succeed. The rejection returns the current
//! epoch so a stale actor learns it lost.

use crate::Kv;

/// System rows live outside the user envelope space ('M'/'S'/'Z').
pub const ROLE_KEY: &[u8] = b"\x00flint\x00role";
pub const CLAIM_KEY_PREFIX: &[u8] = b"\x00flint\x00claim\x00";

/// Fencing epoch: lexicographic (generation, counter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Epoch {
    pub generation: u32,
    pub counter: u32,
}

impl Epoch {
    pub const ZERO: Epoch = Epoch {
        generation: 0,
        counter: 0,
    };

    pub fn encode(&self) -> [u8; 8] {
        let mut out = [0u8; 8];
        out[..4].copy_from_slice(&self.generation.to_be_bytes());
        out[4..].copy_from_slice(&self.counter.to_be_bytes());
        out
    }

    pub fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() != 8 {
            return None;
        }
        Some(Self {
            generation: u32::from_be_bytes(raw[..4].try_into().ok()?),
            counter: u32::from_be_bytes(raw[4..].try_into().ok()?),
        })
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({},{})", self.generation, self.counter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Master = 1,
    Replica = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleClaim {
    pub role: Role,
    pub epoch: Epoch,
}

/// Slot-range ownership for one namespace (v0: one contiguous range).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotClaim {
    pub ns: Vec<u8>,
    pub start: u16,
    pub end: u16, // inclusive
    pub epoch: Epoch,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ManifestError {
    /// The write carried an epoch <= the stored one; contains the stored
    /// epoch so the losing actor learns the fence it hit.
    Fenced {
        current: Epoch,
    },
    Corrupt,
}

pub fn read_role(kv: &dyn Kv) -> Option<RoleClaim> {
    let raw = kv.get(ROLE_KEY)?;
    if raw.len() != 9 {
        return None;
    }
    let role = match raw[0] {
        1 => Role::Master,
        2 => Role::Replica,
        _ => return None,
    };
    Some(RoleClaim {
        role,
        epoch: Epoch::decode(&raw[1..])?,
    })
}

/// Fenced role transition: `new.epoch` must strictly exceed the stored
/// epoch. The first claim on a fresh node must still exceed Epoch::ZERO.
pub fn set_role(kv: &dyn Kv, new: RoleClaim) -> Result<(), ManifestError> {
    let current = read_role(kv).map(|c| c.epoch).unwrap_or(Epoch::ZERO);
    if new.epoch <= current {
        return Err(ManifestError::Fenced { current });
    }
    let mut raw = Vec::with_capacity(9);
    raw.push(new.role as u8);
    raw.extend_from_slice(&new.epoch.encode());
    kv.put(ROLE_KEY, &raw);
    Ok(())
}

/// UNFENCED role write — bootstrap only. A checkpoint full sync copies the
/// SOURCE node's manifest (role included); the seeded node must reassert
/// its own identity before serving. Never use this for role transitions —
/// promotions go through the fenced `set_role`.
pub fn force_role(kv: &dyn Kv, claim: RoleClaim) {
    let mut raw = Vec::with_capacity(9);
    raw.push(claim.role as u8);
    raw.extend_from_slice(&claim.epoch.encode());
    kv.put(ROLE_KEY, &raw);
}

fn claim_key(ns: &[u8]) -> Vec<u8> {
    let mut k = CLAIM_KEY_PREFIX.to_vec();
    k.extend_from_slice(ns);
    k
}

pub fn read_claim(kv: &dyn Kv, ns: &[u8]) -> Option<SlotClaim> {
    let raw = kv.get(&claim_key(ns))?;
    if raw.len() != 12 {
        return None;
    }
    Some(SlotClaim {
        ns: ns.to_vec(),
        start: u16::from_be_bytes(raw[..2].try_into().ok()?),
        end: u16::from_be_bytes(raw[2..4].try_into().ok()?),
        epoch: Epoch::decode(&raw[4..])?,
    })
}

/// Fenced ownership write, same strict-monotonicity rule as roles.
pub fn set_claim(kv: &dyn Kv, claim: &SlotClaim) -> Result<(), ManifestError> {
    let current = read_claim(kv, &claim.ns)
        .map(|c| c.epoch)
        .unwrap_or(Epoch::ZERO);
    if claim.epoch <= current {
        return Err(ManifestError::Fenced { current });
    }
    let mut raw = Vec::with_capacity(12);
    raw.extend_from_slice(&claim.start.to_be_bytes());
    raw.extend_from_slice(&claim.end.to_be_bytes());
    raw.extend_from_slice(&claim.epoch.encode());
    kv.put(&claim_key(&claim.ns), &raw);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;

    #[test]
    fn epoch_ordering_is_lexicographic() {
        let e = |g, c| Epoch {
            generation: g,
            counter: c,
        };
        assert!(e(0, 1) > Epoch::ZERO);
        assert!(e(0, 2) > e(0, 1));
        assert!(e(1, 0) > e(0, 999), "generation dominates counter");
        assert!(e(2, 0) > e(1, u32::MAX));
        assert_eq!(Epoch::decode(&e(3, 7).encode()), Some(e(3, 7)));
    }

    #[test]
    fn role_fencing_rejects_stale_and_equal_epochs() {
        let kv = MemKv::new();
        assert_eq!(read_role(&kv), None);
        let e = |g, c| Epoch {
            generation: g,
            counter: c,
        };
        set_role(
            &kv,
            RoleClaim {
                role: Role::Replica,
                epoch: e(0, 1),
            },
        )
        .expect("first claim");
        // Equal epoch: fenced.
        assert_eq!(
            set_role(
                &kv,
                RoleClaim {
                    role: Role::Master,
                    epoch: e(0, 1)
                }
            ),
            Err(ManifestError::Fenced { current: e(0, 1) })
        );
        // Lower epoch: fenced.
        assert_eq!(
            set_role(
                &kv,
                RoleClaim {
                    role: Role::Master,
                    epoch: Epoch::ZERO
                }
            ),
            Err(ManifestError::Fenced { current: e(0, 1) })
        );
        // Strictly greater: accepted; the promotion is durable.
        set_role(
            &kv,
            RoleClaim {
                role: Role::Master,
                epoch: e(0, 2),
            },
        )
        .expect("promotion");
        assert_eq!(
            read_role(&kv),
            Some(RoleClaim {
                role: Role::Master,
                epoch: e(0, 2)
            })
        );
        // Zero epoch can never be written (must exceed ZERO).
        let fresh = MemKv::new();
        assert!(matches!(
            set_role(
                &fresh,
                RoleClaim {
                    role: Role::Master,
                    epoch: Epoch::ZERO
                }
            ),
            Err(ManifestError::Fenced { .. })
        ));
    }

    #[test]
    fn force_role_overwrites_for_bootstrap() {
        let kv = MemKv::new();
        let e = |g, c| Epoch {
            generation: g,
            counter: c,
        };
        // Simulates a replica seeded from a master checkpoint: the copied
        // role says Master (0,5); the seeded node reasserts Replica at the
        // same epoch, and future promotion still requires exceeding it.
        set_role(
            &kv,
            RoleClaim {
                role: Role::Master,
                epoch: e(0, 5),
            },
        )
        .expect("copied role");
        force_role(
            &kv,
            RoleClaim {
                role: Role::Replica,
                epoch: e(0, 5),
            },
        );
        assert_eq!(
            read_role(&kv),
            Some(RoleClaim {
                role: Role::Replica,
                epoch: e(0, 5)
            })
        );
        assert!(matches!(
            set_role(
                &kv,
                RoleClaim {
                    role: Role::Master,
                    epoch: e(0, 5)
                }
            ),
            Err(ManifestError::Fenced { .. })
        ));
        set_role(
            &kv,
            RoleClaim {
                role: Role::Master,
                epoch: e(0, 6),
            },
        )
        .expect("promotion above copied epoch");
    }

    #[test]
    fn slot_claims_roundtrip_with_fencing() {
        let kv = MemKv::new();
        let e = |g, c| Epoch {
            generation: g,
            counter: c,
        };
        let claim = SlotClaim {
            ns: b"0".to_vec(),
            start: 0,
            end: 16383,
            epoch: e(0, 1),
        };
        set_claim(&kv, &claim).expect("first claim");
        assert_eq!(read_claim(&kv, b"0"), Some(claim.clone()));
        // Shrinking the range requires a higher epoch.
        let shrunk = SlotClaim {
            start: 0,
            end: 8191,
            epoch: e(0, 1),
            ..claim.clone()
        };
        assert!(matches!(
            set_claim(&kv, &shrunk),
            Err(ManifestError::Fenced { .. })
        ));
        let shrunk_ok = SlotClaim {
            epoch: e(1, 0),
            ..shrunk
        };
        set_claim(&kv, &shrunk_ok).expect("re-claim at higher epoch");
        assert_eq!(read_claim(&kv, b"0").expect("claim").end, 8191);
        // Claims are per-namespace.
        assert_eq!(read_claim(&kv, b"other"), None);
    }
}
