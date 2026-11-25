use crate::ports::Rand;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

/// Deterministic xorshift64* generator, for simulations and tests.
#[derive(Clone, Debug)]
pub struct SeededRand(u64);

impl SeededRand {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
}

impl Rand for SeededRand {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// Non-deterministic generator seeded from the process' hash randomness.
/// Not cryptographic: only used to jitter election timeouts.
#[derive(Debug)]
pub struct OsRand(SeededRand);

impl Default for OsRand {
    fn default() -> Self {
        let mut h = RandomState::new().build_hasher();
        h.write_u64(std::process::id() as u64);
        Self(SeededRand::new(h.finish()))
    }
}

impl Rand for OsRand {
    fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }
}
