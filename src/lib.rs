//! # cortex9-raft-log-store
//!
//! Raft consensus core as a pure, deterministic state machine (`domain`),
//! storage/clock/randomness ports (`ports`), an application driver
//! (`application`) and in-memory/file adapters (`adapters`).
//! See `CONTRACT.md` for invariants and scope.

#![forbid(unsafe_code)]

pub mod adapters;
pub mod application;
pub mod domain;
pub mod ports;
