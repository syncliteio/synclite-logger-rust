//! Core types and errors shared across all SyncLite logger crates.
//!
//! This crate has **zero database dependencies** and is pulled in by every
//! other crate in the workspace. Keep it small and stable.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod ids;
pub mod record;
pub mod sql_policy;

pub use error::{Error, Result};
pub use ids::{Backend, CommitId, DeviceId, DeviceType, OperationId, SegmentSequence};

