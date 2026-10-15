//! Starting child processes one at a time.
//!
//! On macOS, Rust creates a child's pipes with `pipe()`, then marks them close-on-exec in a
//! separate step. A child that another thread starts in between inherits them. If a
//! long-lived server inherits a crashed server's stdout, that stdout never reaches end of
//! file, and the attempt to connect to the crashed server hangs until it times out. Linux
//! creates pipes atomically with `pipe2`, so there the lock only costs a moment.

use std::sync::{Mutex, MutexGuard, PoisonError};

static SPAWNING: Mutex<()> = Mutex::new(());

/// Hold this while starting a child process, and only for that: it's a blocking lock
pub fn lock() -> MutexGuard<'static, ()> {
    SPAWNING.lock().unwrap_or_else(PoisonError::into_inner)
}
