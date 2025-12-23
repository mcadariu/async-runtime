//! A simple async runtime built from scratch for learning purposes.
//!
//! This runtime includes:
//! - Multi-threaded executor with work stealing
//! - I/O reactor using mio (epoll/kqueue)
//! - Async TCP networking primitives
//! - Timer support
//!
//! # Example
//! ```no_run
//! use async_runtime::Runtime;
//!
//! let rt = Runtime::new();
//! rt.block_on(async {
//!     println!("Hello from async runtime!");
//! });
//! ```

mod executor;
mod reactor;
mod task;
mod waker;
mod net;
mod time;

pub use executor::{spawn, Runtime};
pub use net::{TcpListener, TcpStream};
pub use time::sleep;

use std::future::Future;

/// Spawns a task on the current runtime.
pub fn spawn_task<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    spawn(future);
}
