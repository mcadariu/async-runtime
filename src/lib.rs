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

pub fn spawn_task<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    spawn(future);
}
