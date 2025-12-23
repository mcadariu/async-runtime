use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// A task is a Future that can be polled by the executor.
pub struct Task {
    /// The future being executed. We box it to allow different future types.
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Task {
    /// Create a new task from a future.
    pub fn new<F>(future: F) -> Arc<Self>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
        })
    }

    /// Poll the task's future.
    /// Returns true if the task is complete (Ready), false if still Pending.
    pub fn poll(self: &Arc<Self>, context: &mut Context) -> Poll<()> {
        let mut future = self.future.lock().unwrap();
        future.as_mut().poll(context)
    }
}
