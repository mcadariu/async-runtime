use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

pub struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Task {
    pub fn new<F>(future: F) -> Arc<Self>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
        })
    }

    pub fn poll(self: &Arc<Self>, context: &mut Context) -> Poll<()> {
        let mut future = self.future.lock().unwrap();
        future.as_mut().poll(context)
    }
}
