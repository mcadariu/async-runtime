use mio::{Events, Interest, Poll, Token, Waker as MioWaker};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::Duration;

/// The reactor handles I/O events using mio (epoll/kqueue).
pub struct Reactor {
    /// mio's Poll instance for the OS event loop.
    poll: Mutex<Poll>,
    /// Map from mio Token to task Waker.
    /// When an I/O event occurs, we wake the corresponding task.
    wakers: Mutex<HashMap<usize, Waker>>,
    /// Waker to interrupt the reactor event loop.
    waker: Arc<MioWaker>,
}

impl Reactor {
    /// Create a new reactor.
    pub fn new() -> io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(MioWaker::new(poll.registry(), Token(usize::MAX))?);

        Ok(Reactor {
            poll: Mutex::new(poll),
            wakers: Mutex::new(HashMap::new()),
            waker,
        })
    }

    /// Register interest in I/O events for a source.
    /// Returns a token that identifies this registration.
    pub fn register<S>(
        &self,
        source: &mut S,
        interest: Interest,
        waker: Waker,
    ) -> io::Result<usize>
    where
        S: mio::event::Source,
    {
        let mut wakers = self.wakers.lock().unwrap();

        // Find next available token
        let token_value = (0..)
            .find(|&i| !wakers.contains_key(&i))
            .expect("Token overflow");

        let token = Token(token_value);

        // Register with mio
        let poll = self.poll.lock().unwrap();
        poll.registry().register(source, token, interest)?;
        drop(poll);

        // Store the waker
        wakers.insert(token_value, waker);

        Ok(token_value)
    }

    /// Reregister interest for an existing source.
    pub fn reregister<S>(
        &self,
        source: &mut S,
        token: usize,
        interest: Interest,
    ) -> io::Result<()>
    where
        S: mio::event::Source,
    {
        let poll = self.poll.lock().unwrap();
        poll.registry().reregister(source, Token(token), interest)
    }

    /// Deregister a source.
    pub fn deregister<S>(&self, source: &mut S, token: usize) -> io::Result<()>
    where
        S: mio::event::Source,
    {
        let poll = self.poll.lock().unwrap();
        poll.registry().deregister(source)?;
        drop(poll);

        let mut wakers = self.wakers.lock().unwrap();
        wakers.remove(&token);

        Ok(())
    }

    /// Run the reactor event loop.
    /// This should be called on a dedicated thread.
    pub fn run(&self) {
        let mut events = Events::with_capacity(1024);

        loop {
            // Wait for events with a timeout
            let mut poll = self.poll.lock().unwrap();
            if let Err(e) = poll.poll(&mut events, Some(Duration::from_millis(100))) {
                eprintln!("Reactor poll error: {}", e);
                continue;
            }
            drop(poll);

            // Process events
            for event in events.iter() {
                let token = event.token();

                // Skip the waker token
                if token == Token(usize::MAX) {
                    continue;
                }

                // Wake the task associated with this token
                let wakers = self.wakers.lock().unwrap();
                if let Some(waker) = wakers.get(&token.0) {
                    waker.wake_by_ref();
                }
            }
        }
    }

    /// Get a clone of the reactor's waker.
    pub fn waker(&self) -> Arc<MioWaker> {
        Arc::clone(&self.waker)
    }
}
