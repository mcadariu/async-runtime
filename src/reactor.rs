use mio::{Events, Interest, Poll, Token, Waker as MioWaker};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::Duration;

pub struct Reactor {
    poll: Mutex<Poll>,
    wakers: Mutex<HashMap<usize, Waker>>,
    waker: Arc<MioWaker>,
}

impl Reactor {
    pub fn new() -> io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(MioWaker::new(poll.registry(), Token(usize::MAX))?);

        Ok(Reactor {
            poll: Mutex::new(poll),
            wakers: Mutex::new(HashMap::new()),
            waker,
        })
    }

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

        let token_value = (0..)
            .find(|&i| !wakers.contains_key(&i))
            .expect("Token overflow");

        let token = Token(token_value);

        let poll = self.poll.lock().unwrap();
        poll.registry().register(source, token, interest)?;
        drop(poll);

        wakers.insert(token_value, waker);

        Ok(token_value)
    }

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

    pub fn run(&self) {
        let mut events = Events::with_capacity(1024);

        loop {
            let mut poll = self.poll.lock().unwrap();
            if let Err(e) = poll.poll(&mut events, Some(Duration::from_millis(100))) {
                eprintln!("Reactor poll error: {}", e);
                continue;
            }
            drop(poll);

            for event in events.iter() {
                let token = event.token();

                if token == Token(usize::MAX) {
                    continue;
                }

                let wakers = self.wakers.lock().unwrap();
                if let Some(waker) = wakers.get(&token.0) {
                    waker.wake_by_ref();
                }
            }
        }
    }

    pub fn waker(&self) -> Arc<MioWaker> {
        Arc::clone(&self.waker)
    }
}
