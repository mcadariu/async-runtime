use mio::Interest;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::executor;

fn register_with_reactor<S>(
    source: &mut S,
    token_slot: &Mutex<Option<usize>>,
    interest: Interest,
    waker: Waker,
) -> io::Result<()>
where
    S: mio::event::Source,
{
    let reactor = executor::reactor();
    let mut token = token_slot.lock().unwrap();

    if let Some(existing_token) = *token {
        reactor.reregister(source, existing_token, interest)?;
    } else {
        let new_token = reactor.register(source, interest, waker)?;
        *token = Some(new_token);
    }

    Ok(())
}

pub struct TcpListener {
    inner: Arc<Mutex<mio::net::TcpListener>>,
    token: Mutex<Option<usize>>,
}

impl TcpListener {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = mio::net::TcpListener::bind(addr)?;
        Ok(TcpListener {
            inner: Arc::new(Mutex::new(listener)),
            token: Mutex::new(None),
        })
    }

    pub fn accept(&self) -> AcceptFuture<'_> {
        AcceptFuture {
            listener: Arc::clone(&self.inner),
            token: &self.token,
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().unwrap().local_addr()
    }
}

pub struct AcceptFuture<'a> {
    listener: Arc<Mutex<mio::net::TcpListener>>,
    token: &'a Mutex<Option<usize>>,
}

impl<'a> Future for AcceptFuture<'a> {
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut listener = self.listener.lock().unwrap();

        match listener.accept() {
            Ok((stream, addr)) => {
                Poll::Ready(Ok((
                    TcpStream {
                        inner: Arc::new(Mutex::new(stream)),
                        token: Mutex::new(None),
                    },
                    addr,
                )))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                register_with_reactor(
                    &mut *listener,
                    self.token,
                    Interest::READABLE,
                    cx.waker().clone(),
                )
                .ok();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub struct TcpStream {
    inner: Arc<Mutex<mio::net::TcpStream>>,
    token: Mutex<Option<usize>>,
}

impl TcpStream {
    pub fn connect(addr: SocketAddr) -> ConnectFuture {
        ConnectFuture {
            addr,
            stream: None,
            token: None,
        }
    }

    pub fn read<'a>(&'a self, buf: &'a mut [u8]) -> ReadFuture<'a> {
        ReadFuture {
            stream: Arc::clone(&self.inner),
            token: &self.token,
            buf,
        }
    }

    pub fn write<'a>(&'a self, buf: &'a [u8]) -> WriteFuture<'a> {
        WriteFuture {
            stream: Arc::clone(&self.inner),
            token: &self.token,
            buf,
        }
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().unwrap().peer_addr()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock().unwrap().local_addr()
    }
}

pub struct ConnectFuture {
    addr: SocketAddr,
    stream: Option<Arc<Mutex<mio::net::TcpStream>>>,
    token: Option<usize>,
}

impl Future for ConnectFuture {
    type Output = io::Result<TcpStream>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.stream.is_none() {
            let stream = mio::net::TcpStream::connect(self.addr)?;
            self.stream = Some(Arc::new(Mutex::new(stream)));
        }

        let stream = Arc::clone(self.stream.as_ref().unwrap());
        let mut stream_guard = stream.lock().unwrap();

        match stream_guard.peer_addr() {
            Ok(_) => {
                drop(stream_guard);
                Poll::Ready(Ok(TcpStream {
                    inner: stream,
                    token: Mutex::new(self.token),
                }))
            }
            Err(ref e) if e.kind() == io::ErrorKind::NotConnected => {
                let token_slot = Mutex::new(self.token);
                register_with_reactor(
                    &mut *stream_guard,
                    &token_slot,
                    Interest::WRITABLE,
                    cx.waker().clone(),
                )
                .ok();
                self.token = *token_slot.lock().unwrap();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub struct ReadFuture<'a> {
    stream: Arc<Mutex<mio::net::TcpStream>>,
    token: &'a Mutex<Option<usize>>,
    buf: &'a mut [u8],
}

impl<'a> Future for ReadFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let mut stream = this.stream.lock().unwrap();

        match stream.read(this.buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                register_with_reactor(
                    &mut *stream,
                    this.token,
                    Interest::READABLE,
                    cx.waker().clone(),
                )
                .ok();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub struct WriteFuture<'a> {
    stream: Arc<Mutex<mio::net::TcpStream>>,
    token: &'a Mutex<Option<usize>>,
    buf: &'a [u8],
}

impl<'a> Future for WriteFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut stream = self.stream.lock().unwrap();

        match stream.write(self.buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                register_with_reactor(
                    &mut *stream,
                    self.token,
                    Interest::WRITABLE,
                    cx.waker().clone(),
                )
                .ok();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
