use std::{
    io,
    io::{IoSlice, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    os::fd::AsRawFd,
    pin::Pin,
    task::{Context, Poll},
};

use futures_lite::Stream;
use mio::Interest;
use socket2::{Domain, Protocol, Socket, Type};

use super::worker::CONTEXT;

#[derive(Debug)]
pub struct Async<T: AsRawFd> {
    io: T,
    id: usize,
}

impl<T: AsRawFd> Async<T> {
    fn new(io: T) -> io::Result<Self> {
        let id = CONTEXT.with(|context| {
            context
                .get()
                .unwrap()
                .poller
                .borrow_mut()
                .register(&io, Interest::READABLE | Interest::WRITABLE)
        })?;
        Ok(Self { io, id })
    }
}

impl<T: AsRawFd> AsRef<T> for Async<T> {
    fn as_ref(&self) -> &T {
        &self.io
    }
}

impl<T: AsRawFd> Drop for Async<T> {
    fn drop(&mut self) {
        CONTEXT.with(|context| {
            context
                .get()
                .unwrap()
                .poller
                .borrow_mut()
                .deregister(self.id, &self.io)
        });
    }
}

impl Async<TcpListener> {
    pub fn connect(address: SocketAddr) -> io::Result<Self> {
        let socket_type = Type::STREAM.nonblocking();
        let socket = Socket::new(
            Domain::for_address(address),
            socket_type,
            Some(Protocol::TCP),
        )?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&address.into())?;
        socket.listen(32768)?;
        Async::new(socket.into())
    }
}

impl Stream for Async<TcpListener> {
    type Item = io::Result<Async<TcpStream>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_ref().io.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(true)?;
                Poll::Ready(Some(Async::new(stream)))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                CONTEXT.with(|context| {
                    context
                        .get()
                        .unwrap()
                        .poller
                        .borrow_mut()
                        .add(self.id, cx.waker().clone())
                });
                Poll::Pending
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl Async<TcpStream> {
    pub fn connect(address: SocketAddr) -> io::Result<Self> {
        let socket_type = Type::STREAM.nonblocking();
        let socket = Socket::new(
            Domain::for_address(address),
            socket_type,
            Some(Protocol::TCP),
        )?;
        match socket.connect(&address.into()) {
            Ok(_) => {}
            #[cfg(unix)]
            Err(err) if err.raw_os_error() == Some(libc::EINPROGRESS) => {}
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => return Err(err),
        }
        Async::new(socket.into())
    }
}

impl tokio::io::AsyncRead for Async<TcpStream> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        unsafe {
            let bytes =
                &mut *(buf.unfilled_mut() as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]);
            match self.io.read(bytes) {
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    CONTEXT.with(|context| {
                        context
                            .get()
                            .unwrap()
                            .poller
                            .borrow_mut()
                            .add(self.id, cx.waker().clone())
                    });
                    Poll::Pending
                }
                Ok(n) => {
                    buf.assume_init(n);
                    buf.advance(n);
                    Poll::Ready(Ok(()))
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        }
    }
}

impl tokio::io::AsyncWrite for Async<TcpStream> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.io.write(buf) {
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                CONTEXT.with(|context| {
                    context
                        .get()
                        .unwrap()
                        .poller
                        .borrow_mut()
                        .add(self.id, cx.waker().clone())
                });
                Poll::Pending
            }
            x => Poll::Ready(x),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.io.flush())
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.io.shutdown(Shutdown::Both))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.io.write_vectored(bufs) {
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                CONTEXT.with(|context| {
                    context
                        .get()
                        .unwrap()
                        .poller
                        .borrow_mut()
                        .add(self.id, cx.waker().clone())
                });
                Poll::Pending
            }
            x => Poll::Ready(x),
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}
