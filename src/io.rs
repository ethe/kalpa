use std::{io, os::fd::AsRawFd, task::Waker, time::Duration};

use mio::{unix::SourceFd, Events, Interest, Poll, Token};
use slab::Slab;

#[derive(Debug)]
pub(crate) struct Poller {
    poller: Poll,
    events: Events,
    active: Slab<Option<Waker>>,
}

impl Poller {
    pub(crate) fn with_capacity(capacity: usize) -> io::Result<Self> {
        Ok(Self {
            poller: Poll::new()?,
            events: Events::with_capacity(capacity),
            active: Slab::new(),
        })
    }

    pub(crate) fn poll(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.poller.poll(&mut self.events, timeout)?;
        for event in &self.events {
            let waker = self
                .active
                .get_mut(event.token().0)
                .expect("io event is registered but not found in poller");

            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }
        self.events.clear();
        Ok(())
    }

    pub(crate) fn waker(&mut self) -> io::Result<mio::Waker> {
        mio::Waker::new(self.poller.registry(), Token(self.active.insert(None)))
    }

    pub(crate) fn register<Fd: AsRawFd>(
        &mut self,
        fd: &Fd,
        interests: Interest,
    ) -> io::Result<usize> {
        let entry = self.active.vacant_entry();
        let key = entry.key();
        self.poller
            .registry()
            .register(&mut SourceFd(&fd.as_raw_fd()), Token(key), interests)?;
        entry.insert(None);
        Ok(key)
    }

    pub(crate) fn add(&mut self, id: usize, waker: Waker) {
        *self.active.get_mut(id).unwrap() = Some(waker);
    }

    pub(crate) fn deregister<Fd: AsRawFd>(&mut self, id: usize, fd: &Fd) {
        self.active.remove(id);
        self.poller
            .registry()
            .deregister(&mut SourceFd(&fd.as_raw_fd()))
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "deregister polling event failed, fd: {}, e: {}",
                    fd.as_raw_fd(),
                    e
                )
            });
    }
}
