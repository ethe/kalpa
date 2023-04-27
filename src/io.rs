use std::{io, task::Waker, time::Duration};

pub use mio::Interest;
use mio::{event, Events, Poll, Token};
use slab::Slab;

#[derive(Debug)]
struct WakerInterests {
    interests: Interest,
    waker: Waker,
}

#[derive(Debug)]
pub struct Poller {
    poller: Poll,
    events: Events,
    active: Slab<Slab<WakerInterests>>,
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

            let mut id = None;

            let is = if event.is_readable() {
                Interest::is_readable
            } else if event.is_writable() {
                Interest::is_writable
            } else {
                continue;
            };

            for (i, waker) in waker.iter() {
                if is(waker.interests) {
                    id = Some(i);
                    break;
                }
            }

            if let Some(id) = id {
                let waker = waker.remove(id);
                waker.waker.wake();
            }
        }
        self.events.clear();
        Ok(())
    }

    pub(crate) fn waker(&mut self) -> io::Result<mio::Waker> {
        mio::Waker::new(
            self.poller.registry(),
            Token(self.active.insert(Slab::new())),
        )
    }

    pub fn register<S>(&mut self, source: &mut S, interests: Interest) -> io::Result<usize>
    where
        S: event::Source + ?Sized,
    {
        let entry = self.active.vacant_entry();
        let key = entry.key();
        self.poller
            .registry()
            .register(source, Token(key), interests)?;
        entry.insert(Slab::with_capacity(2));
        Ok(key)
    }

    pub fn add(&mut self, id: usize, waker: Waker, interests: Interest) {
        self.active
            .get_mut(id)
            .unwrap()
            .insert(WakerInterests { interests, waker });
    }

    pub fn deregister<S>(&mut self, id: usize, source: &mut S)
    where
        S: event::Source + ?Sized,
    {
        self.active.remove(id);
        self.poller
            .registry()
            .deregister(source)
            .unwrap_or_else(|e| tracing::warn!("deregister polling event failed, e: {}", e));
    }
}
