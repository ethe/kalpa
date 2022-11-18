use std::{
    marker::PhantomData,
    panic::{RefUnwindSafe, UnwindSafe},
};

use async_task::{spawn_unchecked, Runnable};
use crossbeam_channel::{Receiver, Sender};
use once_cell::sync::OnceCell;

use crate::ScopedTask;

pub(crate) static BLOCKING_TASKS: OnceCell<Sender<Runnable>> = OnceCell::new();

pub(crate) struct BlockingWorker {
    recv: Receiver<Runnable>,
}

impl UnwindSafe for BlockingWorker {}
impl RefUnwindSafe for BlockingWorker {}

impl BlockingWorker {
    pub(crate) fn new(recv: Receiver<Runnable>) -> Self {
        BlockingWorker { recv }
    }

    pub(crate) fn run(&self) {
        loop {
            match self.recv.recv() {
                Ok(runnable) => {
                    runnable.run();
                }
                Err(_) => break,
            }
        }
    }
}

pub fn spawn_blocking_scoped<'task, R, F>(f: F) -> ScopedTask<'task, R>
where
    R: 'task + Send,
    F: 'task + FnOnce() -> R + Send,
{
    let (runnable, task) = unsafe {
        spawn_unchecked(async move { f() }, |runnable| {
            let send = BLOCKING_TASKS
                .get()
                .expect("blocking task queue does not initialized");
            send.send(runnable)
                .expect("spawn blocking task failed, there is no blocking task worker alived")
        })
    };
    runnable.schedule();
    ScopedTask {
        task,
        _marker: PhantomData,
    }
}
