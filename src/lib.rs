mod blocking;
mod io;
pub mod net;
mod worker;

use std::{
    future::Future,
    marker::PhantomData,
    panic::catch_unwind,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    thread,
    thread::JoinHandle,
};

use async_task::{Runnable, Task};
pub use blocking::spawn_blocking_scoped;
use blocking::{BlockingWorker, BLOCKING_TASKS};
use crossbeam_queue::SegQueue;
use futures_lite::future;

use self::{
    io::Poller,
    worker::{Worker, CONTEXT, ID},
};

#[must_use = "scoped-task must be awaited in its lifetime"]
pub struct ScopedTask<'task, T> {
    task: Task<T>,
    _marker: PhantomData<&'task ()>,
}

impl<T> std::panic::UnwindSafe for ScopedTask<'_, T> {}
impl<T> std::panic::RefUnwindSafe for ScopedTask<'_, T> {}

impl<T> Future for ScopedTask<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|st| &mut st.task).poll(cx) }
    }
}

impl<T> Drop for ScopedTask<'_, T> {
    fn drop(&mut self) {
        drop(&mut self.task)
    }
}

#[derive(Default)]
pub struct ExecutorBuilder {
    workers: usize,
    blocking_workers: usize,
}

impl ExecutorBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn workers(self, num: usize) -> Self {
        Self {
            workers: num,
            blocking_workers: self.blocking_workers,
        }
    }

    pub fn blocking_workers(self, num: usize) -> Self {
        Self {
            workers: self.workers,
            blocking_workers: num,
        }
    }

    pub fn build(self) -> std::io::Result<Executor> {
        Ok(Executor::new(self.workers, self.blocking_workers))
    }
}

struct WorkerHandler {
    join: JoinHandle<()>,
    assigned: Arc<SegQueue<Runnable>>,
    waker: Arc<mio::Waker>,
}

pub struct Executor {
    workers: Vec<WorkerHandler>,
    blocking_workers: Vec<JoinHandle<()>>,
}

unsafe impl Send for Executor {}
unsafe impl Sync for Executor {}

impl Drop for Executor {
    fn drop(&mut self) {
        for join in self.blocking_workers.drain(..) {
            join.join().unwrap();
        }
        for ex in self.workers.drain(..) {
            ex.waker.wake().expect("wake async worke failed");
            ex.join.join().unwrap();
        }
    }
}

impl Executor {
    pub fn new(workers: usize, blocking_workers: usize) -> Self {
        let workers = (0..workers)
            .into_iter()
            .map(|id| {
                let assigned = Arc::new(SegQueue::new());
                let local_assigned = Arc::clone(&assigned);
                let mut poller = Poller::with_capacity(256).expect("initialize poller error");
                let waker = Arc::new(poller.waker().expect("try get poller waker failed"));
                let local_waker = waker.clone();

                let join = thread::Builder::new()
                    .name(format!("async-worker-{}", id))
                    .spawn(move || {
                        ID.with(|thread_id| {
                            thread_id.set(id).unwrap();
                        });

                        let executor = Worker::new(local_assigned, poller, local_waker);

                        loop {
                            match catch_unwind(|| {
                                future::block_on(executor.run(async move {
                                    // TODO: use atomic object to graceful exition
                                    futures_lite::future::pending::<()>().await;
                                }));
                            }) {
                                Ok(()) => break,
                                Err(_) => tracing::error!("async worker paniced"),
                            }
                        }
                    })
                    .expect("failed to spawn async worker thread");

                WorkerHandler {
                    join,
                    assigned,
                    waker,
                }
            })
            .collect();

        let (send, recv) = crossbeam_channel::unbounded();
        let blocking_workers = (0..blocking_workers)
            .into_iter()
            .map(|id| {
                let recv = recv.clone();
                thread::Builder::new()
                    .name(format!("blocking-worker-{}", id))
                    .spawn(|| {
                        let worker = BlockingWorker::new(recv);
                        loop {
                            match catch_unwind(|| worker.run()) {
                                Ok(()) => break,
                                Err(_) => tracing::error!("async worker paniced"),
                            }
                        }
                    })
                    .expect("failed to spawn blocking worker thread")
            })
            .collect();

        BLOCKING_TASKS
            .set(send)
            .expect("blocking task sender init twice");

        Self {
            workers,
            blocking_workers,
        }
    }
}

impl Executor {
    pub fn spawn_range<Fact, Fut>(&self, factory: Fact) -> Vec<Task<Fut::Output>>
    where
        Fut: 'static + Future,
        Fut::Output: 'static + Send,
        Fact: 'static + Fn() -> Fut + Clone + Send + Sync,
    {
        self.workers
            .iter()
            .enumerate()
            .map(|(id, worker)| {
                let owned_thread = id;
                let assigned = Arc::clone(&worker.assigned);
                let factory = factory.clone();
                let schedule = schedule(owned_thread, assigned, Arc::clone(&worker.waker));
                let (runnable, task) = unsafe {
                    async_task::spawn_unchecked(async move { factory().await }, schedule)
                };
                runnable.schedule();
                task
            })
            .collect()
    }
}

pub fn spawn<F>(future: F) -> Task<F::Output>
where
    F: 'static + Future,
    F::Output: 'static,
{
    spawn_unchecked(future)
}

pub fn scoped_spawn<'future, F>(future: F) -> ScopedTask<'future, F::Output>
where
    F: 'future + Future,
    F::Output: 'future,
{
    ScopedTask {
        task: spawn_unchecked(future),
        _marker: PhantomData,
    }
}

fn spawn_unchecked<F>(future: F) -> Task<F::Output>
where
    F: Future,
{
    let owned_thread = ID.with(|id| {
        *id.get()
            .expect("spawn_local is called outside the worker thread")
    });
    let (assigned, waker) = CONTEXT.with(|context| {
        let context = unsafe { context.get().unwrap_unchecked() };
        (Arc::clone(&context.assigned), Arc::clone(&context.waker))
    });

    let schedule = schedule(owned_thread, assigned, waker);
    let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) };
    runnable.schedule();
    task
}

fn schedule(
    owned_thread: usize,
    assigned: Arc<SegQueue<Runnable>>,
    waker: Arc<mio::Waker>,
) -> impl Fn(Runnable) {
    move |runnable| {
        ID.with(|id| {
            if let Some(&id) = id.get() {
                if id == owned_thread {
                    CONTEXT.with(|context| {
                        let context = unsafe { context.get().unwrap_unchecked() };
                        context.local.borrow_mut().push_back(runnable);
                    });
                    return;
                }
            }
            assigned.push(runnable);
            waker.wake().expect("wake worker failed");
        })
    }
}
