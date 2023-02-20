#[cfg(feature = "blocking")]
mod blocking;
#[cfg(feature = "io")]
pub mod io;
#[cfg(feature = "net")]
pub mod net;
mod worker;

use std::{
    future::Future,
    marker::PhantomData,
    mem::MaybeUninit,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    thread,
    thread::JoinHandle,
};

use async_task::{Runnable, Task};
use crossbeam_queue::SegQueue;
use event_listener::Event;
use futures_lite::{future, pin};
use futures_util::future::join_all;
#[cfg(feature = "blocking")]
use once_cell::sync::OnceCell;
use waker_fn::waker_fn;
pub use worker::CONTEXT;

#[cfg(feature = "io")]
use self::io::Poller;
use self::worker::Worker;

#[cfg(feature = "blocking")]
static BLOCKING_EXECUTOR: OnceCell<blocking::Executor> = OnceCell::new();

#[must_use = "because it must be awaited in its lifetime"]
#[repr(transparent)]
pub struct ScopedTask<'task, T> {
    task: MaybeUninit<Task<T>>,
    _marker: PhantomData<&'task &'task mut ()>,
}

impl<T> std::panic::UnwindSafe for ScopedTask<'_, T> {}
impl<T> std::panic::RefUnwindSafe for ScopedTask<'_, T> {}

impl<T> Future for ScopedTask<'_, T> {
    type Output = T;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|st| st.task.assume_init_mut()) }.poll(cx)
    }
}

impl<T> Drop for ScopedTask<'_, T> {
    fn drop(&mut self) {
        let task =
            unsafe { std::mem::replace(&mut self.task, MaybeUninit::uninit()).assume_init() }
                .cancel();
        pin!(task);
        let waker = waker_fn(move || {});
        let mut cx = Context::from_waker(&waker);
        if task.as_mut().poll(&mut cx).is_pending() {
            panic!("cancel scoped task should always success immediately");
        }
    }
}

impl<T> ScopedTask<'_, T> {
    #[inline]
    fn new(task: Task<T>) -> Self {
        ScopedTask {
            task: MaybeUninit::new(task),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        unsafe { self.task.assume_init_ref() }.is_finished()
    }
}

impl<T> ScopedTask<'static, T> {
    #[inline]
    pub fn detach(mut self) {
        let task = std::mem::replace(&mut self.task, MaybeUninit::uninit());
        std::mem::forget(self);
        unsafe { task.assume_init() }.detach()
    }
}

pub struct ExecutorBuilder<'executor> {
    worker_num: usize,
    #[cfg(feature = "blocking")]
    max_blocking_thread_num: usize,
    _marker: PhantomData<&'executor ()>,
}

impl Default for ExecutorBuilder<'_> {
    fn default() -> Self {
        Self {
            worker_num: std::thread::available_parallelism().unwrap().get(),
            #[cfg(feature = "blocking")]
            max_blocking_thread_num: usize::MAX,
            _marker: PhantomData,
        }
    }
}

impl<'executor> ExecutorBuilder<'executor> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn worker_num(self, num: usize) -> Self {
        Self {
            worker_num: num,
            #[cfg(feature = "blocking")]
            max_blocking_thread_num: self.max_blocking_thread_num,
            _marker: PhantomData,
        }
    }

    #[cfg(feature = "blocking")]
    pub fn max_blocking_thread_num(self, num: usize) -> Self {
        Self {
            worker_num: self.worker_num,
            max_blocking_thread_num: num,
            _marker: PhantomData,
        }
    }

    pub fn build(self) -> std::io::Result<Executor<'executor>> {
        Executor::new(
            self.worker_num,
            #[cfg(feature = "blocking")]
            self.max_blocking_thread_num,
        )
    }
}

struct WorkerHandler {
    join: JoinHandle<()>,
    assigned: Arc<SegQueue<Runnable>>,
    #[cfg(feature = "io")]
    waker: Arc<mio::Waker>,
}

pub struct Executor<'executor> {
    workers: Vec<WorkerHandler>,
    closer: Arc<Event>,
    _marker: PhantomData<&'executor ()>,
}

unsafe impl Send for Executor<'_> {}
unsafe impl Sync for Executor<'_> {}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        self.closer.notify(self.workers.len());
        for worker in self.workers.drain(..) {
            #[cfg(feature = "io")]
            worker
                .waker
                .wake()
                .expect("wake worker to accpet spawned task must be successed");
            worker
                .join
                .join()
                .expect("blocking worker should successfully exit");
        }
    }
}

impl<'executor> Executor<'executor> {
    pub fn builder() -> ExecutorBuilder<'executor> {
        ExecutorBuilder::new()
    }

    fn new(
        worker_num: usize,
        #[cfg(feature = "blocking")] max_blocking_thread_num: usize,
    ) -> std::io::Result<Self> {
        let closer = Arc::new(Event::new());
        let mut workers = Vec::with_capacity(worker_num);

        for worker_id in 0..worker_num {
            let assigned = Arc::new(SegQueue::new());
            let closer = Arc::clone(&closer);

            workers.push(Self::create_worker_handler(worker_id, assigned, closer)?);
        }

        #[cfg(feature = "blocking")]
        BLOCKING_EXECUTOR.get_or_init(|| blocking::Executor::new(max_blocking_thread_num));

        Ok(Self {
            workers,
            closer,
            _marker: PhantomData,
        })
    }

    fn create_worker_handler(
        worker_id: usize,
        assigned: Arc<SegQueue<Runnable>>,
        closer: Arc<Event>,
    ) -> std::io::Result<WorkerHandler> {
        #[cfg(feature = "io")]
        use worker::NR_TASKS;

        let local_assigned = Arc::clone(&assigned);

        #[cfg(feature = "io")]
        let mut poller = Poller::with_capacity(NR_TASKS)?;
        #[cfg(feature = "io")]
        let waker = Arc::new(poller.waker()?);
        #[cfg(feature = "io")]
        let local_waker = waker.clone();

        let join = thread::Builder::new()
            .name(format!("async-worker-{}", worker_id))
            .spawn(move || {
                let worker = Worker::new(
                    worker_id,
                    local_assigned,
                    #[cfg(feature = "io")]
                    poller,
                    #[cfg(feature = "io")]
                    local_waker,
                );

                loop {
                    let closer = Arc::clone(&closer);
                    future::block_on(worker.run(async move {
                        closer.listen().await;
                    }));
                }
            })?;

        Ok(WorkerHandler {
            join,
            assigned,
            #[cfg(feature = "io")]
            waker,
        })
    }
}

impl<'executor> Executor<'executor> {
    pub fn run<MakeF, F>(&'executor self, maker: MakeF) -> Vec<F::Output>
    where
        F: 'executor + Future,
        F::Output: 'executor + Send,
        MakeF: 'executor + Fn() -> F + Clone + Send,
    {
        future::block_on(join_all(self.workers.iter().enumerate().map(
            |(id, worker)| {
                let owned_thread = id;
                let assigned = Arc::clone(&worker.assigned);
                let maker = maker.clone();

                #[cfg(feature = "io")]
                let schedule = schedule(owned_thread, assigned, Arc::clone(&worker.waker));

                #[cfg(not(feature = "io"))]
                let schedule = schedule(owned_thread, assigned);

                let (runnable, task) =
                    unsafe { async_task::spawn_unchecked(async move { maker().await }, schedule) };
                runnable.schedule();

                #[cfg(feature = "io")]
                worker
                    .waker
                    .wake()
                    .expect("wake worker to accpet spawned task must be successed");

                task
            },
        )))
    }
}

pub fn spawn<'future, F>(future: F) -> ScopedTask<'future, F::Output>
where
    F: 'future + Future,
    F::Output: 'future,
{
    ScopedTask::new(unsafe { spawn_unchecked(future) })
}

pub(crate) unsafe fn spawn_unchecked<F>(future: F) -> Task<F::Output>
where
    F: Future,
{
    #[cfg(not(feature = "io"))]
    let (owned_thread, assigned, id) = CONTEXT.with(|context| {
        let context = context.get().expect("context should be initialized");
        (
            context.id,
            Arc::clone(&context.assigned),
            context.active.borrow_mut().vacant_entry().key(),
        )
    });
    #[cfg(not(feature = "io"))]
    let schedule = schedule(owned_thread, assigned);

    #[cfg(feature = "io")]
    let (owned_thread, assigned, id, waker) = CONTEXT.with(|context| {
        let context = context.get().expect("context should be initialized");
        (
            context.id,
            Arc::clone(&context.assigned),
            context.active.borrow_mut().vacant_entry().key(),
            Arc::clone(&context.waker),
        )
    });
    #[cfg(feature = "io")]
    let schedule = schedule(owned_thread, assigned, waker);

    let (runnable, task) = async_task::spawn_unchecked(
        async move {
            let _guard = CallOnDrop(|| {
                CONTEXT.with(|context| {
                    let context = context.get().expect("context should be initialized");
                    context.active.borrow_mut().try_remove(id);
                })
            });
            future.await
        },
        schedule,
    );

    CONTEXT.with(|context| {
        let context = context.get().expect("context should be initialized");
        context.active.borrow_mut().insert(runnable.waker());
    });

    runnable.schedule();

    task
}

fn schedule(
    owned_thread: usize,
    assigned: Arc<SegQueue<Runnable>>,
    #[cfg(feature = "io")] waker: Arc<mio::Waker>,
) -> impl Fn(Runnable) {
    move |runnable| {
        CONTEXT.with(|context| {
            if let Some(id) = context.get().map(|cx| cx.id) {
                if id == owned_thread {
                    CONTEXT.with(|context| {
                        let context = context.get().expect("context should be initialized");
                        context.local.borrow_mut().push_back(runnable);
                    });
                    return;
                }
            }
            assigned.push(runnable);

            #[cfg(feature = "io")]
            waker
                .wake()
                .expect("wake worker by task scheduling must be ok");
        })
    }
}

#[cfg(feature = "blocking")]
pub fn unblock<T, F>(f: F) -> Task<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    blocking::Executor::spawn(async move { f() })
}

struct CallOnDrop<F: Fn()>(F);

impl<F: Fn()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

#[cfg(test)]
mod test {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn static_task() {
        use crate::{spawn, Executor};

        let hello_world = "hello world";
        Executor::builder()
            .worker_num(1)
            .build()
            .unwrap()
            .run(|| async {
                spawn(async move {
                    let _ = hello_world;
                })
                .detach();
            });
    }

    #[test]
    fn unblock_task() {
        use std::time::Duration;

        use crate::{unblock, Executor};

        Executor::builder()
            .worker_num(1)
            .max_blocking_thread_num(1)
            .build()
            .unwrap()
            .run(|| async {
                let step = Arc::new(AtomicUsize::new(1));
                let inner_step = Arc::clone(&step);
                let task = unblock(move || {
                    std::thread::sleep(Duration::from_secs(1));
                    debug_assert!(inner_step
                        .compare_exchange(2, 3, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok());
                });
                debug_assert!(step
                    .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok());
                task.await;
            });
    }
}
