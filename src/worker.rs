use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    panic::{RefUnwindSafe, UnwindSafe},
    sync::Arc,
    task::Waker,
};

use async_task::Runnable;
use crossbeam_queue::SegQueue;
use futures_lite::future;
use once_cell::unsync::OnceCell;
use slab::Slab;

#[cfg(feature = "io")]
use super::io::Poller;

pub(crate) const NR_TASKS: usize = 256;

thread_local! {
    pub static CONTEXT: OnceCell<Context> = OnceCell::new();
}

#[derive(Debug)]
pub struct Context {
    pub(crate) id: usize,
    pub(crate) local: RefCell<VecDeque<Runnable>>,
    pub(crate) assigned: Arc<SegQueue<Runnable>>,
    #[cfg(feature = "io")]
    pub poller: RefCell<Poller>,
    #[cfg(feature = "io")]
    pub waker: Arc<mio::Waker>,
    pub(crate) active: RefCell<Slab<Waker>>,
}

impl Context {
    fn new(
        id: usize,
        assigned: Arc<SegQueue<Runnable>>,
        #[cfg(feature = "io")] poller: Poller,
        #[cfg(feature = "io")] waker: Arc<mio::Waker>,
    ) -> Self {
        Context {
            id,
            local: RefCell::new(VecDeque::new()),
            assigned,
            #[cfg(feature = "io")]
            poller: RefCell::new(poller),
            #[cfg(feature = "io")]
            waker,
            active: RefCell::new(Slab::new()),
        }
    }
}

pub(crate) struct Worker;

impl UnwindSafe for Worker {}
impl RefUnwindSafe for Worker {}

impl Worker {
    pub(crate) fn new(
        id: usize,
        assigned: Arc<SegQueue<Runnable>>,
        #[cfg(feature = "io")] poller: Poller,
        #[cfg(feature = "io")] waker: Arc<mio::Waker>,
    ) -> Self {
        CONTEXT.with(|context| {
            context
                .set(Context::new(
                    id,
                    assigned,
                    #[cfg(feature = "io")]
                    poller,
                    #[cfg(feature = "io")]
                    waker,
                ))
                .expect("context can not be setted twice")
        });
        Worker
    }

    pub(crate) async fn run(&self, future: impl Future<Output = ()>) {
        // A future that runs tasks forever.
        let run_forever = async move {
            loop {
                CONTEXT.with(|context| {
                    let context = context.get().expect("context should be initialized");
                    let mut capacity = NR_TASKS;

                    while capacity > 0 {
                        let runnable = context.local.borrow_mut().pop_front();
                        if let Some(runnable) = runnable {
                            runnable.run();
                            capacity -= 1;
                        } else {
                            break;
                        }
                    }

                    while capacity > 0 {
                        if let Some(runnable) = context.assigned.pop() {
                            runnable.run();
                            capacity -= 1;
                        } else {
                            break;
                        }
                    }
                });

                future::yield_now().await;

                #[cfg(feature = "io")]
                {
                    use std::time::Duration;

                    CONTEXT.with(|context| {
                        let context = context.get().expect("context should be initialized");
                        let timeout =
                            if context.local.borrow().is_empty() && context.assigned.is_empty() {
                                None
                            } else {
                                Some(Duration::ZERO)
                            };
                        context
                            .poller
                            .borrow_mut()
                            .poll(timeout)
                            .unwrap_or_else(|e| {
                                tracing::error!("async worker polling failed: {}", e)
                            });
                    })
                }
            }
        };

        // Run `future` and `run_forever` concurrently until `future` completes.
        future::or(future, run_forever).await;
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        CONTEXT.with(|context| {
            let context = context.get().expect("context should be initialized");
            for waker in context.active.borrow_mut().drain() {
                waker.wake();
            }
            while let Some(runnable) = context.assigned.pop() {
                drop(runnable);
            }
            while let Some(runnable) = context.local.borrow_mut().pop_front() {
                drop(runnable);
            }
        })
    }
}

#[cfg(test)]
mod test {
    use core::future::Future;
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    use async_task::Task;
    use crossbeam_queue::SegQueue;
    use futures_lite::{future, future::yield_now};

    use super::{Worker, CONTEXT};
    use crate::io::Poller;

    fn spawn_local<T>(future: impl Future<Output = T>) -> Task<T> {
        let schedule = move |runnable| {
            CONTEXT.with(|context| {
                context
                    .get()
                    .unwrap()
                    .local
                    .borrow_mut()
                    .push_back(runnable);
            });
        };
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) };
        runnable.schedule();
        task
    }

    #[test]
    fn worker_could_run() {
        let mut poller = Poller::with_capacity(1).unwrap();
        let waker = Arc::new(poller.waker().unwrap());
        let ex = Worker::new(0, Arc::new(SegQueue::new()), poller, waker);

        let task = spawn_local(async { 1 + 2 });
        future::block_on(ex.run(async {
            let res = task.await * 2;
            assert_eq!(res, 6);
        }));
    }

    #[test]
    fn task_coud_be_yielded() {
        let mut poller = Poller::with_capacity(1).unwrap();
        let waker = Arc::new(poller.waker().unwrap());
        let ex = Worker::new(0, Arc::new(SegQueue::new()), poller, waker);

        let counter = Rc::new(RefCell::new(0));
        let counter1 = Rc::clone(&counter);
        let task = spawn_local(async {
            {
                let mut c = counter1.borrow_mut();
                assert_eq!(*c, 0);
                *c = 1;
            }
            let counter_clone = Rc::clone(&counter1);
            let t = spawn_local(async {
                {
                    let mut c = counter_clone.borrow_mut();
                    assert_eq!(*c, 1);
                    *c = 2;
                }
                yield_now().await;
                {
                    let mut c = counter_clone.borrow_mut();
                    assert_eq!(*c, 3);
                    *c = 4;
                }
            });
            yield_now().await;
            {
                let mut c = counter1.borrow_mut();
                assert_eq!(*c, 2);
                *c = 3;
            }
            t.await;
        });
        future::block_on(ex.run(task));
        assert_eq!(*counter.as_ref().borrow(), 4);
    }
}
