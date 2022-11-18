use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    panic::{RefUnwindSafe, UnwindSafe},
    sync::Arc,
    time::Duration,
};

use async_task::Runnable;
use crossbeam_queue::SegQueue;
use futures_lite::future;
use mio::Waker;
use once_cell::unsync::OnceCell;

use super::io::Poller;

const NR_TASKS: usize = 256;

thread_local! {
    pub(crate) static ID: OnceCell<usize> = OnceCell::new();
    pub(crate) static CONTEXT: OnceCell<Context> = OnceCell::new();
}

#[derive(Debug)]
pub(crate) struct Context {
    pub(crate) local: RefCell<VecDeque<Runnable>>,
    pub(crate) assigned: Arc<SegQueue<Runnable>>,
    pub(crate) poller: RefCell<Poller>,
    pub(crate) waker: Arc<Waker>,
}

impl Context {
    fn new(assigned: Arc<SegQueue<Runnable>>, poller: Poller, waker: Arc<Waker>) -> Self {
        Context {
            local: RefCell::new(VecDeque::new()),
            assigned,
            poller: RefCell::new(poller),
            waker,
        }
    }
}

pub(crate) struct Worker;

impl UnwindSafe for Worker {}
impl RefUnwindSafe for Worker {}

impl Worker {
    pub(crate) fn new(
        assigned: Arc<SegQueue<Runnable>>,
        poller: Poller,
        waker: Arc<Waker>,
    ) -> Self {
        CONTEXT.with(|context| {
            context
                .set(Context::new(assigned, poller, waker))
                .expect("context can not be setted twice")
        });
        Worker
    }

    pub(crate) async fn run(&self, future: impl Future<Output = ()>) {
        // A future that runs tasks forever.
        let run_forever = async move {
            loop {
                CONTEXT.with(|context| {
                    let context = unsafe { context.get().unwrap_unchecked() };
                    let mut capacity = NR_TASKS;

                    for _ in 0..(capacity) {
                        let runnable = context.local.borrow_mut().pop_front();
                        if let Some(runnable) = runnable {
                            runnable.run();
                            capacity -= 1;
                        } else {
                            break;
                        }
                    }

                    for _ in 0..(capacity) {
                        if let Some(runnable) = context.assigned.pop() {
                            runnable.run();
                            capacity -= 1;
                        } else {
                            break;
                        }
                    }
                });

                future::yield_now().await;

                CONTEXT.with(|context| {
                    let context = context.get().unwrap();
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
                        .unwrap_or_else(|e| tracing::error!("async worker polling failed: {}", e));
                });
            }
        };

        // Run `future` and `run_forever` concurrently until `future` completes.
        future::or(future, run_forever).await;
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
    fn test_runtime() {
        let mut poller = Poller::with_capacity(1).unwrap();
        let waker = Arc::new(poller.waker().unwrap());
        let ex = Worker::new(Arc::new(SegQueue::new()), poller, waker);

        let task = spawn_local(async { 1 + 2 });
        future::block_on(ex.run(async {
            let res = task.await * 2;
            assert_eq!(res, 6);
        }));
    }

    #[test]
    fn test_yield() {
        let mut poller = Poller::with_capacity(1).unwrap();
        let waker = Arc::new(poller.waker().unwrap());
        let ex = Worker::new(Arc::new(SegQueue::new()), poller, waker);

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
