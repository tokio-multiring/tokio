#![warn(rust_2018_idioms)]
#![cfg(all(
    tokio_unstable,
    feature = "full",
    feature = "io-reactor",
    target_os = "linux",
    not(miri)
))]

//! Integration tests for driving a Tokio runtime with an external [`Reactor`].
//!
//! `EpollReactor` below is written against `tokio::runtime::reactor` alone,
//! using an edge-triggered epoll instance and Tokio's shared-driver parking
//! discipline (first worker to park drives epoll and keeps timers, others wait
//! on a condition variable).

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest, Ready};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::reactor::{ParkMode, Reactor, Readiness, RegistrationToken, Timers};
use tokio::runtime::{Builder, Runtime};

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant};

/// Key of the descriptor that wakes the thread in `epoll_wait`.
const WAKE: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Park {
    Awake,
    Driving,
    Waiting,
    Notified,
}

#[derive(Debug)]
struct Worker {
    park: Mutex<Park>,
    unparked: Condvar,
}

#[derive(Debug)]
struct Registered {
    file: OwnedFd,
    readiness: Readiness,
}

#[derive(Debug, Default)]
struct Counts {
    registered: AtomicUsize,
    deregistered: AtomicUsize,
    drove: AtomicUsize,
    polled: AtomicUsize,
    waited: AtomicUsize,
    timer_unparks: AtomicUsize,
    before_steal: AtomicUsize,
    started: AtomicUsize,
    stopped: AtomicUsize,
    shutdowns: AtomicUsize,
}

#[derive(Debug)]
struct EpollReactor {
    epoll: OwnedFd,
    wake: OwnedFd,
    /// Held by the worker currently in `epoll_wait`.
    driver: Mutex<()>,
    /// `worker + 1` of the worker currently driving `epoll_wait` as timekeeper,
    /// or `0` if none. Published with `SeqCst` *before* calling
    /// `Timers::next_timeout()` so `unpark_for_timer()` cannot race it.
    timekeeper: AtomicUsize,
    workers: Vec<Worker>,
    registered: Mutex<HashMap<u64, Registered>>,
    next_key: AtomicU64,
    allow_block_in_place: AtomicBool,
    before_steal_waker: Mutex<Option<Waker>>,
    counts: Counts,
}

fn check(result: libc::c_int) -> io::Result<libc::c_int> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

impl EpollReactor {
    fn new(workers: usize) -> io::Result<EpollReactor> {
        // SAFETY: both calls return a new descriptor or -1.
        let (epoll, wake) = unsafe {
            let epoll = OwnedFd::from_raw_fd(check(libc::epoll_create1(libc::EPOLL_CLOEXEC))?);
            let flags = libc::EFD_CLOEXEC | libc::EFD_NONBLOCK;
            let wake = OwnedFd::from_raw_fd(check(libc::eventfd(0, flags))?);
            (epoll, wake)
        };
        let reactor = EpollReactor {
            epoll,
            wake,
            driver: Mutex::new(()),
            timekeeper: AtomicUsize::new(0),
            workers: (0..workers)
                .map(|_| Worker {
                    park: Mutex::new(Park::Awake),
                    unparked: Condvar::new(),
                })
                .collect(),
            registered: Mutex::new(HashMap::new()),
            next_key: AtomicU64::new(0),
            allow_block_in_place: AtomicBool::new(true),
            before_steal_waker: Mutex::new(None),
            counts: Counts::default(),
        };
        reactor.watch(&reactor.wake, libc::EPOLLIN as u32, WAKE)?;
        Ok(reactor)
    }

    fn watch(&self, file: &OwnedFd, events: u32, key: u64) -> io::Result<()> {
        let mut event = libc::epoll_event { events, u64: key };
        // SAFETY: both descriptors are open and `event` is valid.
        check(unsafe {
            libc::epoll_ctl(
                self.epoll.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                file.as_raw_fd(),
                &mut event,
            )
        })?;
        Ok(())
    }

    fn wake_driver(&self) {
        let one = 1u64.to_ne_bytes();
        // SAFETY: writes eight bytes from a buffer of eight.
        unsafe { libc::write(self.wake.as_raw_fd(), one.as_ptr().cast(), one.len()) };
    }

    fn drive(&self, timeout: Option<Duration>) {
        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 64];
        let millis = match timeout {
            None => -1,
            Some(timeout) => timeout
                .as_micros()
                .div_ceil(1000)
                .min(libc::c_int::MAX as u128) as libc::c_int,
        };
        // SAFETY: `events` has room for 64 events.
        let count =
            unsafe { libc::epoll_wait(self.epoll.as_raw_fd(), events.as_mut_ptr(), 64, millis) };
        let count = usize::try_from(count).unwrap_or(0);

        for event in &events[..count] {
            let (bits, key) = (event.events as libc::c_int, event.u64);
            if key == WAKE {
                let mut counter = [0u8; 8];
                // SAFETY: reads eight bytes into a buffer of eight.
                unsafe { libc::read(self.wake.as_raw_fd(), counter.as_mut_ptr().cast(), 8) };
                continue;
            }
            let mut ready = Ready::EMPTY;
            if bits & (libc::EPOLLIN | libc::EPOLLPRI) != 0 {
                ready |= Ready::READABLE;
            }
            if bits & libc::EPOLLOUT != 0 {
                ready |= Ready::WRITABLE;
            }
            if bits & (libc::EPOLLRDHUP | libc::EPOLLHUP) != 0 {
                ready |= Ready::READABLE | Ready::READ_CLOSED;
            }
            if bits & (libc::EPOLLHUP | libc::EPOLLERR) != 0 {
                ready |= Ready::READABLE | Ready::WRITABLE | Ready::WRITE_CLOSED;
            }
            let readiness = self
                .registered
                .lock()
                .unwrap()
                .get(&key)
                .map(|registered| registered.readiness.clone());
            if let Some(readiness) = readiness {
                readiness.set_ready(ready);
            }
        }
    }
}

impl Reactor for EpollReactor {
    fn register(
        &self,
        fd: BorrowedFd<'_>,
        _interest: Interest,
    ) -> io::Result<(Readiness, RegistrationToken)> {
        let file = fd.try_clone_to_owned()?;
        let readiness = Readiness::new();
        let key = self.next_key.fetch_add(1, SeqCst);
        let mut registered = self.registered.lock().unwrap();
        let events = libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLRDHUP | libc::EPOLLET;
        self.watch(&file, events as u32, key)?;
        registered.insert(
            key,
            Registered {
                file,
                readiness: readiness.clone(),
            },
        );
        self.counts.registered.fetch_add(1, SeqCst);
        Ok((readiness, RegistrationToken(key)))
    }

    fn deregister(&self, token: RegistrationToken) -> io::Result<()> {
        let registered = self.registered.lock().unwrap().remove(&token.0);
        if let Some(registered) = registered {
            // SAFETY: both descriptors are open.
            check(unsafe {
                libc::epoll_ctl(
                    self.epoll.as_raw_fd(),
                    libc::EPOLL_CTL_DEL,
                    registered.file.as_raw_fd(),
                    std::ptr::null_mut(),
                )
            })?;
        }
        self.counts.deregistered.fetch_add(1, SeqCst);
        Ok(())
    }

    fn park(&self, worker: usize, mode: ParkMode, timers: &Timers<'_>) -> bool {
        if mode == ParkMode::Poll {
            self.counts.polled.fetch_add(1, SeqCst);
            if let Ok(_driver) = self.driver.try_lock() {
                self.drive(Some(Duration::ZERO));
                return true;
            }
            return false;
        }

        let ParkMode::Park(limit) = mode else {
            unreachable!();
        };

        let slot = &self.workers[worker];
        let mut park = slot.park.lock().unwrap();
        if *park == Park::Notified {
            *park = Park::Awake;
            return false;
        }

        if let Ok(_driver) = self.driver.try_lock() {
            *park = Park::Driving;
            drop(park);

            // Publish timekeeper *before* sampling `timers.next_timeout()` so
            // any concurrent timer insert either wakes us or is observed here.
            self.timekeeper.store(worker + 1, SeqCst);
            let timer_timeout = timers.next_timeout();
            let timeout = match (limit, timer_timeout) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            };
            if timeout != Some(Duration::ZERO) {
                self.drive(timeout);
            }
            self.timekeeper.store(0, SeqCst);

            *slot.park.lock().unwrap() = Park::Awake;
            self.counts.drove.fetch_add(1, SeqCst);
            return true;
        }

        self.counts.waited.fetch_add(1, SeqCst);
        *park = Park::Waiting;
        let still_waiting = |park: &mut Park| *park == Park::Waiting;
        park = match limit {
            None => slot.unparked.wait_while(park, still_waiting).unwrap(),
            Some(timeout) => {
                slot.unparked
                    .wait_timeout_while(park, timeout, still_waiting)
                    .unwrap()
                    .0
            }
        };
        *park = Park::Awake;
        false
    }

    fn unpark(&self, worker: usize) {
        let slot = &self.workers[worker];
        let mut park = slot.park.lock().unwrap();
        match std::mem::replace(&mut *park, Park::Notified) {
            Park::Driving => self.wake_driver(),
            Park::Waiting => slot.unparked.notify_one(),
            Park::Awake | Park::Notified => {}
        }
    }

    fn unpark_for_timer(&self) {
        self.counts.timer_unparks.fetch_add(1, SeqCst);
        if self.timekeeper.load(SeqCst) != 0 {
            self.wake_driver();
        }
    }

    fn allow_block_in_place(&self) -> bool {
        self.allow_block_in_place.load(SeqCst)
    }

    fn before_steal(&self, _worker: usize) {
        self.counts.before_steal.fetch_add(1, SeqCst);
        if let Some(waker) = self.before_steal_waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn worker_start(&self, _worker: usize) {
        self.counts.started.fetch_add(1, SeqCst);
    }

    fn worker_stop(&self, _worker: usize) {
        self.counts.stopped.fetch_add(1, SeqCst);
    }

    fn shutdown(&self) {
        self.counts.shutdowns.fetch_add(1, SeqCst);
        for registered in self.registered.lock().unwrap().values() {
            registered.readiness.shutdown();
        }
    }
}

fn runtime(mut builder: Builder) -> (Runtime, Arc<EpollReactor>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let runtime = builder
        .enable_time()
        .io_reactor(move |workers| {
            let reactor = Arc::new(EpollReactor::new(workers)?);
            tx.send(reactor.clone()).unwrap();
            Ok(reactor as Arc<dyn Reactor>)
        })
        .build()
        .unwrap();
    (runtime, rx.recv().unwrap())
}

fn multi_thread(workers: usize) -> (Runtime, Arc<EpollReactor>) {
    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(workers);
    runtime(builder)
}

fn current_thread() -> (Runtime, Arc<EpollReactor>) {
    runtime(Builder::new_current_thread())
}

async fn echo(clients: usize, rounds: usize) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let mut connections = Vec::new();
        for _ in 0..clients {
            let (mut stream, _) = listener.accept().await.unwrap();
            connections.push(tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    let count = stream.read(&mut buf).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    stream.write_all(&buf[..count]).await.unwrap();
                }
            }));
        }
        for connection in connections {
            connection.await.unwrap();
        }
    });

    let clients: Vec<_> = (0..clients)
        .map(|client| {
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(address).await.unwrap();
                for round in 0..rounds {
                    let message = format!("client {client} round {round}");
                    stream.write_all(message.as_bytes()).await.unwrap();
                    let mut reply = vec![0u8; message.len()];
                    stream.read_exact(&mut reply).await.unwrap();
                    assert_eq!(reply, message.as_bytes());
                }
            })
        })
        .collect();
    for client in clients {
        client.await.unwrap();
    }
    server.await.unwrap();
}

#[test]
fn the_factory_is_told_the_number_of_workers() {
    let (_runtime, reactor) = multi_thread(3);
    assert_eq!(reactor.workers.len(), 3);
    let (_runtime, reactor) = current_thread();
    assert_eq!(reactor.workers.len(), 1);
}

#[test]
fn a_factory_error_fails_the_build() {
    let error = Builder::new_multi_thread()
        .io_reactor(|_| Err(io::Error::new(io::ErrorKind::Unsupported, "no reactor")))
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}

#[test]
fn sockets_and_metrics_are_served_by_the_reactor() {
    let (runtime, reactor) = multi_thread(4);
    runtime.block_on(echo(16, 50));

    let expected = 1 + 2 * 16;
    assert_eq!(reactor.counts.registered.load(SeqCst), expected);
    assert_eq!(reactor.counts.deregistered.load(SeqCst), expected);
    assert!(reactor.registered.lock().unwrap().is_empty());
    assert_ne!(reactor.counts.drove.load(SeqCst), 0);

    let metrics = runtime.metrics();
    assert_eq!(metrics.io_driver_fd_registered_count(), expected as u64);
    assert_eq!(metrics.io_driver_fd_deregistered_count(), expected as u64);
}

#[test]
fn sockets_are_served_on_a_current_thread_runtime() {
    let (runtime, reactor) = current_thread();
    runtime.block_on(echo(4, 20));

    assert_eq!(reactor.counts.registered.load(SeqCst), 1 + 2 * 4);
    assert_eq!(reactor.counts.deregistered.load(SeqCst), 1 + 2 * 4);
    assert_ne!(reactor.counts.drove.load(SeqCst), 0);
    assert_eq!(reactor.counts.waited.load(SeqCst), 0);
}

#[test]
fn async_fd_works_on_a_reactor_runtime() {
    let (runtime, reactor) = multi_thread(2);
    runtime.block_on(async {
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        // SAFETY: `a` and `b` are valid open file descriptors owned by AsyncFd.
        let a = unsafe { AsyncFd::register(a).unwrap() };
        let b = unsafe { AsyncFd::register(b).unwrap() };

        {
            let mut guard = a.writable().await.unwrap();
            let n = guard
                .try_io(|inner| io::Write::write(&mut inner.get_ref(), b"hi"))
                .unwrap()
                .unwrap();
            assert_eq!(n, 2);
        }
        {
            let mut guard = b.readable().await.unwrap();
            let mut buf = [0u8; 4];
            let n = guard
                .try_io(|inner| io::Read::read(&mut inner.get_ref(), &mut buf))
                .unwrap()
                .unwrap();
            assert_eq!(&buf[..n], b"hi");
        }
    });
    assert_eq!(reactor.counts.registered.load(SeqCst), 2);
    assert_eq!(reactor.counts.deregistered.load(SeqCst), 2);
}

#[test]
fn a_timer_bounds_the_park() {
    for (runtime, _reactor) in [multi_thread(2), current_thread()] {
        let started = Instant::now();
        runtime.block_on(async {
            tokio::spawn(tokio::time::sleep(Duration::from_millis(50)))
                .await
                .unwrap();
        });
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(50), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    }
}

#[test]
fn a_timer_set_while_workers_are_parked_wakes_one() {
    let (runtime, reactor) = multi_thread(2);
    while reactor.counts.waited.load(SeqCst) == 0
        || *reactor.workers[0].park.lock().unwrap() == Park::Awake
        || *reactor.workers[1].park.lock().unwrap() == Park::Awake
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    let before = reactor.counts.timer_unparks.load(SeqCst);

    let started = Instant::now();
    runtime.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
    let elapsed = started.elapsed();

    assert!(elapsed >= Duration::from_millis(50), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    assert_ne!(reactor.counts.timer_unparks.load(SeqCst), before);
}

/// Repeatedly races timer insertions against worker park/unpark transitions
/// to verify no timer wake is lost during driver handoff.
#[test]
fn concurrent_timer_insert_and_worker_handoff_never_loses_timer() {
    let (runtime, _reactor) = multi_thread(4);
    for _ in 0..40 {
        let handle = runtime.handle().clone();
        // Nudge workers so driver ownership rotates while a short timer is inserted.
        let churn = runtime.spawn(async {
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
        });
        let started = Instant::now();
        runtime.block_on(async {
            let t = handle.spawn(tokio::time::sleep(Duration::from_millis(5)));
            let _ = churn.await;
            t.await.unwrap();
        });
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timer took too long: {:?}",
            started.elapsed()
        );
    }
}

/// Paused time and auto-advance must continue to work on `current_thread`.
#[test]
fn paused_time_auto_advances_on_current_thread() {
    let mut builder = Builder::new_current_thread();
    builder.start_paused(true);
    let (runtime, _reactor) = runtime(builder);

    let real_start = Instant::now();
    runtime.block_on(async {
        let before = tokio::time::Instant::now();
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(before.elapsed() >= Duration::from_secs(30));
    });
    assert!(
        real_start.elapsed() < Duration::from_secs(2),
        "paused clock slept in real time: {:?}",
        real_start.elapsed()
    );
}

/// `TimerFlavor::Alternative` works cleanly with an external reactor.
#[test]
fn alternative_timer_works_with_reactor() {
    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(2).enable_alt_timer();
    let (runtime, _reactor) = runtime(builder);

    let started = Instant::now();
    runtime.block_on(async {
        tokio::spawn(tokio::time::sleep(Duration::from_millis(30)))
            .await
            .unwrap();
    });
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(30), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
}

/// Timers that outlive runtime shutdown must fail instead of hanging on both
/// `multi_thread` and `current_thread`.
#[test]
fn shutdown_fails_timers_on_multi_thread_and_current_thread() {
    for (runtime, _reactor) in [multi_thread(2), current_thread()] {
        let mut sleep = Box::pin(runtime.block_on(async {
            let mut sleep = Box::pin(tokio::time::sleep(Duration::from_secs(60)));
            // Poll once to register it in the timer wheel.
            let _ = futures::poll!(sleep.as_mut());
            // Wrap in Option to avoid clippy::async_yields_async.
            Some(sleep)
        }))
        .take()
        .unwrap();

        drop(runtime);

        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            futures::executor::block_on(sleep.as_mut());
        }));
        assert!(
            panicked.is_err(),
            "polling timer after shutdown should panic"
        );
    }
}

/// Calling `Readiness::set_ready` after `Readiness::shutdown` must be a safe
/// no-op even with debug assertions enabled.
#[test]
fn set_ready_after_shutdown_is_noop() {
    let readiness = Readiness::new();
    readiness.shutdown();
    readiness.set_ready(Ready::READABLE | Ready::WRITABLE);
}

#[test]
fn a_task_spawned_from_outside_unparks_a_worker() {
    let (runtime, reactor) = multi_thread(2);
    while *reactor.workers[0].park.lock().unwrap() == Park::Awake
        || *reactor.workers[1].park.lock().unwrap() == Park::Awake
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    let (tx, rx) = std::sync::mpsc::channel();
    runtime.spawn(async move { tx.send(std::thread::current().id()).unwrap() });
    let ran_on = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_ne!(ran_on, std::thread::current().id());
}

#[test]
fn workers_and_shutdown_are_announced() {
    let (runtime, reactor) = multi_thread(3);
    runtime.block_on(echo(2, 2));
    assert_eq!(reactor.counts.started.load(SeqCst), 3);
    assert_eq!(reactor.counts.stopped.load(SeqCst), 0);
    assert_eq!(reactor.counts.shutdowns.load(SeqCst), 0);

    drop(runtime);
    assert_eq!(reactor.counts.started.load(SeqCst), 3);
    assert_eq!(reactor.counts.stopped.load(SeqCst), 3);
    assert_ne!(reactor.counts.shutdowns.load(SeqCst), 0);

    let (runtime, reactor) = current_thread();
    runtime.block_on(echo(1, 1));
    drop(runtime);
    assert_eq!(reactor.counts.started.load(SeqCst), 0);
    assert_ne!(reactor.counts.shutdowns.load(SeqCst), 0);
}

#[test]
fn shutdown_fails_what_is_still_registered() {
    let (runtime, reactor) = multi_thread(1);
    let listener = runtime.block_on(async { TcpListener::bind("127.0.0.1:0").await.unwrap() });
    assert_eq!(reactor.registered.lock().unwrap().len(), 1);

    runtime.shutdown_background();
    while reactor.counts.shutdowns.load(SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    let accepted = futures::executor::block_on(listener.accept());
    let error = accepted.unwrap_err();
    assert!(tokio::runtime::is_rt_shutdown_err(&error), "{error:?}");
}

#[test]
fn block_in_place_respects_allow_block_in_place() {
    let (runtime, reactor) = multi_thread(2);
    reactor.allow_block_in_place.store(false, SeqCst);

    let err = runtime.block_on(async {
        tokio::spawn(async {
            tokio::task::block_in_place(|| {});
        })
        .await
        .unwrap_err()
    });
    assert!(err.is_panic());
    let msg = err.into_panic();
    let msg = msg
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| msg.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(msg.contains("does not support it"), "{msg:?}");

    // Runtime remains healthy and shuts down cleanly after the refused call.
    runtime.block_on(echo(2, 5));
    drop(runtime);
}

#[test]
fn block_in_place_succeeds_when_allowed() {
    let (runtime, _reactor) = multi_thread(2);
    runtime.block_on(async {
        tokio::spawn(async {
            tokio::task::block_in_place(|| std::thread::sleep(Duration::from_millis(10)));
            tokio::time::sleep(Duration::from_millis(10)).await;
        })
        .await
        .unwrap();
    });
}

#[test]
fn before_steal_hook_wakes_local_work() {
    let (runtime, reactor) = multi_thread(2);
    runtime.block_on(async {
        let reactor_for_task = reactor.clone();
        let done = tokio::spawn(async move {
            let mut yielded = false;
            std::future::poll_fn(|cx| {
                if yielded {
                    std::task::Poll::Ready(())
                } else {
                    yielded = true;
                    *reactor_for_task.before_steal_waker.lock().unwrap() = Some(cx.waker().clone());
                    std::task::Poll::Pending
                }
            })
            .await;
        });
        done.await.unwrap();
    });
    assert_ne!(reactor.counts.before_steal.load(SeqCst), 0);
}

/// `tokio::fs` must work cleanly on an external reactor runtime even when
/// `io-uring` is compiled in (it must fall back to `spawn_blocking`, not
/// register an fs ring on an unpolled mio driver).
#[test]
fn fs_works_on_a_reactor_runtime() {
    let (runtime, _reactor) = multi_thread(2);
    let path = std::env::temp_dir().join(format!(
        "tokio-io-reactor-fs-test-{}.txt",
        std::process::id()
    ));
    runtime.block_on(async {
        tokio::fs::write(&path, b"hello reactor").await.unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes, b"hello reactor");
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
#[cfg(feature = "signal")]
fn signals_are_refused() {
    let (runtime, _reactor) = current_thread();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(async {
            let _ = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup());
        })
    }))
    .unwrap_err();
    let message = panic.downcast_ref::<&str>().copied().unwrap_or_default();
    assert!(message.contains("driven by an I/O reactor"), "{message:?}");
}
