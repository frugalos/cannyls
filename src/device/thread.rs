use fibers::sync::oneshot;
use fibers::Spawn;
use fibers_tasque::{DefaultCpuTaskQueue, TaskQueueExt};
use futures::{Async, Future, Poll};
use slog::Logger;
use std::cmp::max;
use std::ops::Range;
use std::sync::mpsc as std_mpsc;
use std::sync::mpsc::{RecvTimeoutError, SendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use trackable::error::ErrorKindExt;

use device::command::{Command, CommandReceiver, CommandSender, UsageLumpRange};
use device::queue::DeadlineQueue;
use device::{DeviceBuilder, DeviceStatus};
use lump::LumpId;
use metrics::DeviceMetrics;
use nvm::NonVolatileMemory;
use storage::{Storage, StorageUsage};
use {Error, ErrorKind, Result};

/// デバイスの実行スレッド.
#[derive(Debug)]
pub struct DeviceThread<N, S>
where
    N: NonVolatileMemory + Send + 'static,
    S: Spawn + Send + Clone + 'static,
{
    metrics: DeviceMetrics,
    queue: DeadlineQueue,
    storage: Arc<Mutex<Storage<N>>>,
    // storage: Arc<Mutex<Storage>,
    idle_threshold: Duration,
    max_queue_len: usize,
    max_keep_busy_duration: Duration,
    busy_threshold: usize,
    start_busy_time: Option<Instant>,
    command_tx: CommandSender,
    command_rx: CommandReceiver,
    logger: Logger,
    spawner: Option<S>,
}
impl<N, S> DeviceThread<N, S>
where
    N: NonVolatileMemory + Send + 'static,
    S: Spawn + Send + Clone + 'static,
{
    /// デバイスの実行スレッドを起動する.
    pub fn spawn<F>(
        builder: DeviceBuilder<S>,
        init_storage: F,
    ) -> (DeviceThreadHandle, DeviceThreadMonitor)
    where
        F: FnOnce() -> Result<Storage<N>> + Send + 'static,
    {
        let mut metrics = DeviceMetrics::new(&builder.metrics);
        metrics.status.set(f64::from(DeviceStatus::Starting as u8));

        let (command_tx, command_rx) = std_mpsc::channel();
        let (monitored, monitor) = oneshot::monitor();
        let handle = DeviceThreadHandle {
            command_tx: command_tx.clone(),
            metrics: Arc::new(metrics.clone()),
        };
        thread::spawn(move || {
            let result = track!(init_storage()).and_then(|storage| {
                metrics.storage = Some(storage.metrics().clone());
                metrics.status.set(f64::from(DeviceStatus::Running as u8));
                let mut device = DeviceThread {
                    metrics: metrics.clone(),
                    queue: DeadlineQueue::new(),
                    storage: Arc::new(Mutex::new(storage)),
                    idle_threshold: builder.idle_threshold,
                    max_queue_len: builder.max_queue_len,
                    max_keep_busy_duration: builder.max_keep_busy_duration,
                    busy_threshold: builder.busy_threshold,
                    start_busy_time: None,
                    command_tx,
                    command_rx,
                    logger: builder.logger,
                    spawner: builder.spawner,
                };
                loop {
                    match track!(device.run_once()) {
                        Err(e) => break Err(e),
                        Ok(false) => break Ok(()),
                        Ok(true) => {}
                    }
                }
            });
            metrics.status.set(f64::from(DeviceStatus::Stopped as u8));
            metrics.storage = None;
            monitored.exit(result);
        });

        (handle, DeviceThreadMonitor(monitor))
    }

    fn run_once(&mut self) -> Result<bool> {
        if let Ok(command) = self.command_rx.try_recv() {
            track!(self.check_queue_limit())?;
            self.queue.push(command);
            Ok(true)
        } else if let Some(command) = self.queue.pop() {
            track!(self.check_overload())?;
            self.metrics.dequeued_commands.increment(&command);
            track!(self.handle_command(command))
        } else {
            match self.command_rx.recv_timeout(self.idle_threshold) {
                Err(RecvTimeoutError::Disconnected) => unreachable!(),
                Err(RecvTimeoutError::Timeout) => {
                    self.metrics.side_jobs.increment();
                    // track!(self.storage.run_side_job_once())?;
                    let mut lock = self.storage.lock().unwrap();
                    track!(lock.run_side_job_once())?;
                    Ok(true)
                }
                Ok(command) => {
                    track!(self.check_queue_limit())?;
                    self.queue.push(command);
                    Ok(true)
                }
            }
        }
    }

    fn handle_command(&mut self, command: Command) -> Result<bool> {
        match command {
            Command::Get(c) => {
                // let result = track!(self.storage.get(c.lump_id()));
                let mut lock = self.storage.lock().unwrap();
                let result = track!(lock.get(c.lump_id()));
                if result.is_err() {
                    self.metrics.failed_commands.get.increment();
                }
                c.reply(result);
                Ok(true)
            }
            Command::Head(c) => {
                // let value = self.storage.head(c.lump_id());
                let mut lock = self.storage.lock().unwrap();
                let value = lock.head(c.lump_id());
                c.reply(Ok(value));
                Ok(true)
            }
            Command::List(c) => {
                // let value = self.storage.list();
                let mut lock = self.storage.lock().unwrap();
                let value = lock.list();
                c.reply(Ok(value));
                Ok(true)
            }
            Command::ListRange(c) => {
                // let value = self.storage.list_range(c.lump_range());
                let mut lock = self.storage.lock().unwrap();
                let value = lock.list_range(c.lump_range());
                c.reply(Ok(value));
                Ok(true)
            }
            Command::Put(c) => {
                debug!(self.logger, "Put LumpId=(\"{}\")", c.lump_id());
                // let result = track!(self.storage.put(c.lump_id(), c.lump_data()));
                let mut lock = self.storage.lock().unwrap();
                let result = track!(lock.put(c.lump_id(), c.lump_data()));
                if result.is_err() {
                    self.metrics.failed_commands.put.increment();
                }
                if let Some(e) = maybe_critical_error(&result) {
                    c.reply(result);
                    Err(e)
                } else {
                    let do_sync = c.do_sync_journal();
                    c.reply(result);
                    if do_sync {
                        // let sync_result = track!(self.storage.journal_sync());
                        let sync_result = track!(lock.journal_sync());
                        sync_result.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            }
            Command::Delete(c) => {
                // let result = track!(self.storage.delete(c.lump_id()));
                let mut lock = self.storage.lock().unwrap();
                let result = track!(lock.delete(c.lump_id()));
                if result.is_err() {
                    self.metrics.failed_commands.delete.increment();
                }
                if let Some(e) = maybe_critical_error(&result) {
                    c.reply(result);
                    Err(e)
                } else {
                    let do_sync = c.do_sync_journal();
                    c.reply(result);
                    if do_sync {
                        // let sync_result = track!(self.storage.journal_sync());
                        let sync_result = track!(lock.journal_sync());
                        sync_result.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            }
            Command::DeleteRange(c) => {
                // let result = track!(self.storage.delete_range(c.lump_range().clone()));
                let mut lock = self.storage.lock().unwrap();
                let result = track!(lock.delete_range(c.lump_range().clone()));
                if result.is_err() {
                    self.metrics.failed_commands.delete_range.increment();
                }
                if let Some(e) = maybe_critical_error(&result) {
                    c.reply(result);
                    Err(e)
                } else {
                    let do_sync = c.do_sync_journal();
                    c.reply(result);
                    if do_sync {
                        // let sync_result = track!(self.storage.journal_sync());
                        let sync_result = track!(lock.journal_sync());
                        sync_result.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            }
            Command::UsageRange(c) => {
                match self.spawner {
                    None => {
                        // println!("usage_range at naive");
                        let mut lock = self.storage.lock().unwrap();
                        let result = lock.usage_range(c.lump_range());
                        c.reply(Ok(result));
                    }
                    Some(_) => {
                        let storage = self.storage.clone();
                        let future = DefaultCpuTaskQueue
                            .async_call(move || {
                                // println!("usage_range at async_call");
                                let mut lock = storage.lock().unwrap();
                                let result = lock.usage_range(c.lump_range());
                                c.reply(Ok(result));
                            })
                            .map_err(|_| ());

                        // let progress = c.lump_range().start.clone();
                        // let future = StorageUsageFuture {
                        //     storage: self.storage.clone(),
                        //     usage_lump_range: Some(c),
                        //     skip: 0,
                        //     usage: 0,
                        // };

                        self.spawner.as_ref().expect("Never fails").spawn(future);
                    }
                }
                Ok(true)
            }
            Command::Stop(_) => Ok(false),
        }
    }

    fn check_overload(&mut self) -> Result<()> {
        if self.queue.len() < self.busy_threshold {
            if self.start_busy_time.is_some() {
                self.start_busy_time = None;
            }
        } else if let Some(elapsed) = self.start_busy_time.map(|t| t.elapsed()) {
            track_assert!(elapsed <= self.max_keep_busy_duration, ErrorKind::DeviceBusy;
                              elapsed, self.max_keep_busy_duration, self.busy_threshold);
        } else {
            self.start_busy_time = Some(Instant::now());
        }
        Ok(())
    }

    fn check_queue_limit(&mut self) -> Result<()> {
        track_assert!(self.queue.len() <= self.max_queue_len, ErrorKind::DeviceBusy;
                      self.queue.len(), self.max_queue_len);
        Ok(())
    }
}

/// ストレージのデータが壊れている可能性があるエラーかどうかを判定.
fn maybe_critical_error<T>(result: &Result<T>) -> Option<Error> {
    result.as_ref().err().and_then(|e| match *e.kind() {
        ErrorKind::InconsistentState | ErrorKind::StorageCorrupted | ErrorKind::Other => {
            Some(e.clone())
        }
        _ => None,
    })
}

/// デバイスの実行スレッドの死活監視用オブジェクト.
#[derive(Debug)]
pub struct DeviceThreadMonitor(oneshot::Monitor<(), Error>);
impl Future for DeviceThreadMonitor {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        track!(self
            .0
            .poll()
            .map_err(|e| e.unwrap_or_else(|| ErrorKind::DeviceTerminated
                .cause("`DeviceThread` terminated unintentionally")
                .into())))
    }
}

/// デバイススレッドを操作するためのハンドル.
#[derive(Debug, Clone)]
pub struct DeviceThreadHandle {
    command_tx: CommandSender,
    metrics: Arc<DeviceMetrics>, // 必須では無いが`Clone`時の効率を上げるために`Arc`で囲む.
}
impl DeviceThreadHandle {
    pub fn send_command(&self, command: Command) {
        self.metrics.enqueued_commands.increment(&command);
        if let Err(SendError(command)) = self.command_tx.send(command) {
            self.metrics.dequeued_commands.increment(&command);
            self.metrics.failed_commands.increment(&command);
        }
    }
    pub fn metrics(&self) -> &Arc<DeviceMetrics> {
        &self.metrics
    }
}

#[derive(Debug)]
pub struct StorageUsageFuture<N>
where
    N: NonVolatileMemory,
{
    storage: Arc<Mutex<Storage<N>>>,
    usage_lump_range: Option<UsageLumpRange>,
    skip: usize,
    usage: u32,
}

impl<N> Future for StorageUsageFuture<N>
where
    N: NonVolatileMemory,
{
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let range = self.usage_lump_range.as_ref().expect("").lump_range();

        let lock = self.storage.lock().unwrap();
        let result = lock.usage_range_skip(range, self.skip, 10000000);
        let usage_bytes = result.as_bytes().unwrap_or(0);
        self.skip += 10000000;
        self.usage += usage_bytes;

        // println!("usage_bytes:{}", usage_bytes);

        if usage_bytes == 0 {
            let result = match self.usage {
                0 => StorageUsage::unknown(),
                _ => StorageUsage::approximate(self.usage),
            };
            let mut usage_lump_range = None;
            std::mem::swap(&mut self.usage_lump_range, &mut usage_lump_range);
            // println!("end-future: {:?}", result);
            usage_lump_range.expect("").reply(Ok(result));
            return Ok(Async::Ready(()));
        }

        Ok(Async::NotReady)
    }
}
