use fibers::sync::oneshot;
use futures::{Future, Poll};
use slog::Logger;
use std::sync::mpsc as std_mpsc;
use std::sync::mpsc::{RecvTimeoutError, SendError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use trackable::error::ErrorKindExt;

use super::execution_observer::ExecutionObserver;
use super::failure::TakedownPolicy;
use device::command::{Command, CommandReceiver, CommandSender};
use device::queue::DeadlineQueue;
use device::{DeviceBuilder, DeviceStatus};
use metrics::DeviceMetrics;
use nvm::NonVolatileMemory;
use storage::Storage;
use {Error, ErrorKind, Result};

/// デバイスの実行スレッド.
#[derive(Debug)]
pub struct DeviceThread<N>
where
    N: NonVolatileMemory + Send + 'static,
{
    metrics: DeviceMetrics,
    queue: DeadlineQueue,
    storage: Storage<N>,
    idle_threshold: Duration,
    max_queue_len: usize,
    max_keep_busy_duration: Duration,
    busy_threshold: usize,
    start_busy_time: Option<Instant>,
    command_tx: CommandSender,
    command_rx: CommandReceiver,
    logger: Logger,
    execution_observer: ExecutionObserver,
}
impl<N> DeviceThread<N>
where
    N: NonVolatileMemory + Send + 'static,
{
    /// デバイスの実行スレッドを起動する.
    pub fn spawn<F>(
        builder: DeviceBuilder,
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
                let takedown_policy = builder.failure_policy.takedown_policy;
                let mut device = DeviceThread {
                    metrics: metrics.clone(),
                    queue: DeadlineQueue::new(),
                    storage,
                    idle_threshold: builder.idle_threshold,
                    max_queue_len: builder.max_queue_len,
                    max_keep_busy_duration: builder.max_keep_busy_duration,
                    busy_threshold: builder.busy_threshold,
                    start_busy_time: None,
                    command_tx,
                    command_rx,
                    logger: builder.logger.clone(),
                    execution_observer: ExecutionObserver::new(
                        builder.logger,
                        builder.failure_policy.io_latency_threshold.clone(),
                        builder.failure_policy.io_error_threshold.clone(),
                    ),
                };
                loop {
                    // takedown_policy に応じて、エラー時にどうするか決める
                    // 実装を簡単にするため、どんな場合もエラーの回数は覚えておく
                    let mut error_count: u64 = 0;
                    match track!(device.run_once()) {
                        Err(e) => {
                            error_count += 1;
                            match &takedown_policy {
                                &TakedownPolicy::Stop => break Err(e),
                                &TakedownPolicy::Tolerate(limit) => {
                                    if error_count >= limit {
                                        break Err(e);
                                    }
                                }
                                &TakedownPolicy::Keep => {}
                            }
                        }
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
            let result = track!(self.handle_command(command))?;
            if self.execution_observer.is_failing() {
                warn!(self.logger, "execution_observer says it's failing!");
                return Err(ErrorKind::Other
                    .cause("execution_observer says it's failing!")
                    .into());
            }
            Ok(result)
        } else {
            match self.command_rx.recv_timeout(self.idle_threshold) {
                Err(RecvTimeoutError::Disconnected) => unreachable!(),
                Err(RecvTimeoutError::Timeout) => {
                    self.metrics.side_jobs.increment();
                    track!(self.storage.run_side_job_once())?;
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

    /// Ok(true) を返す -> スレッド続行
    /// Ok(false) を返す -> スレッド正常終了
    /// Err(...) を返す -> スレッド異常終了
    fn handle_command(&mut self, command: Command) -> Result<bool> {
        match command {
            Command::Get(c) => {
                // time here
                let now = Instant::now();
                let result = track!(self.storage.get(c.lump_id()));
                let elapsed = now.elapsed();
                if result.is_err() {
                    self.metrics.failed_commands.get.increment();
                }
                // record here
                debug!(self.logger, "execution_observer is recording... (read)");
                self.execution_observer
                    .observe(elapsed, now, result.is_err());
                c.reply(result);
                Ok(true)
            }
            Command::Head(c) => {
                let value = self.storage.head(c.lump_id());
                c.reply(Ok(value));
                Ok(true)
            }
            Command::List(c) => {
                let value = self.storage.list();
                c.reply(Ok(value));
                Ok(true)
            }
            Command::ListRange(c) => {
                let value = self.storage.list_range(c.lump_range());
                c.reply(Ok(value));
                Ok(true)
            }
            Command::Put(c) => {
                debug!(self.logger, "Put LumpId=(\"{}\")", c.lump_id());
                // time here
                let now = Instant::now();
                let result = track!(self.storage.put(c.lump_id(), c.lump_data()));
                let elapsed = now.elapsed();
                if result.is_err() {
                    self.metrics.failed_commands.put.increment();
                }
                // record time
                debug!(self.logger, "execution_observer is recording... (write)");
                self.execution_observer
                    .observe(elapsed, now, result.is_err());

                if let Some(e) = maybe_critical_error(&result) {
                    c.reply(result);
                    Err(e)
                } else {
                    let do_sync = c.do_sync_journal();
                    c.reply(result);
                    if do_sync {
                        let sync_result = track!(self.storage.journal_sync());
                        sync_result.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            }
            Command::Delete(c) => {
                // time here
                let now = Instant::now();
                let result = track!(self.storage.delete(c.lump_id()));
                let elapsed = now.elapsed();
                if result.is_err() {
                    self.metrics.failed_commands.delete.increment();
                }
                // record here
                debug!(self.logger, "execution_observer is recording... (delete)");
                self.execution_observer
                    .observe(elapsed, now, result.is_err());
                if let Some(e) = maybe_critical_error(&result) {
                    c.reply(result);
                    Err(e)
                } else {
                    let do_sync = c.do_sync_journal();
                    c.reply(result);
                    if do_sync {
                        let sync_result = track!(self.storage.journal_sync());
                        sync_result.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            }
            Command::DeleteRange(c) => {
                // time here
                let now = Instant::now();
                let result = track!(self.storage.delete_range(c.lump_range()));
                let elapsed = now.elapsed();
                if result.is_err() {
                    self.metrics.failed_commands.delete_range.increment();
                }
                // record here
                debug!(
                    self.logger,
                    "execution_observer is recording... (delete_range)",
                );
                self.execution_observer
                    .observe(elapsed, now, result.is_err());
                if let Some(e) = maybe_critical_error(&result) {
                    c.reply(result);
                    Err(e)
                } else {
                    let do_sync = c.do_sync_journal();
                    c.reply(result);
                    if do_sync {
                        let sync_result = track!(self.storage.journal_sync());
                        sync_result.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            }
            Command::UsageRange(c) => {
                let usage = self.storage.usage_range(c.lump_range());
                c.reply(Ok(usage));
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
