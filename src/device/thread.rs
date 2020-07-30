use fibers::sync::oneshot;
use futures::{Future, Poll};
use slog::Logger;
use std::fmt::Debug;
use std::sync::mpsc as std_mpsc;
use std::sync::mpsc::{RecvTimeoutError, SendError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use trackable::error::ErrorKindExt;

use device::command::{Command, CommandReceiver, CommandSender};
use device::long_queue_policy::LongQueuePolicy;
use device::probabilistic::{Dropper, ProbabilisticDropper};
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
    long_queue_policy: LongQueuePolicy,
    dropper: Option<Box<dyn Dropper>>,
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
                // LongQueuePolicy が Drop だったら、この後 run_once で使うため、dropper を作っておく。
                let dropper = if let LongQueuePolicy::Drop { ratio } = builder.long_queue_policy {
                    Some(
                        Box::new(ProbabilisticDropper::new(builder.logger.clone(), ratio))
                            as Box<dyn Dropper>,
                    )
                } else {
                    None
                };
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
                    logger: builder.logger,
                    long_queue_policy: builder.long_queue_policy,
                    dropper,
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
            return self.push_to_queue(command);
        }
        if let Some(command) = self.queue.pop() {
            let result = track!(self.check_overload());
            // 過負荷になっていたら、long_queue_policy に応じて挙動を変える
            if let Err(e) = result {
                match &self.long_queue_policy {
                    LongQueuePolicy::RefuseNewRequests => {}
                    LongQueuePolicy::Stop => return Err(e),
                    LongQueuePolicy::Drop { .. } => {
                        // 確率 ratio で drop する
                        if self.dropper.as_mut().unwrap().will_drop() {
                            let elapsed = self.start_busy_time.map(|t| t.elapsed().as_secs());
                            info!(
                                self.logger,
                                "Request dropped: {:?}",
                                command;
                                "queue_len" => self.queue.len(),
                                "from_busy (sec)" => elapsed,
                            );
                            self.metrics.dequeued_commands.increment(&command);
                            let result = self.handle_command_with_error(
                                command,
                                ErrorKind::RequestDropped.cause(e).into(),
                            );
                            return Ok(result);
                        }
                    }
                }
            }
            self.metrics.dequeued_commands.increment(&command);
            return track!(self.handle_command(command));
        }

        match self.command_rx.recv_timeout(self.idle_threshold) {
            Err(RecvTimeoutError::Disconnected) => unreachable!(),
            Err(RecvTimeoutError::Timeout) => {
                self.metrics.side_jobs.increment();
                track!(self.storage.run_side_job_once())?;
                Ok(true)
            }
            Ok(command) => self.push_to_queue(command),
        }
    }

    /// ここでも command の処理をせざるを得ない都合上、終了しないかどうかの bool 値を返す。
    fn push_to_queue(&mut self, command: Command) -> Result<bool> {
        let result = track!(self.check_overload());
        let prioritized = command.prioritized();
        if let Err(e) = self.check_queue_limit() {
            // queue length の hard limit を突破しているので、prioritized かどうかに関係なくエラーを返す。
            // 常にリクエストを拒否すれば問題ない。
            let elapsed = self.start_busy_time.map(|t| t.elapsed().as_secs());
            info!(
                self.logger, "Request refused (hard limit): {:?}",
                command;
                "queue_len" => self.queue.len(),
                "queue_len_hard_limit" => self.max_queue_len,
                "from_busy (sec)" => elapsed,
            );
            self.metrics.dequeued_commands.increment(&command);
            let result =
                self.handle_command_with_error(command, ErrorKind::RequestRefused.cause(e).into());
            return Ok(result);
        }
        if let (Err(e), false) = (result, prioritized) {
            match &self.long_queue_policy {
                LongQueuePolicy::RefuseNewRequests => {
                    let elapsed = self.start_busy_time.map(|t| t.elapsed().as_secs());
                    info!(
                        self.logger, "Request refused: {:?}",
                        command;
                        "queue_len" => self.queue.len(),
                        "from_busy (sec)" => elapsed,
                    );
                    self.metrics.dequeued_commands.increment(&command);
                    let result = self.handle_command_with_error(
                        command,
                        ErrorKind::RequestRefused.cause(e).into(),
                    );
                    return Ok(result);
                }
                LongQueuePolicy::Stop => return Err(e),
                LongQueuePolicy::Drop { .. } => {}
            }
        }
        self.queue.push(command);
        Ok(true)
    }

    fn handle_command(&mut self, command: Command) -> Result<bool> {
        match command {
            Command::Get(c) => {
                let result = track!(self.storage.get(c.lump_id()));
                if result.is_err() {
                    self.metrics.failed_commands.get.increment();
                }
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
                let result = track!(self.storage.put(c.lump_id(), c.lump_data()));
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
                        let sync_result = track!(self.storage.journal_sync());
                        sync_result.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            }
            Command::Delete(c) => {
                let result = track!(self.storage.delete(c.lump_id()));
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
                        let sync_result = track!(self.storage.journal_sync());
                        sync_result.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            }
            Command::DeleteRange(c) => {
                let result = track!(self.storage.delete_range(c.lump_range()));
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

    // command に対し、常に指定されたエラーを返答する。
    // この関数自身は常に成功するため、handle_command と違い bool を返す。
    fn handle_command_with_error(&mut self, command: Command, error: Error) -> bool {
        match command {
            Command::Get(c) => c.reply(Err(error)),
            Command::Head(c) => c.reply(Err(error)),
            Command::List(c) => c.reply(Err(error)),
            Command::ListRange(c) => c.reply(Err(error)),
            Command::Put(c) => c.reply(Err(error)),
            Command::Delete(c) => c.reply(Err(error)),
            Command::DeleteRange(c) => c.reply(Err(error)),
            Command::UsageRange(c) => c.reply(Err(error)),
            Command::Stop(_) => {
                // ここに来た場合だけ false を返し、残りのパスは全て true を返す。
                return false;
            }
        }
        true
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
