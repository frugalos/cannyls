use prometrics::metrics::MetricBuilder;
use std::time::Duration;

use super::thread::DeviceThread;
use super::{failure::FailurePolicy, Device, DeviceHandle};
use nvm::NonVolatileMemory;
use slog::{Discard, Logger};
use storage::Storage;
use Result;

/// `Device`のビルダ.
#[derive(Debug, Clone)]
pub struct DeviceBuilder {
    pub(crate) metrics: MetricBuilder,
    pub(crate) idle_threshold: Duration,
    pub(crate) max_queue_len: usize,
    pub(crate) max_keep_busy_duration: Duration,
    pub(crate) busy_threshold: usize,
    pub(crate) logger: Logger,
    pub(crate) failure_policy: FailurePolicy,
}
impl DeviceBuilder {
    /// デフォルト設定で`DeviceBuilder`インスタンスを生成する.
    pub fn new() -> Self {
        DeviceBuilder {
            metrics: MetricBuilder::new(),
            idle_threshold: Duration::from_millis(100),
            max_queue_len: 100_000,
            max_keep_busy_duration: Duration::from_secs(600),
            busy_threshold: 1_000,
            logger: Logger::root(Discard, o!()),
            failure_policy: FailurePolicy::default(),
        }
    }

    /// メトリクス用の共通設定を登録する.
    ///
    /// デフォルト値は`MetricBuilder::new()`.
    pub fn metrics(&mut self, metrics: MetricBuilder) -> &mut Self {
        self.metrics = metrics;
        self
    }

    /// デバイスが暇だと判定するための閾値(時間)を設定する.
    ///
    /// この値以上、新規コマンドを受信しない期間が続いた場合には、
    /// デバイス(用のスレッド)が空いていると判断されて、
    /// ストレージの補助タスクが実行されるようになる.
    ///
    /// デフォルト値は`Duration::from_millis(100)`.
    pub fn idle_threshold(&mut self, duration: Duration) -> &mut Self {
        self.idle_threshold = duration;
        self
    }

    /// デバイスの最大キュー長.
    ///
    /// これを超えた数のコマンドがデバイスのキューに溜まると、
    /// そのデバイスは致命的に過負荷であると判断され、
    /// `ErrorKind::DeviceBusy`を終了理由として停止する.
    ///
    /// デフォルト値は`100_000`.
    pub fn max_queue_len(&mut self, n: usize) -> &mut Self {
        self.max_queue_len = n;
        self
    }

    /// デバイスが最大継続ビジー時間.
    ///
    /// これを超えてビジーな状態が続いた場合には、何か異常が発生しているものと判断され、
    /// `ErrorKind::DeviceBusy`の終了理由でデバイスが停止する.
    ///
    /// ビジー状態かどうかの判断には`busy_threshold`の値を用いる.
    ///
    /// デフォルト値は`Duration::from_secs(600)`.
    pub fn max_keep_busy_duration(&mut self, duration: Duration) -> &mut Self {
        self.max_keep_busy_duration = duration;
        self
    }

    /// デバイスがビジー状態かどうかを判定するための閾値.
    ///
    /// コマンドのキューの長さがこの値を超えている場合には、
    /// そのデバイスはビジー状態であるとみなされる.
    ///
    /// デバイス側は、特定のコマンドの優先度等は分からないため、
    /// ビジー状態だからといってコマンドを拒否することはないが、
    /// この状態が一定(`max_keep_busy_duration`)以上継続した場合には、
    /// そのデバイスが何かしらの異常により過負荷に陥っていると判断して、
    /// 停止させられる.
    ///
    /// デフォルト値は`1_000`.
    pub fn busy_threshold(&mut self, n: usize) -> &mut Self {
        self.busy_threshold = n;
        self
    }

    /// デバイススレッド用の logger を登録する
    pub fn logger(&mut self, logger: Logger) -> &mut Self {
        self.logger = logger;
        self
    }

    /// FailuerPolicy を登録する
    pub fn failure_policy(&mut self, failure_policy: FailurePolicy) -> &mut Self {
        self.failure_policy = failure_policy;
        self
    }

    /// 指定されたストレージを扱う`Device`を起動する.
    ///
    /// 起動したデバイス用に、一つの専用OSスレッドが割り当てられる.
    ///
    /// なお、スレッド起動後には、まず`init_storage()`が呼び出されて、
    /// ストレージインスタンスが生成される.
    ///
    /// # 注意
    ///
    /// 返り値の`Device`インスタンスが破棄されると、
    /// 起動したデバイススレッドも停止させられるので注意が必要.
    pub fn spawn<F, N>(&self, init_storage: F) -> Device
    where
        F: FnOnce() -> Result<Storage<N>> + Send + 'static,
        N: NonVolatileMemory + Send + 'static,
    {
        let (thread_handle, thread_monitor) = DeviceThread::spawn(self.clone(), init_storage);
        Device::new(thread_monitor, DeviceHandle(thread_handle))
    }
}
impl Default for DeviceBuilder {
    fn default() -> Self {
        Self::new()
    }
}
