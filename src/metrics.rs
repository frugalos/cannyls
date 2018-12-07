//! [Prometheus][prometheus]用のメトリクス.
//!
//! [prometheus]: https://prometheus.io/
use prometrics::metrics::{Counter, Gauge, MetricBuilder};

use block::BlockSize;
use device::{Command, DeviceStatus};
use storage::{JournalRecord, StorageHeader};

/// ジャーナル領域のキュー（リングバッファ）のメトリクス.
#[derive(Debug, Clone)]
pub struct JournalQueueMetrics {
    pub(crate) capacity_bytes: Gauge,
    pub(crate) consumed_bytes_at_starting: Counter,
    pub(crate) consumed_bytes_at_running: Counter,
    pub(crate) released_bytes: Counter,
    pub(crate) enqueued_records_at_starting: JournalRecordCounter,
    pub(crate) enqueued_records_at_running: JournalRecordCounter,
    pub(crate) dequeued_records: JournalRecordCounter,
}
impl JournalQueueMetrics {
    /// キューの容量.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_queue_capacity_bytes <GAUGE>
    /// ```
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes.value() as u64
    }

    /// キューによって消費されたバイト数の合計.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_queue_consumed_bytes_total { phase="starting|running" } <COUNTER>
    /// ```
    pub fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes_at_starting.value() as u64
            + self.consumed_bytes_at_running.value() as u64
    }

    /// キューが解放したバイト数の合計.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_queue_released_bytes_total <COUNTER>
    /// ```
    pub fn released_bytes(&self) -> u64 {
        self.released_bytes.value() as u64
    }

    /// キューの使用量.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_queue_consumed_bytes_total - cannyls_journal_queue_released_bytes_total
    /// ```
    pub fn usage_bytes(&self) -> u64 {
        // NOTE: 以下の順番で値を取得しないとアンダーフローする可能性がある
        let dec = self.released_bytes();
        let inc = self.consumed_bytes();
        inc - dec
    }

    /// キューに追加されたレコードの数.
    ///
    /// 返り値のタプルの第一要素は`phase="starting"`ラベルを持ち、第二要素は`phase="running"`ラベルを持つ.
    ///
    /// これらのカウンタは、通常のストレージ操作以外に、内部のGCの再配置によってもインクリメントされる.
    /// そのため、外部から発行された各操作の正確な数を知りたいのであれば、
    /// `DeviceMetrics`の方を参照する必要がある.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_queue_enqueued_records_total { type="put|embed|delete", phase="starting|running" } <COUNTER>
    /// ```
    pub fn enqueued_records(&self) -> (&JournalRecordCounter, &JournalRecordCounter) {
        (
            &self.enqueued_records_at_starting,
            &self.enqueued_records_at_running,
        )
    }

    /// キューから取り除かれたレコードの数.
    ///
    /// ここでカウントしている値には
    /// 「キューからは取り除かれたが、まだGC処理中で物理的には解放されていない」レコードも含まれている.
    ///
    /// そのため`queue_len()`の値が`0`でも、`usage_bytes()`の値は`0`ではないことがある.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_queue_dequeued_records_total { type="put|embed|delete" } <COUNTER>
    /// ```
    pub fn dequeued_records(&self) -> &JournalRecordCounter {
        &self.dequeued_records
    }

    /// 現在のキューの長さ(i.e., リングバッファの要素数).
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// sum(cannyls_journal_queue_enqueued_records_total - cannyls_journal_queue_dequeued_records_total)
    /// ```
    pub fn queue_len(&self) -> u64 {
        // NOTE: 以下の順番で値を取得しないとアンダーフローする可能性がある
        let dec = self.dequeued_records.sum();
        let inc = self.enqueued_records_at_starting.sum() + self.enqueued_records_at_running.sum();
        inc - dec
    }

    pub(crate) fn new(builder: &MetricBuilder) -> Self {
        let mut builder = builder.clone();
        builder.namespace("cannyls").subsystem("journal_queue");
        JournalQueueMetrics {
            capacity_bytes: builder
                .gauge("capacity_bytes")
                .help("Capacity of the ring buffer")
                .finish()
                .expect("Never fails"),
            consumed_bytes_at_starting: builder
                .counter("consumed_bytes_total")
                .help("Number of bytes consumed by the ring buffer")
                .label("phase", "starting")
                .finish()
                .expect("Never fails"),
            consumed_bytes_at_running: builder
                .counter("consumed_bytes_total")
                .help("Number of bytes consumed by the ring buffer")
                .label("phase", "running")
                .finish()
                .expect("Never fails"),
            released_bytes: builder
                .counter("released_bytes_total")
                .help("Number of bytes released from the ring buffer")
                .finish()
                .expect("Never fails"),
            enqueued_records_at_starting: JournalRecordCounter::new(
                &builder,
                "enqueued_records_total",
                "Number of records enqueued to the ring buffer",
                Some("starting"),
            ),
            enqueued_records_at_running: JournalRecordCounter::new(
                &builder,
                "enqueued_records_total",
                "Number of records enqueued to the ring buffer",
                Some("running"),
            ),
            dequeued_records: JournalRecordCounter::new(
                &builder,
                "dequeued_records_total",
                "Number of records dequeued from the ring buffer",
                None,
            ),
        }
    }
}

/// ストレージのジャーナル領域のメトリクス.
#[derive(Debug, Clone)]
pub struct JournalRegionMetrics {
    pub(crate) gc_enqueued_records: Counter,
    pub(crate) gc_dequeued_records: Counter,
    pub(crate) syncs: Counter,
    queue: JournalQueueMetrics,
}
impl JournalRegionMetrics {
    /// GC用のキューに格納されたレコードの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_region_gc_enqueued_records_total <COUNTER>
    /// ```
    pub fn gc_enqueued_records(&self) -> u64 {
        self.gc_enqueued_records.value() as u64
    }

    /// GC用のキューから取り出されたレコードの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_region_gc_dequeued_records_total <COUNTER>
    /// ```
    pub fn gc_dequeued_records(&self) -> u64 {
        self.gc_dequeued_records.value() as u64
    }

    /// `NonVolatileMemory`への同期命令の発行回数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_journal_region_syncs_total <COUNTER>
    /// ```
    pub fn syncs(&self) -> u64 {
        self.syncs.value() as u64
    }

    /// リングバッファのメトリクスを返す.
    pub fn queue(&self) -> &JournalQueueMetrics {
        &self.queue
    }

    pub(crate) fn new(builder: &MetricBuilder, queue: JournalQueueMetrics) -> Self {
        let mut builder = builder.clone();
        builder.namespace("cannyls").subsystem("journal_region");
        JournalRegionMetrics {
            gc_enqueued_records: builder
                .counter("gc_enqueued_records_total")
                .help("Number of records enqueued to the queue for GC")
                .finish()
                .expect("Never fails"),
            gc_dequeued_records: builder
                .counter("gc_dequeued_records_total")
                .help("Number of records dequeued from the queue for GC")
                .finish()
                .expect("Never fails"),
            syncs: builder
                .counter("syncs_total")
                .help("Number of synchronization instructions issued to the physical device")
                .finish()
                .expect("Never fails"),
            queue,
        }
    }
}

/// ジャーナル領域に格納されるレコードの種別毎のカウンタ.
///
/// なお、制御系のレコードは扱いが特殊なため、カウント対象には含まれていない.
#[derive(Debug, Clone)]
pub struct JournalRecordCounter {
    pub(crate) put: Counter,
    pub(crate) embed: Counter,
    pub(crate) delete: Counter,
    pub(crate) delete_range: Counter,
}
impl JournalRecordCounter {
    /// PUTレコードの数.
    pub fn put(&self) -> u64 {
        self.put.value() as u64
    }

    /// EMBEDレコードの数.
    pub fn embed(&self) -> u64 {
        self.embed.value() as u64
    }

    /// DELETEレコードの数.
    pub fn delete(&self) -> u64 {
        self.delete.value() as u64
    }

    /// DELETE_RANGEレコードの数.
    pub fn delete_range(&self) -> u64 {
        self.delete_range.value() as u64
    }

    pub(crate) fn increment<B>(&self, record: &JournalRecord<B>) {
        match *record {
            JournalRecord::Delete { .. } => self.delete.increment(),
            JournalRecord::EndOfRecords | JournalRecord::GoToFront => {}
            JournalRecord::Put { .. } => self.put.increment(),
            JournalRecord::Embed { .. } => self.embed.increment(),
            JournalRecord::DeleteRange { .. } => self.delete_range.increment(),
        }
    }

    fn new(builder: &MetricBuilder, name: &str, help: &str, phase: Option<&str>) -> Self {
        let counter = |record_type| {
            let mut m = builder.counter(name);
            if let Some(phase) = phase {
                m.label("phase", phase);
            }
            m.help(help)
                .label("type", record_type)
                .finish()
                .expect("Never fails")
        };
        JournalRecordCounter {
            put: counter("put"),
            embed: counter("embed"),
            delete: counter("delete"),
            delete_range: counter("delete_range"),
        }
    }

    fn sum(&self) -> u64 {
        self.put() + self.embed() + self.delete()
    }
}

/// データ領域用のアロケータのメトリクス.
#[derive(Debug, Clone)]
pub struct DataAllocatorMetrics {
    pub(crate) inserted_free_portions: Counter,
    pub(crate) removed_free_portions: Counter,
    pub(crate) allocated_portions_at_starting: Counter,
    pub(crate) allocated_portions_at_running: Counter,
    pub(crate) allocated_bytes_at_starting: Counter,
    pub(crate) allocated_bytes_at_running: Counter,
    pub(crate) released_portions: Counter,
    pub(crate) released_bytes: Counter,
    pub(crate) nospace_failures: Counter,
    pub(crate) block_size: BlockSize,
    pub(crate) capacity_bytes: u64,
}
impl DataAllocatorMetrics {
    /// フリーリストに挿入された要素の数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_inserted_free_portions_total <COUNTER>
    /// ```
    pub fn inserted_free_portions(&self) -> u64 {
        self.inserted_free_portions.value() as u64
    }

    /// フリーリストから削除された要素の数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_removed_free_portions_total <COUNTER>
    /// ```
    pub fn removed_free_portions(&self) -> u64 {
        self.removed_free_portions.value() as u64
    }

    /// フリーリストの長さ.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_inserted_free_portions_total - cannyls_data_allocator_removed_free_portions_total
    /// ```
    pub fn free_list_len(&self) -> usize {
        // NOTE: 以下の順番で値を取得しないとアンダーフローする可能性がある
        let dec = self.removed_free_portions();
        let inc = self.inserted_free_portions();
        (inc - dec) as usize
    }

    /// 部分領域の割当回数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_allocated_portions_total { phase="starting|running" } <COUNTER>
    /// ```
    pub fn allocated_portions(&self) -> u64 {
        self.allocated_portions_at_starting.value() as u64
            + self.allocated_portions_at_running.value() as u64
    }

    /// これまでに割り当てた部分領域のバイト数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_allocated_bytes_total { phase="starting|running" } <COUNTER>
    /// ```
    pub fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes_at_starting.value() as u64
            + self.allocated_bytes_at_running.value() as u64
    }

    /// 部分領域の解放回数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_released_portions_total <COUNTER>
    /// ```
    pub fn released_portions(&self) -> u64 {
        self.released_portions.value() as u64
    }

    /// これまでに解放された部分領域のバイト数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_released_bytes_total <COUNTER>
    /// ```
    pub fn released_bytes(&self) -> u64 {
        self.released_bytes.value() as u64
    }

    /// 空き領域不足による割当失敗回数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_nospace_failures_total <COUNTER>
    /// ```
    pub fn nospace_failures(&self) -> u64 {
        self.nospace_failures.value() as u64
    }

    pub(crate) fn new(builder: &MetricBuilder, capacity_bytes: u64, block_size: BlockSize) -> Self {
        let mut builder = builder.clone();
        builder.namespace("cannyls").subsystem("data_allocator");
        DataAllocatorMetrics {
            inserted_free_portions: builder
                .counter("inserted_free_portions_total")
                .help("Number of inserted portions into free list")
                .finish()
                .expect("Never fails"),
            removed_free_portions: builder
                .counter("removed_free_portions_total")
                .help("Number of removed portions from free list")
                .finish()
                .expect("Never fails"),
            allocated_portions_at_starting: builder
                .counter("allocated_portions_total")
                .help("Number of allocated portions")
                .label("phase", "starting")
                .finish()
                .expect("Never fails"),
            allocated_portions_at_running: builder
                .counter("allocated_portions_total")
                .help("Number of allocated portions")
                .label("phase", "running")
                .finish()
                .expect("Never fails"),
            allocated_bytes_at_starting: builder
                .counter("allocated_bytes_total")
                .help("Number of allocated bytes")
                .label("phase", "starting")
                .finish()
                .expect("Never fails"),
            allocated_bytes_at_running: builder
                .counter("allocated_bytes_total")
                .help("Number of allocated bytes")
                .label("phase", "running")
                .finish()
                .expect("Never fails"),
            released_portions: builder
                .counter("released_portions_total")
                .help("Number of released portions")
                .finish()
                .expect("Never fails"),
            released_bytes: builder
                .counter("released_bytes_total")
                .help("Number of released bytes")
                .finish()
                .expect("Never fails"),
            nospace_failures: builder
                .counter("nospace_failures_total")
                .help("Number of allocation failures caused by no available space")
                .finish()
                .expect("Never fails"),
            capacity_bytes,
            block_size,
        }
    }

    #[cfg(test)]
    pub(crate) fn usage_bytes(&self) -> u64 {
        // NOTE: 以下の順番で値を取得しないとアンダーフローする可能性がある
        let dec = self.released_bytes();
        let inc = self.allocated_bytes();
        inc - dec
    }

    pub(crate) fn count_allocation(&self, size: u16) {
        self.allocated_portions_at_running.increment();
        self.allocated_bytes_at_running
            .add_u64(u64::from(self.block_size.as_u16()) * u64::from(size));
    }

    pub(crate) fn count_releasion(&self, size: u16) {
        self.released_portions.increment();
        self.released_bytes
            .add_u64(u64::from(self.block_size.as_u16()) * u64::from(size));
    }
}

/// [`Device`]のメトリクス.
///
/// [`Device`]: ../device/struct.Device.html
#[derive(Debug, Clone)]
pub struct DeviceMetrics {
    pub(crate) status: Gauge,
    pub(crate) enqueued_commands: DeviceCommandCounter,
    pub(crate) dequeued_commands: DeviceCommandCounter,
    pub(crate) failed_commands: DeviceCommandCounter,
    pub(crate) busy_commands: DeviceCommandCounter,
    pub(crate) side_jobs: Counter,
    pub(crate) storage: Option<StorageMetrics>,
}
impl DeviceMetrics {
    /// デバイスの稼働状態.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// # 0=stopped
    /// # 1=starting
    /// # 2=running
    /// cannyls_device_status = 0|1|2
    /// ```
    pub fn status(&self) -> DeviceStatus {
        match self.status.value() as u8 {
            0 => DeviceStatus::Stopped,
            1 => DeviceStatus::Starting,
            2 => DeviceStatus::Running,
            _ => unreachable!(),
        }
    }

    /// デバイスのキューに挿入されたコマンドの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_device_enqueued_commands_total { command="put" } = <COUNTER>
    /// cannyls_device_enqueued_commands_total { command="get" } = <COUNTER>
    /// cannyls_device_enqueued_commands_total { command="head" } = <COUNTER>
    /// cannyls_device_enqueued_commands_total { command="delete" } = <COUNTER>
    /// cannyls_device_enqueued_commands_total { command="list" } = <COUNTER>
    /// cannyls_device_enqueued_commands_total { command="stop" } = <COUNTER>
    /// ```
    pub fn enqueued_commands(&self) -> &DeviceCommandCounter {
        &self.enqueued_commands
    }

    /// デバイスのキューから取り出されたコマンドの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_device_dequeued_commands_total { command="put" } = <COUNTER>
    /// cannyls_device_dequeued_commands_total { command="get" } = <COUNTER>
    /// cannyls_device_dequeued_commands_total { command="head" } = <COUNTER>
    /// cannyls_device_dequeued_commands_total { command="delete" } = <COUNTER>
    /// cannyls_device_dequeued_commands_total { command="list" } = <COUNTER>
    /// cannyls_device_dequeued_commands_total { command="stop" } = <COUNTER>
    /// ```
    pub fn dequeued_commands(&self) -> &DeviceCommandCounter {
        &self.dequeued_commands
    }

    /// 実行に失敗したコマンドの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_device_failed_commands_total { command="put" } = <COUNTER>
    /// cannyls_device_failed_commands_total { command="get" } = <COUNTER>
    /// cannyls_device_failed_commands_total { command="head" } = <COUNTER>
    /// cannyls_device_failed_commands_total { command="delete" } = <COUNTER>
    /// cannyls_device_failed_commands_total { command="list" } = <COUNTER>
    /// cannyls_device_failed_commands_total { command="stop" } = <COUNTER>
    /// ```
    pub fn failed_commands(&self) -> &DeviceCommandCounter {
        &self.failed_commands
    }

    /// デバイスが忙しくて実行を諦めたコマンドの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_device_busy_commands_total { command="put" } = <COUNTER>
    /// cannyls_device_busy_commands_total { command="get" } = <COUNTER>
    /// cannyls_device_busy_commands_total { command="head" } = <COUNTER>
    /// cannyls_device_busy_commands_total { command="delete" } = <COUNTER>
    /// cannyls_device_busy_commands_total { command="list" } = <COUNTER>
    /// cannyls_device_busy_commands_total { command="stop" } = <COUNTER>
    /// ```
    pub fn busy_commands(&self) -> &DeviceCommandCounter {
        &self.busy_commands
    }

    /// 補助タスクの実行回数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_device_side_jobs_total <COUNTER>
    /// ```
    pub fn side_jobs(&self) -> u64 {
        self.side_jobs.value() as u64
    }

    /// デバイスキューの長さ(i.e., 実行待ちのコマンド数).
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// sum(cannyls_device_enqueued_commands_total - cannyls_device_dequeued_commands_total)
    /// ```
    pub fn queue_len(&self) -> usize {
        // NOTE: 以下の順番で値を取得しないとアンダーフローする可能性がある
        let dec = self.dequeued_commands.sum();
        let inc = self.enqueued_commands.sum();
        (inc - dec) as usize
    }

    /// ストレージのメトリクスを返す.
    ///
    /// デバイスの状態が`Running`以外の場合には`None`が返る.
    pub fn storage(&self) -> Option<&StorageMetrics> {
        self.storage.as_ref()
    }

    pub(crate) fn new(builder: &MetricBuilder) -> Self {
        let mut builder = builder.clone();
        builder.namespace("cannyls").subsystem("device");
        DeviceMetrics {
            status: builder
                .gauge("status")
                .help("Status of the device (0=stopped, 1=starting, 2=running)")
                .finish()
                .expect("Never fails"),
            enqueued_commands: DeviceCommandCounter::new(
                &builder,
                "enqueued_commands_total",
                "Number of enqueued commands",
            ),
            dequeued_commands: DeviceCommandCounter::new(
                &builder,
                "dequeued_commands_total",
                "Number of dequeued commands",
            ),
            failed_commands: DeviceCommandCounter::new(
                &builder,
                "failed_commands_total",
                "Number of commands failed to execute",
            ),
            busy_commands: DeviceCommandCounter::new(
                &builder,
                "busy_commands_total",
                "Number of commands gave up to execute due to the device is busy",
            ),
            side_jobs: builder
                .counter("side_jobs_total")
                .help("Number of exeuction of side jobs")
                .finish()
                .expect("Never fails"),
            storage: None,
        }
    }
}

/// デバイスのコマンド毎のカウンタ.
#[derive(Debug, Clone)]
pub struct DeviceCommandCounter {
    pub(crate) put: Counter,
    pub(crate) get: Counter,
    pub(crate) head: Counter,
    pub(crate) delete: Counter,
    pub(crate) delete_range: Counter,
    pub(crate) list: Counter,
    pub(crate) list_range: Counter,
    pub(crate) stop: Counter,
}
impl DeviceCommandCounter {
    /// PUTコマンド用のカウンタの値を返す.
    pub fn put(&self) -> u64 {
        self.put.value() as u64
    }

    /// GETコマンド用のカウンタの値を返す.
    pub fn get(&self) -> u64 {
        self.get.value() as u64
    }

    /// HEADコマンド用のカウンタの値を返す.
    pub fn head(&self) -> u64 {
        self.head.value() as u64
    }

    /// DELETEコマンド用のカウンタの値を返す.
    pub fn delete(&self) -> u64 {
        self.delete.value() as u64
    }

    /// DELETE_RANGEコマンド用のカウンタの値を返す.
    pub fn delete_range(&self) -> u64 {
        self.delete_range.value() as u64
    }

    /// LISTコマンド用のカウンタの値を返す.
    pub fn list(&self) -> u64 {
        self.list.value() as u64
    }

    /// LISTコマンド用のカウンタの値を返す.
    pub fn list_range(&self) -> u64 {
        self.list_range.value() as u64
    }

    /// STOPコマンド用のカウンタの値を返す.
    pub fn stop(&self) -> u64 {
        self.stop.value() as u64
    }

    pub(crate) fn new(builder: &MetricBuilder, name: &str, help: &str) -> Self {
        let counter = |command| {
            builder
                .counter(name)
                .help(help)
                .label("command", command)
                .finish()
                .expect("Never fails")
        };
        DeviceCommandCounter {
            put: counter("put"),
            get: counter("get"),
            head: counter("head"),
            delete: counter("delete"),
            delete_range: counter("delete_range"),
            list: counter("list"),
            list_range: counter("list_range"),
            stop: counter("stop"),
        }
    }

    pub(crate) fn increment(&self, command: &Command) {
        match *command {
            Command::Put { .. } => self.put.increment(),
            Command::Get { .. } => self.get.increment(),
            Command::Head { .. } => self.head.increment(),
            Command::Delete { .. } => self.delete.increment(),
            Command::DeleteRange { .. } => self.delete_range.increment(),
            Command::List { .. } => self.list.increment(),
            Command::ListRange { .. } => self.list_range.increment(),
            Command::Stop { .. } => self.stop.increment(),
        }
    }

    fn sum(&self) -> u64 {
        self.put() + self.get() + self.head() + self.delete() + self.list() + self.stop()
    }
}

/// [`Storage`]のメトリクス.
///
/// [`Storage`]: ../storage/struct.Storage.html
///
/// # Prometheus
///
/// `Methods`節に記載の無いメトリクスのみを掲載:
///
/// ```prometheus
/// cannyls_storage_header { version="<MAJOR>.<MINOR>", block_size="<BLOCK_SIZE>", uuid="<UUID>", journal_region_size="<BYTES>", data_region_size="<BYTES>" } 1
/// ```
#[derive(Debug, Clone)]
pub struct StorageMetrics {
    pub(crate) put_lumps_at_starting: Counter,
    pub(crate) put_lumps_at_running: Counter,
    pub(crate) delete_lumps: Counter,
    pub(crate) get_journal_lumps: Counter,
    pub(crate) get_data_lumps: Counter,
    header: Gauge,
    original_header: StorageHeader, // `header`からも復元できるが効率のためにこちらも保持しておく
    journal_region: JournalRegionMetrics,
    data_region: DataRegionMetrics,
}
impl StorageMetrics {
    /// ストレージに追加されたlumpの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_storage_put_lumps_total { phase="starting|running" } <COUNTER>
    /// ```
    pub fn put_lumps(&self) -> u64 {
        self.put_lumps_at_starting.value() as u64 + self.put_lumps_at_running.value() as u64
    }

    /// ストレージから削除されたlumpの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_storage_delete_lumps_total <COUNTER>
    /// ```
    pub fn delete_lumps(&self) -> u64 {
        self.delete_lumps.value() as u64
    }

    /// ジャーナル領域から取得したlumpの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_storage_get_lumps_total { region="journal" } <COUNTER>
    /// ```
    pub fn get_journal_lumps(&self) -> u64 {
        self.get_journal_lumps.value() as u64
    }

    /// データ領域から取得したlumpの数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_storage_get_lumps_total { region="data" } <COUNTER>
    /// ```
    pub fn get_data_lumps(&self) -> u64 {
        self.get_data_lumps.value() as u64
    }

    /// 現在のlump数.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_storage_put_lumps_total - cannyls_storage_delete_lumps_total
    /// ```
    pub fn lumps(&self) -> usize {
        // NOTE: 以下の順番で値を取得しないとアンダーフローする可能性がある
        let dec = self.delete_lumps();
        let inc = self.put_lumps();
        (inc - dec) as usize
    }

    /// ストレージのヘッダ情報.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_storage_header { version="{MAJOR}.{MINOR}", block_size="...", uuid="...", journal_region_size="...", data_region_size="..." } 1
    /// ```
    pub fn header(&self) -> &StorageHeader {
        &self.original_header
    }

    /// ジャーナル領域のメトリクスを返す.
    pub fn journal_region(&self) -> &JournalRegionMetrics {
        &self.journal_region
    }

    /// データ領域のメトリクスを返す.
    pub fn data_region(&self) -> &DataRegionMetrics {
        &self.data_region
    }

    pub(crate) fn new(
        builder: &MetricBuilder,
        header: &StorageHeader,
        journal_region: JournalRegionMetrics,
        data_region: DataRegionMetrics,
    ) -> Self {
        let mut builder = builder.clone();
        builder.namespace("cannyls").subsystem("storage");
        StorageMetrics {
            header: builder
                .gauge("header")
                .help("Header information of the storage")
                .label(
                    "version",
                    &format!("{}.{}", header.major_version, header.minor_version),
                )
                .label("block_size", &header.block_size.as_u16().to_string())
                .label("uuid", &header.instance_uuid.to_string())
                .label(
                    "journal_region_size",
                    &header.journal_region_size.to_string(),
                )
                .label("data_region_size", &header.data_region_size.to_string())
                .initial_value(1.0)
                .finish()
                .expect("Never fails"),
            put_lumps_at_starting: builder
                .counter("put_lumps_total")
                .help("Number of lumps putted on the storage")
                .label("phase", "starting")
                .finish()
                .expect("Never fails"),
            put_lumps_at_running: builder
                .counter("put_lumps_total")
                .help("Number of lumps putted on the storage")
                .label("phase", "running")
                .finish()
                .expect("Never fails"),
            delete_lumps: builder
                .counter("delete_lumps_total")
                .help("Number of lumps deleted from the storage")
                .finish()
                .expect("Never fails"),
            get_journal_lumps: builder
                .counter("get_lumps_total")
                .help("Number of lumps got from the storage")
                .label("region", "journal")
                .finish()
                .expect("Never fails"),
            get_data_lumps: builder
                .counter("get_lumps_total")
                .help("Number of lumps got from the storage")
                .label("region", "data")
                .finish()
                .expect("Never fails"),
            original_header: header.clone(),
            journal_region,
            data_region,
        }
    }
}

/// ストレージのデータ領域のメトリクス.
#[derive(Debug, Clone)]
pub struct DataRegionMetrics {
    pub(crate) capacity_bytes: Gauge,
    allocator: DataAllocatorMetrics,
}
impl DataRegionMetrics {
    /// データ領域の容量を返す.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_region_capacity_bytes <GAUGE>
    /// ```
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes.value() as u64
    }

    /// データ領域の使用量を返す.
    ///
    /// # Prometheus
    ///
    /// ```prometheus
    /// cannyls_data_allocator_allocated_bytes_total - cannyls_data_allocator_released_bytes_total
    /// ```
    pub fn usage_bytes(&self) -> u64 {
        // NOTE: 以下の順番で値を取得しないとアンダーフローする可能性がある
        let dec = self.allocator.released_bytes();
        let inc = self.allocator.allocated_bytes();
        inc - dec
    }

    /// アロケータのメトリクスを返す.
    pub fn allocator(&self) -> &DataAllocatorMetrics {
        &self.allocator
    }

    pub(crate) fn new(
        builder: &MetricBuilder,
        capacity: u64,
        allocator: DataAllocatorMetrics,
    ) -> Self {
        let mut builder = builder.clone();
        builder.namespace("cannyls").subsystem("data_region");
        DataRegionMetrics {
            capacity_bytes: builder
                .gauge("capacity_bytes")
                .help("Capacity of the data region")
                .initial_value(capacity as f64)
                .finish()
                .expect("Never fails"),
            allocator,
        }
    }
}
