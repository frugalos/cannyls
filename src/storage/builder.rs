use prometrics::metrics::MetricBuilder;
use std::io::SeekFrom;
use uuid::Uuid;

use crate::block::BlockSize;
use crate::metrics::{DataAllocatorMetrics, StorageMetrics};
use crate::nvm::NonVolatileMemory;
use crate::storage::allocator::DataPortionAllocator;
use crate::storage::data_region::DataRegion;
use crate::storage::header::FULL_HEADER_SIZE;
use crate::storage::index::LumpIndex;
use crate::storage::journal::{JournalRegion, JournalRegionOptions};
use crate::storage::{
    Storage, StorageHeader, MAJOR_VERSION, MAX_DATA_REGION_SIZE, MAX_JOURNAL_REGION_SIZE,
    MINOR_VERSION,
};
use crate::{ErrorKind, Result};

/// `Storage`のビルダ.
#[derive(Debug, Clone)]
pub struct StorageBuilder {
    journal_region_ratio: f64,
    instance_uuid: Option<Uuid>,
    journal: JournalRegionOptions,
    metrics: MetricBuilder,
}
impl StorageBuilder {
    /// 新しい`StorageBuilder`インスタンスを生成する.
    pub fn new() -> Self {
        StorageBuilder {
            journal_region_ratio: 0.01,
            instance_uuid: None,
            journal: JournalRegionOptions::default(),
            metrics: MetricBuilder::new(),
        }
    }

    /// ストレージインスタンスを識別するためのUUIDを設定する.
    ///
    /// ストレージの作成時とオープン時で、指定した値の使われ方が異なる:
    ///
    /// - 作成時:
    ///   - ここで指定した値が識別子として採用される
    ///   - 本メソッドが呼ばれていない場合は、ランダムなUUIDが割り当てられる
    /// - オープン時:
    ///   - ここで指定した値と、ストレージの識別子が比較され、もし異なっている場合にはオープンに失敗する
    ///   - 本メソッドが呼ばれていない場合は、特にチェックは行われない
    pub fn instance_uuid(&mut self, uuid: Uuid) -> &mut Self {
        self.instance_uuid = Some(uuid);
        self
    }

    /// 利用可能な領域全体に占めるジャーナル領域の割合を設定する.
    ///
    /// 取り得る値は、0.0から1.0の間の小数である。
    /// この範囲外の値が指定された場合には、ストレージの構築時(e.g., `create()`呼び出し時)にエラーが返される.
    /// デフォルト値は、`0.01`.
    ///
    /// なお、これはストレージの新規作成時にのみ反映される値であり、
    /// 既存のストレージを開く場合には、作成時に指定された値が使用される.
    pub fn journal_region_ratio(&mut self, ratio: f64) -> &mut Self {
        self.journal_region_ratio = ratio;
        self
    }

    /// ジャーナル領域用のリングバッファのGCキューの長さ、を設定する.
    ///
    /// この値は、一回のGCで対象となるレコードの数、と等しい.
    ///
    /// デフォルト値は`4096`.
    ///
    /// # 参考
    ///
    /// - [ストレージのジャーナル領域のGC方法][gc]
    ///
    /// [gc]: https://github.com/frugalos/cannyls/wiki/Journal-Region-GC
    pub fn journal_gc_queue_size(&mut self, size: usize) -> &mut Self {
        self.journal.gc_queue_size = size;
        self
    }

    /// 物理デバイスへのジャーナルの同期間隔、を設定する.
    ///
    /// この値で指定された数のレコードがジャーナルに追加される度に、
    /// メモリ上のバッファが書き戻された上で、同期命令(e.g., `fdatasync`)が発行される.
    /// つまり、この間隔が長いほど書き込み時の性能は向上するが、信頼性は下がることになる.
    ///
    /// デフォルト値は`4096`.
    pub fn journal_sync_interval(&mut self, interval: usize) -> &mut Self {
        self.journal.sync_interval = interval;
        self
    }

    /// ストレージのブロックサイズを指定する.
    ///
    /// ここで指定した値は、ストレージの生成時にのみ使われる.
    /// (オープン時には、ヘッダに格納されている既存の値が使用される)
    ///
    /// デフォルト値は`BlockSize::min()`.
    ///
    /// # 注意
    ///
    /// ストレージのブロックサイズには、それが使用するNVMのブロック境界に揃った値を指定する必要がある.
    /// もしそうではない値が指定された場合には、ストレージの生成処理がエラーとなる.
    pub fn block_size(&mut self, block_size: BlockSize) -> &mut Self {
        self.journal.block_size = block_size;
        self
    }

    /// メトリクス用の共通設定を登録する.
    ///
    /// デフォルト値は`MetricBuilder::new()`.
    pub fn metrics(&mut self, metrics: MetricBuilder) -> &mut Self {
        self.metrics = metrics;
        self
    }

    /// 新規にストレージを生成する.
    pub fn create<N>(&self, mut nvm: N) -> Result<Storage<N>>
    where
        N: NonVolatileMemory,
    {
        let storage_block_size = self.journal.block_size;

        // NVMのブロック境界に揃っているかを確認
        track_assert!(
            storage_block_size.contains(nvm.block_size()),
            ErrorKind::InvalidInput; storage_block_size, nvm.block_size()
        );

        let header = track!(self.make_header(nvm.capacity(), storage_block_size))?;

        track_io!(nvm.seek(SeekFrom::Start(0)))?;
        track!(nvm.aligned_write_all(|mut temp_buf| {
            // ストレージのヘッダを書き込む
            track!(header.write_header_region_to(&mut temp_buf))?;

            // ジャーナル領域を初期化する
            track!(JournalRegion::<N>::initialize(temp_buf, storage_block_size))?;

            Ok(())
        }))?;
        track!(nvm.sync())?;

        track!(self.open(nvm))
    }

    /// 既に存在するストレージをオープンする.
    pub fn open<N>(&self, mut nvm: N) -> Result<Storage<N>>
    where
        N: NonVolatileMemory,
    {
        track_io!(nvm.seek(SeekFrom::Start(0)))?;

        // ヘッダを読み込む(アライメントを保証するためにバッファを経由)
        let buf = track!(nvm.aligned_read_bytes(FULL_HEADER_SIZE as usize))?;
        let mut header = track!(StorageHeader::read_from(&buf[..]))?;

        // ストレージのマイナーバージョンが古い場合には、最新に更新する
        if header.minor_version < MINOR_VERSION {
            header.minor_version = MINOR_VERSION;

            track_io!(nvm.seek(SeekFrom::Start(0)))?;
            track!(nvm.aligned_write_all(|temp_buf| {
                track!(header.write_header_region_to(temp_buf))?;
                Ok(())
            }))?;
        }

        // `nvm`がストレージが採用しているブロックサイズに対応可能かを確認
        //
        // ヘッダに記載のストレージのブロックサイズが、NVMのブロック境界に揃っている場合には、
        // 完全一致ではなくても許容する
        track_assert!(
            header.block_size.contains(nvm.block_size()),
            ErrorKind::InvalidInput
        );
        let mut journal_options = self.journal.clone();
        journal_options.block_size = header.block_size;

        // UUIDをチェック
        if let Some(expected_uuid) = self.instance_uuid {
            track_assert_eq!(header.instance_uuid, expected_uuid, ErrorKind::InvalidInput);
        }

        // ジャーナルからインデックスとアロケータの状態を復元する
        let mut lump_index = LumpIndex::new();
        let (journal_nvm, data_nvm) = track!(header.split_regions(nvm))?;
        let journal_region = track!(JournalRegion::open(
            journal_nvm,
            &mut lump_index,
            &self.metrics,
            journal_options
        ))?;

        let allocator = track!(DataPortionAllocator::build(
            DataAllocatorMetrics::new(&self.metrics, header.data_region_size, header.block_size),
            lump_index.data_portions(),
        ))?;

        // データ領域を準備
        let data_region = DataRegion::new(&self.metrics, allocator, data_nvm);

        let metrics = StorageMetrics::new(
            &self.metrics,
            &header,
            journal_region.metrics().clone(),
            data_region.metrics().clone(),
        );
        metrics.put_lumps_at_starting.add_u64(lump_index.len());
        Ok(Storage::new(
            header,
            journal_region,
            data_region,
            lump_index,
            metrics,
        ))
    }

    fn make_header(&self, capacity: u64, block_size: BlockSize) -> Result<StorageHeader> {
        let journal_and_data_region_size = track_assert_some!(
            capacity.checked_sub(StorageHeader::calc_region_size(block_size)),
            ErrorKind::InvalidInput,
            "Too small capacity: {}",
            capacity
        );

        track_assert!(
            0.0 <= self.journal_region_ratio && self.journal_region_ratio <= 1.0,
            ErrorKind::InvalidInput,
            "Invalid journal region ratio: {}",
            self.journal_region_ratio
        );
        let journal_region_size =
            (journal_and_data_region_size as f64 * self.journal_region_ratio) as u64;
        let journal_region_size = block_size.ceil_align(journal_region_size);
        track_assert!(
            journal_region_size <= MAX_JOURNAL_REGION_SIZE,
            ErrorKind::InvalidInput,
            "Too large journal region: {} (capacity={}, ratio={})",
            journal_region_size,
            capacity,
            self.journal_region_ratio
        );

        let data_region_size =
            block_size.floor_align(journal_and_data_region_size - journal_region_size);
        track_assert!(
            data_region_size <= MAX_DATA_REGION_SIZE,
            ErrorKind::InvalidInput,
            "Too large data region: {} (capacity={}, journal_region_ratio={})",
            data_region_size,
            capacity,
            self.journal_region_ratio
        );

        Ok(StorageHeader {
            major_version: MAJOR_VERSION,
            minor_version: MINOR_VERSION,
            instance_uuid: self.instance_uuid.unwrap_or_else(Uuid::new_v4),
            block_size,
            journal_region_size,
            data_region_size,
        })
    }
}
impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}
