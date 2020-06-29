//! Lump用のストレージ.
//!
//! このモジュール自体は、具体的なI/O処理(e.g., ファイル処理)とは切り離されており、データ構造の実装に近い.
//!
//! 利用の際には、使用する[NonVolatileMemory]実装を指定した上で、[Device]経由で動作させる必要がある.
//!
//! # 参考
//!
//! - [ストレージフォーマット(v1.0)][format]
//! - [ストレージのジャーナル領域のGC方法][gc]
//!
//! [NonVolatileMemory]: ../nvm/trait.NonVolatileMemory.html
//! [Device]: ../device/struct.Device.html
//! [format]: https://github.com/frugalos/cannyls/wiki/Storage-Format
//! [gc]: https://github.com/frugalos/cannyls/wiki/Journal-Region-GC
pub use self::address::Address;
pub use self::builder::StorageBuilder;
pub use self::header::StorageHeader;
pub use self::journal::{JournalEntry, JournalRecord, JournalSnapshot};

pub(crate) use self::data_region::DataRegionLumpData; // `lump`モジュール用に公開

use self::data_region::DataRegion;
use self::index::LumpIndex;
use self::journal::JournalRegion;
use self::portion::Portion;
use block::BlockSize;
use lump::{LumpData, LumpDataInner, LumpHeader, LumpId};
use metrics::StorageMetrics;
use nvm::NonVolatileMemory;
use std::ops::Range;
use Result;

mod address;
mod allocator;
mod builder;
mod data_region;
mod header;
mod index;
mod journal;
mod portion;

/// ストレージの先頭に書き込まれるマジックナンバー.
///
/// "**LU**mp **S**torage **F**ormat"の略.
pub const MAGIC_NUMBER: [u8; 4] = *b"lusf";

/// ストレージフォーマットの現在のメジャーバージョン.
///
/// メジャーバージョンが異なるストレージ同士のデータ形式には互換性が無い.
pub const MAJOR_VERSION: u16 = 1;

/// ストレージフォーマットの現在のマイナーバージョン.
///
/// マイナーバージョンには、後方互換性がある.
pub const MINOR_VERSION: u16 = 1;

/// ジャーナル領域の最大サイズ(バイト単位).
///
/// およそ1TB.
pub const MAX_JOURNAL_REGION_SIZE: u64 = Address::MAX;

/// データ領域の最大サイズ(バイト単位).
///
/// およそ512TB.
pub const MAX_DATA_REGION_SIZE: u64 = Address::MAX * BlockSize::MIN as u64;

/// Lumpを格納するためのストレージ.
///
/// 基本的には、`Storage`インスタンスの構築後は[Device]経由で操作することが想定されている.
///
/// ストレージのフォーマットに関しては[ストレージフォーマット(v1.0)][format]を参照のこと.
///
/// [Device]: ../device/struct.Device.html
/// [format]: https://github.com/frugalos/cannyls/wiki/Storage-Format
#[derive(Debug)]
pub struct Storage<N>
where
    N: NonVolatileMemory,
{
    header: StorageHeader,
    journal_region: JournalRegion<N>,
    data_region: DataRegion<N>,
    lump_index: LumpIndex,
    metrics: StorageMetrics,
}
impl<N> Storage<N>
where
    N: NonVolatileMemory,
{
    pub(crate) fn new(
        header: StorageHeader,
        journal_region: JournalRegion<N>,
        data_region: DataRegion<N>,
        lump_index: LumpIndex,
        metrics: StorageMetrics,
    ) -> Self {
        Storage {
            header,
            journal_region,
            data_region,
            lump_index,
            metrics,
        }
    }

    /// デフォルト設定で、新規にストレージを生成する.
    pub fn create(nvm: N) -> Result<Self> {
        track!(StorageBuilder::new().create(nvm))
    }

    /// デフォルト設定で、既に存在するストレージをオープンする.
    pub fn open(nvm: N) -> Result<Self> {
        track!(StorageBuilder::new().open(nvm))
    }

    /// ストレージのヘッダ情報を返す.
    pub fn header(&self) -> &StorageHeader {
        &self.header
    }

    /// ストレージのメトリクスを返す.
    pub fn metrics(&self) -> &StorageMetrics {
        &self.metrics
    }

    /// ストレージに保存されている中で、指定された範囲が占有するバイト数を返す.
    pub fn usage_range(&self, range: Range<LumpId>) -> StorageUsage {
        self.lump_index.usage_range(range, self.header.block_size)
    }

    /// 指定されたIDのlumpを取得する.
    ///
    /// # Error Handlings
    ///
    /// このメソッドがエラーを返した場合には、
    /// 不整合ないしI/O周りで致命的な問題が発生している可能性があるので、
    /// 以後はこのインスタンスの使用を中止するのが望ましい
    /// (更新系操作とは異なり、何度かリトライを試みても問題はない).
    pub fn get(&mut self, lump_id: &LumpId) -> Result<Option<LumpData>> {
        match self.lump_index.get(lump_id) {
            None => Ok(None),
            Some(portion) => {
                let data = match portion {
                    Portion::Journal(portion) => {
                        self.metrics.get_journal_lumps.increment();
                        let bytes = track!(self.journal_region.get_embedded_data(portion))?;
                        track!(LumpData::new_embedded(bytes))?
                    }
                    Portion::Data(portion) => {
                        self.metrics.get_data_lumps.increment();
                        track!(self.data_region.get(portion).map(LumpData::from))?
                    }
                };
                Ok(Some(data))
            }
        }
    }

    /// 指定されたIDのlumpのヘッダ情報を取得する.
    pub fn head(&self, lump_id: &LumpId) -> Option<LumpHeader> {
        self.lump_index.get(lump_id).map(|portion| LumpHeader {
            approximate_data_size: portion.len(self.header.block_size),
        })
    }

    /// 保存されているlumpのID一覧を返す.
    ///
    /// 結果は昇順にソートされている.
    ///
    /// # 注意
    ///
    /// 例えば巨大なHDDを使用している場合には、lumpの数が数百万以上になることもあるため、
    /// このメソッドは呼び出す際には注意が必要.
    pub fn list(&self) -> Vec<LumpId> {
        self.lump_index.list()
    }

    /// ストレージに保存されている中で、指定された範囲に含まれるLumpIdの一覧を返す.
    pub fn list_range(&mut self, range: Range<LumpId>) -> Vec<LumpId> {
        self.lump_index.list_range(range)
    }

    /// lumpを保存する.
    ///
    /// 既に同じIDのlumpが存在する場合にはデータが上書きされる.
    ///
    /// 新規追加の場合には`Ok(true)`が、上書きの場合には`Ok(false)`が返される.
    ///
    /// # Error Handlings
    ///
    /// このメソッドが`ErrorKind::{Full, InvalidInput}`以外のエラーを返した場合には、
    /// 不整合ないしI/O周りで致命的な問題が発生している可能性があるので、
    /// 以後はこのインスタンスの使用を中止するのが望ましい.
    ///
    /// # 性能上の注意
    ///
    /// 引数に渡される`LumpData`が、`LumpData::new`関数経由で生成されている場合には、
    /// NVMへの書き込み前に、データをブロック境界にアライメントするためのメモリコピーが余分に発生してしまう.
    /// それを避けたい場合には、`Storage::allocate_lump_data`メソッドを使用して`LumpData`を生成すると良い.
    pub fn put(&mut self, lump_id: &LumpId, data: &LumpData) -> Result<bool> {
        let updated = track!(self.delete_if_exists(lump_id, false))?;
        match data.as_inner() {
            LumpDataInner::JournalRegion(data) => {
                track!(self
                    .journal_region
                    .records_embed(&mut self.lump_index, lump_id, data))?;
            }
            LumpDataInner::DataRegion(data) => {
                track!(self.put_lump_to_data_region(lump_id, data))?;
            }
            LumpDataInner::DataRegionUnaligned(data) => {
                let mut aligned_data = DataRegionLumpData::new(data.len(), self.header.block_size);
                aligned_data.as_bytes_mut().copy_from_slice(data);
                track!(self.put_lump_to_data_region(lump_id, &aligned_data))?;
            }
        }
        self.metrics.put_lumps_at_running.increment();
        Ok(!updated)
    }

    /// 指定されたIDのlumpを削除する.
    ///
    /// 削除が行われた場合には`Ok(true)`が、存在しないlumpが指定された場合には`Ok(false)`が、返される.
    ///
    /// # Error Handlings
    ///
    /// このメソッドがエラーを返した場合には、
    /// 不整合ないしI/O周りで致命的な問題が発生している可能性があるので、
    /// 以後はこのインスタンスの使用を中止するのが望ましい.
    pub fn delete(&mut self, lump_id: &LumpId) -> Result<bool> {
        track!(self.delete_if_exists(lump_id, true))
    }

    /// LumpIdのrange [start..end) を用いて、これに含まれるLumpIdを全て削除する。
    ///
    /// 返り値がOk(vec)の場合、このvecは実際に削除したlump id全体となっている。
    /// （注意: rangeには、lusf上にないlump idが一般には含まれている）
    ///
    /// # Error Handlings
    ///
    /// このメソッドがエラーを返した場合には、
    /// 不整合ないしI/O周りで致命的な問題が発生している可能性があるので、
    /// 以後はこのインスタンスの使用を中止するのが望ましい.
    ///
    /// # 注意
    ///
    /// `range`が大量の要素を含む場合には、
    /// このメソッドは巨大なLumpIdの配列を返しうることに注意されたい。
    pub fn delete_range(&mut self, range: Range<LumpId>) -> Result<Vec<LumpId>> {
        let targets = self.lump_index.list_range(range.clone());

        // ジャーナル領域に範囲削除レコードを一つ書き込むため、一度のディスクアクセスが起こる。
        // 削除レコードを範囲分書き込むわけ *ではない* ため、複数回のディスクアクセスは発生しない。
        track!(self
            .journal_region
            .records_delete_range(&mut self.lump_index, range))?;

        for lump_id in &targets {
            if let Some(portion) = self.lump_index.remove(lump_id) {
                self.metrics.delete_lumps.increment();

                if let Portion::Data(portion) = portion {
                    // DataRegion::deleteはメモリアロケータに対する解放要求をするのみで
                    // ディスクにアクセスすることはない。
                    // （管理領域から外すだけで、例えばディスク上の値を0クリアするようなことはない）
                    self.data_region.delete(portion);
                }
            }
        }

        Ok(targets)
    }

    /// ストレージのブロック境界にアライメントされたメモリ領域を保持する`LumpData`インスタンスを返す.
    ///
    /// `LumpData::new`関数に比べて、このメソッドが返した`LumpData`インスタンスは、
    /// 事前に適切なアライメントが行われているため、`Storage::put`による保存時に余計なメモリコピーが
    /// 発生することがなく、より効率的となる.
    ///
    /// # 注意
    ///
    /// このストレージが返した`LumpData`インスタンスを、別の(ブロックサイズが異なる)ストレージに
    /// 保存しようとした場合には、エラーが発生する.
    ///
    /// # Errors
    ///
    /// 指定されたサイズが`MAX_SIZE`を超えている場合は、`ErrorKind::InvalidInput`エラーが返される.
    pub fn allocate_lump_data(&self, size: usize) -> Result<LumpData> {
        track!(LumpData::aligned_allocate(size, self.header.block_size))
    }

    /// `allocate_lump_data`メソッドにデータの初期化を加えたメソッド.
    ///
    /// このメソッドの呼び出しは、以下のコードと等価となる:
    /// ```ignore
    /// let mut data = track!(self.allocate_lump_data(bytes.len()))?;
    /// data.as_bytes_mut().copy_from_slice(bytes);
    /// ```
    ///
    /// 詳細な挙動に関しては`allocate_lump_data`のドキュメントを参照のこと.
    pub fn allocate_lump_data_with_bytes(&self, bytes: &[u8]) -> Result<LumpData> {
        let mut data = track!(self.allocate_lump_data(bytes.len()))?;
        data.as_bytes_mut().copy_from_slice(bytes);
        Ok(data)
    }

    /// 補助的な処理を一単位実行する.
    ///
    /// このメソッドを呼ばなくても動作上は問題はないが、
    /// リソースが空いているタイミングで実行することによって、
    /// 全体的な性能を改善できる可能性がある.
    pub fn run_side_job_once(&mut self) -> Result<()> {
        track!(self.journal_region.run_side_job_once(&mut self.lump_index))?;
        Ok(())
    }

    /// メモリにバッファされているジャーナルをディスクに書き出す。
    /// 副作用として、バッファはクリアされる。
    pub fn journal_sync(&mut self) -> Result<()> {
        self.journal_region.sync()
    }

    /// ジャーナル領域に対するGCを実行する。
    ///
    /// ここで実行するGCは、ジャーナル領域のHEADからTAILの間の値を全て検査し、
    /// `journal_gc` を呼び出した段階で破棄できる全エントリを削除する。
    ///
    /// 通常のstorageの使用では、各エントリをジャーナルに追加する際に、
    /// 小規模のGCが走る（正確には `JournalRegion::gc_once`）ので、
    /// このGCを手動で呼び出す必要はない。
    pub fn journal_gc(&mut self) -> Result<()> {
        self.journal_region.gc_all_entries(&mut self.lump_index)
    }

    /// ジャーナル領域のスナップショットを取得する。
    pub fn journal_snapshot(&mut self) -> Result<JournalSnapshot> {
        let (unreleased_head, head, tail, entries) = track!(self.journal_region.journal_entries())?;
        Ok(JournalSnapshot {
            unreleased_head,
            head,
            tail,
            entries,
        })
    }

    /// ジャーナル領域に対する自動小規模GCの有無を切り替えることができる（ユニットテスト用メソッド）。
    ///
    /// デフォルトの設定では、ジャーナル領域への変更操作が行われた際に、
    /// 1-stepのGC(`JournalRegion::gc_once`)が実行されるが、
    /// `set_automatic_gc_mode(false)`を呼び出すことで
    /// この小規模のGCを実行しないようにできる。
    #[allow(dead_code)]
    pub(crate) fn set_automatic_gc_mode(&mut self, enable: bool) {
        self.journal_region.set_automatic_gc_mode(enable);
    }

    fn put_lump_to_data_region(
        &mut self,
        lump_id: &LumpId,
        data: &DataRegionLumpData,
    ) -> Result<()> {
        let portion = track!(self.data_region.put(data))?;
        track!(self
            .journal_region
            .records_put(&mut self.lump_index, lump_id, portion)
            .map_err(|e| {
                self.data_region.delete(portion);
                e
            }))?;
        self.lump_index.insert(*lump_id, Portion::Data(portion));
        Ok(())
    }

    fn delete_if_exists(&mut self, lump_id: &LumpId, do_record: bool) -> Result<bool> {
        if let Some(portion) = self.lump_index.remove(lump_id) {
            self.metrics.delete_lumps.increment();
            if do_record {
                track!(self
                    .journal_region
                    .records_delete(&mut self.lump_index, lump_id,))?;
            }
            if let Portion::Data(portion) = portion {
                self.data_region.delete(portion);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// ストレージ使用量。
#[derive(Debug, Clone)]
pub enum StorageUsage {
    /// 取得に失敗したなど不明であることを表す。
    Unknown,
    /// 近似値。
    Approximate(u64),
}
impl StorageUsage {
    /// 近似値として `StorageUsage` を生成する。
    pub fn approximate(usage: u64) -> Self {
        StorageUsage::Approximate(usage)
    }

    /// 使用量不明として `StorageUsage` を生成する。
    pub fn unknown() -> Self {
        StorageUsage::Unknown
    }

    /// バイト数として近似値を返す。
    pub fn bytecount(&self) -> Option<u64> {
        match *self {
            StorageUsage::Unknown => None,
            StorageUsage::Approximate(bytes) => Some(bytes),
        }
    }
}
impl Default for StorageUsage {
    fn default() -> Self {
        StorageUsage::Unknown
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::mem;
    use tempdir::TempDir;
    use trackable::result::TestResult;

    use super::*;
    use block::BlockSize;
    use lump::{LumpData, LumpId};
    use nvm::{FileNvm, SharedMemoryNvm};
    use ErrorKind;

    #[test]
    fn it_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;

        // create
        let nvm = track!(FileNvm::create(
            dir.path().join("test.lusf"),
            BlockSize::min().ceil_align(1024 * 1024)
        ))?;
        let mut storage = track!(Storage::create(nvm))?;

        assert!(storage.get(&id("000"))?.is_none());
        assert!(storage.put(&id("000"), &data("hello"))?);
        assert!(!storage.put(&id("000"), &data("hello"))?);
        assert_eq!(storage.get(&id("000"))?, Some(data("hello")));
        assert_eq!(
            storage.head(&id("000")).map(|h| h.approximate_data_size),
            Some(5)
        );
        assert!(storage.delete(&id("000"))?);
        assert!(!storage.delete(&id("000"))?);
        assert!(storage.get(&id("000"))?.is_none());
        assert!(storage.head(&id("000")).is_none());

        assert!(storage.put(&id("000"), &data("hello"))?);
        assert!(storage.put(&id("111"), &data("world"))?);
        for _ in 0..10 {
            track!(storage.run_side_job_once())?;
            assert!(storage.put(&id("222"), &data("quux"))?);
            assert!(storage.delete(&id("222"))?);
        }
        mem::drop(storage);

        // open
        let nvm = track!(FileNvm::open(dir.path().join("test.lusf")))?;
        let storage = track!(Storage::open(nvm))?;
        assert_eq!(storage.list(), vec![id("000"), id("111")]);
        Ok(())
    }

    #[test]
    fn full() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;

        let nvm = track!(FileNvm::create(
            dir.path().join("test.lusf"),
            BlockSize::min().ceil_align(1024 * 1024)
        ))?;
        let mut storage = track!(Storage::create(nvm))?;

        assert_eq!(
            track!(storage.put(&id("000"), &zeroed_data(512 * 1024)))?,
            true
        );
        assert_eq!(
            storage.put(&id("000"), &zeroed_data(512 * 1024)).ok(),
            Some(false)
        );
        assert_eq!(
            storage
                .put(&id("111"), &zeroed_data(512 * 1024))
                .err()
                .map(|e| *e.kind()),
            Some(ErrorKind::StorageFull)
        );

        assert_eq!(storage.delete(&id("000")).ok(), Some(true));
        assert_eq!(
            storage.put(&id("111"), &zeroed_data(512 * 1024)).ok(),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn max_size_lump() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;

        let nvm = track!(FileNvm::create(
            dir.path().join("test.lusf"),
            BlockSize::min().ceil_align(100 * 1024 * 1024)
        ))?;
        let mut storage = track!(Storage::create(nvm))?;

        let data = zeroed_data(LumpData::MAX_SIZE);
        assert_eq!(track!(storage.put(&id("000"), &data))?, true);
        assert_eq!(track!(storage.get(&id("000")))?, Some(data));
        Ok(())
    }

    fn id(id: &str) -> LumpId {
        id.parse().unwrap()
    }

    fn data(data: &str) -> LumpData {
        LumpData::new_embedded(Vec::from(data)).unwrap()
    }

    fn zeroed_data(size: usize) -> LumpData {
        let mut data = LumpData::aligned_allocate(size, BlockSize::min()).unwrap();
        for v in data.as_bytes_mut() {
            *v = 0;
        }
        data
    }

    #[test]
    fn open_older_compatible_version_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let path = dir.path().join("test.lusf");

        // create
        let mut header = {
            let nvm = track!(FileNvm::create(&path, 1024 * 1024))?;
            let storage = track!(Storage::create(nvm))?;
            let header = storage.header().clone();
            assert_eq!(header.major_version, MAJOR_VERSION);
            assert_eq!(header.minor_version, MINOR_VERSION);
            header
        };

        // マイナーバージョンを減らして、ヘッダを上書きする
        {
            header.minor_version = header
                .minor_version
                .checked_sub(1)
                .expect("このテストは`MINOR_VERSION >= 1`であることを前提としている");
            let file = track_any_err!(OpenOptions::new().write(true).open(&path))?;
            track!(header.write_to(file))?;
        }

        // open: マイナーバージョンが最新のものに調整されている
        {
            let nvm = track!(FileNvm::open(&path))?;
            let storage = track!(Storage::open(nvm))?;
            let header = storage.header().clone();
            assert_eq!(header.major_version, MAJOR_VERSION);
            assert_eq!(header.minor_version, MINOR_VERSION);
        }

        // ファイル上のヘッダも更新されている
        {
            let file = track_any_err!(OpenOptions::new().read(true).open(&path))?;
            let header = track!(StorageHeader::read_from(file))?;
            assert_eq!(header.major_version, MAJOR_VERSION);
            assert_eq!(header.minor_version, MINOR_VERSION);
        }
        Ok(())
    }

    #[test]
    fn block_size_check_when_create() -> TestResult {
        // [OK] ストレージとNVMのブロックサイズが等しい
        let nvm_block_size = track!(BlockSize::new(1024))?;
        let storage_block_size = track!(BlockSize::new(1024))?;

        let storage = track!(StorageBuilder::new()
            .block_size(storage_block_size)
            .create(memory_nvm(nvm_block_size)))?;
        assert_eq!(storage.header().block_size, storage_block_size);

        // [OK] ストレージがNVMのブロックサイズを包含する
        let nvm_block_size = track!(BlockSize::new(512))?;
        let storage_block_size = track!(BlockSize::new(1024))?;

        let storage = track!(StorageBuilder::new()
            .block_size(storage_block_size)
            .create(memory_nvm(nvm_block_size)))?;
        assert_eq!(storage.header().block_size, storage_block_size);

        // [NG] NVMのブロックサイズが、ストレージのブロックサイズよりも大きい
        let nvm_block_size = track!(BlockSize::new(1024))?;
        let storage_block_size = track!(BlockSize::new(512))?;

        assert!(StorageBuilder::new()
            .block_size(storage_block_size)
            .create(memory_nvm(nvm_block_size))
            .is_err());

        // [NG] ストレージのブロック境界が、NVMのブロック境界に揃っていない
        let nvm_block_size = track!(BlockSize::new(1024))?;
        let storage_block_size = track!(BlockSize::new(1536))?;

        assert!(StorageBuilder::new()
            .block_size(storage_block_size)
            .create(memory_nvm(nvm_block_size))
            .is_err());

        Ok(())
    }

    #[test]
    fn block_size_check_when_open() -> TestResult {
        // 事前準備: ストレージとNVMのブロックサイズを等しくして、ストレージの初期化(生成)を実施
        let initial_nvm_block_size = track!(BlockSize::new(1536))?;
        let storage_block_size = track!(BlockSize::new(1536))?;
        let mut nvm = memory_nvm(initial_nvm_block_size);
        assert!(StorageBuilder::new()
            .block_size(storage_block_size)
            .create(nvm.clone())
            .is_ok());

        // [OK]: NVMとストレージのブロックサイズが等しい
        let storage = track!(Storage::open(nvm.clone()))?;
        assert_eq!(storage.header().block_size, storage_block_size);

        // [OK] ストレージのブロック境界がNVMのブロック境界に揃っている
        nvm.set_block_size(track!(BlockSize::new(512))?);
        let storage = track!(Storage::open(nvm.clone()))?;
        assert_eq!(storage.header().block_size, storage_block_size);

        // [NG] NVMのブロックサイズが、ストレージのブロックサイズよりも大きい
        nvm.set_block_size(track!(BlockSize::new(2048))?);
        assert!(Storage::open(nvm.clone()).is_err());

        // [NG] ストレージのブロック境界が、NVMのブロック境界に揃っていない
        nvm.set_block_size(track!(BlockSize::new(1024))?);
        assert!(Storage::open(nvm.clone()).is_err());

        Ok(())
    }

    fn memory_nvm(block_size: BlockSize) -> SharedMemoryNvm {
        SharedMemoryNvm::with_block_size(vec![0; 1024 * 1024], block_size)
    }

    fn is_put_with(entry: &JournalEntry, id: &LumpId) -> bool {
        if let JournalRecord::Put(id_, _) = entry.record {
            id_ == *id
        } else {
            false
        }
    }

    fn is_delete_with(entry: &JournalEntry, id: &LumpId) -> bool {
        if let JournalRecord::Delete(id_) = entry.record {
            id_ == *id
        } else {
            false
        }
    }

    #[test]
    fn full_gc_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;

        let nvm = track!(FileNvm::create(
            dir.path().join("test.lusf"),
            BlockSize::min().ceil_align(1024 * 1024)
        ))?;
        let mut storage = track!(Storage::create(nvm))?;

        // ストレージへの操作で、小規模GCが自動で発生しないようにする
        storage.set_automatic_gc_mode(false);

        assert!(storage.put(&id("000"), &zeroed_data(42))?);
        assert!(storage.put(&id("010"), &zeroed_data(42))?);

        let entries = storage.journal_snapshot().unwrap().entries;

        assert_eq!(entries.len(), 2);
        assert!(is_put_with(entries.get(0).unwrap(), &id("000")));
        assert!(is_put_with(entries.get(1).unwrap(), &id("010")));

        storage.journal_gc().unwrap();

        let new_entries = storage.journal_snapshot().unwrap().entries;

        for (e1, e2) in entries.iter().zip(new_entries.iter()) {
            assert_eq!(e1.record, e2.record);
            // 注意
            // GCによりジャーナル領域内でのエントリ移動が生じているため、次は成立しない
            // assert_eq!(e1.start, e2.start);
        }

        assert!(storage.delete(&id("000"))?);
        assert!(storage.delete(&id("010"))?);

        let entries = storage.journal_snapshot().unwrap().entries;

        assert_eq!(entries.len(), 4);

        assert!(is_put_with(entries.get(0).unwrap(), &id("000")));
        assert!(is_put_with(entries.get(1).unwrap(), &id("010")));
        assert!(is_delete_with(entries.get(2).unwrap(), &id("000")));
        assert!(is_delete_with(entries.get(3).unwrap(), &id("010")));

        storage.journal_gc().unwrap();

        let entries = storage.journal_snapshot().unwrap().entries;

        assert_eq!(entries.len(), 0);

        Ok(())
    }

    #[test]
    fn journal_overflow_example() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;

        let nvm = track!(FileNvm::create(
            dir.path().join("test.lusf"),
            BlockSize::min().ceil_align(1024 * 400)
        ))?;
        let mut storage = track!(StorageBuilder::new().journal_region_ratio(0.01).create(nvm))?;
        storage.set_automatic_gc_mode(false);

        {
            let header = storage.header();
            assert_eq!(header.journal_region_size, 4096);
        }

        for i in 0..60 {
            assert!(storage.put(&id(&i.to_string()), &zeroed_data(42))?);
        }
        for i in 0..20 {
            assert!(storage.delete(&id(&i.to_string()))?);
        }
        {
            let snapshot = track!(storage.journal_snapshot())?;
            assert_eq!(snapshot.unreleased_head, 0);
            assert_eq!(snapshot.head, 0);
            assert_eq!(snapshot.tail, 2100);
        }

        track!(storage.journal_gc())?;
        {
            let snapshot = track!(storage.journal_snapshot())?;
            assert_eq!(snapshot.unreleased_head, 2100);
            assert_eq!(snapshot.head, 2100);
            assert_eq!(snapshot.tail, 3220);
        }

        track!(storage.journal_gc())?;
        {
            let snapshot = track!(storage.journal_snapshot())?;
            assert_eq!(snapshot.unreleased_head, 3220);
            assert_eq!(snapshot.head, 3220);
            assert_eq!(snapshot.tail, 784);
        }

        Ok(())
    }

    #[test]
    /*
     * cannyls 0.9.2以前では
     * PR23 https://github.com/frugalos/cannyls/pull/23
     * が指摘する問題によりpanicしていた。
     * その問題が発生しないことを確認するためのテスト。
     * （発生していた問題というのは、
     * ジャーナルヘッドに永続化されている`head_position`の値と
     * これに対応するメモリ上のフィールド`unreleased_head`の値にズレが生じることに起因する。
     * 詳細についてはPR23を参考にされたい。）
     */
    fn confirm_that_the_problem_of_pr23_is_resolved() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;

        let nvm = track!(FileNvm::create(
            dir.path().join("test.lusf"),
            BlockSize::min().ceil_align(1024 * 100 * 4)
        ))?;
        let mut storage = track!(StorageBuilder::new().journal_region_ratio(0.01).create(nvm))?;
        assert_eq!(storage.header().journal_region_size, 4096);
        // putやdeleteなどに伴う自動GCをoffにする（コードと説明の簡単さのためでonのままでも再現できる）。
        storage.set_automatic_gc_mode(false);

        let test_lump_id = id("55");

        /*
         * 下のjournalの状態 (A)
         * unreleased_head == 33, head == 33, tail == 66
         * を目指す準備。
         */
        let vec: Vec<u8> = vec![42; 10];
        let lump_data = track!(LumpData::new_embedded(vec))?;
        track!(storage.put(&test_lump_id, &lump_data))?;
        track!(storage.run_side_job_once())?; // GCキューを充填。
        track!(storage.run_side_job_once())?; // syncを行う（今回は意味がない）。
        track!(storage.run_side_job_once())?; // GCを行う。
        track!(storage.run_side_job_once())?; // GCキューを充填する段階で、unreleased headを永続化する。
        {
            let snapshot = storage.journal_snapshot().unwrap();
            assert_eq!(snapshot.unreleased_head, 33);
            assert_eq!(snapshot.head, 66);
            assert_eq!(snapshot.tail, 66);
        }

        // (A)が永続化されていることを確認する。
        std::mem::drop(storage);
        let nvm = track!(FileNvm::open(dir.path().join("test.lusf")))?;
        let mut storage = track!(Storage::open(nvm))?;
        storage.set_automatic_gc_mode(false);
        {
            // (A)
            // ここで重要なのは、unreleased_headが0でない位置に移動していることだけ。
            let snapshot = storage.journal_snapshot().unwrap();
            assert_eq!(snapshot.unreleased_head, 33);
            assert_eq!(snapshot.head, 33); // 再起動後はunreleased_head == headで良い。
            assert_eq!(snapshot.tail, 66);
        }

        // journalの状態(B) を目指す。
        for _ in 0..3 {
            let vec: Vec<u8> = vec![42; 1000];
            let lump_data = track!(LumpData::new_embedded(vec))?;
            track!(storage.put(&test_lump_id, &lump_data))?;
            track!(storage.delete(&test_lump_id))?;
        }
        track!(storage.run_side_job_once())?; // GCキューを充填。
        track!(storage.run_side_job_once())?; // syncを行う（今回は意味がない）。
        track!(storage.run_side_job_once())?; // GCを行う。
        {
            // (B)
            // (B)は(C)に入る前準備なので特記するべき状態ではない。
            let snapshot = storage.journal_snapshot().unwrap();
            assert_eq!(snapshot.unreleased_head, 3198);
            assert_eq!(snapshot.head, 3198);
            assert_eq!(snapshot.tail, 3198);
        }
        /*
         * ジャーナル領域のhead positionはunreleased_headの値と常に等しいため
         * この段階ではジャーナル領域のhead positionフィールドは位置3198を指している。
         * これ以降ではPR23と同様に位置33に対して値42を書き込み、不正なtagとして認識させることを試みるが、
         * cannyls 0.9.2以降では問題にならない。
         *
         * 注意:
         *  cannyls 0.9.2以前では、head positionがこの段階で位置3198を指す保証はない。
         *  実際として、PR23の段階では古いunreleased_headの値33を指していた。
         */

        // tailを一周させ、位置33に対して値42を書き込む。
        let vec: Vec<u8> = vec![42; 2000];
        let lump_data = track!(LumpData::new_embedded(vec))?;
        track!(storage.put(&test_lump_id, &lump_data))?;
        {
            // (C)
            // 位置33の周辺を値`42`で上書きした状態。
            let snapshot = storage.journal_snapshot().unwrap();
            assert_eq!(snapshot.unreleased_head, 3198);
            assert_eq!(snapshot.head, 3198);
            assert_eq!(snapshot.tail, 2023);
        }

        // storageがcrashして再起動する操作群を模倣する。
        std::mem::drop(storage);
        let nvm = track!(FileNvm::open(dir.path().join("test.lusf")))?;
        let mut storage = track!(Storage::open(nvm))?;
        {
            let snapshot = storage.journal_snapshot().unwrap();
            assert_eq!(snapshot.unreleased_head, 3198);
            assert_eq!(snapshot.head, 3198);
            assert_eq!(snapshot.tail, 2023);
        }

        Ok(())
    }
}
