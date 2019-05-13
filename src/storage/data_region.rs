use byteorder::{BigEndian, ByteOrder};
use prometrics::metrics::MetricBuilder;
use std::io::{Read, SeekFrom, Write};

use block::{AlignedBytes, BlockSize};
use metrics::DataRegionMetrics;
use nvm::NonVolatileMemory;
use storage::allocator::DataPortionAllocator;
use storage::portion::DataPortion;
use {ErrorKind, Result};

/// 各データの末尾に埋め込まれる情報のサイズ.
const LUMP_DATA_TRAILER_SIZE: usize = 2;

/// ランプのデータを格納するための領域.
#[derive(Debug)]
pub struct DataRegion<N> {
    allocator: DataPortionAllocator,
    nvm: N,
    block_size: BlockSize,
    metrics: DataRegionMetrics,
    safe_release_mode: bool,

    // 削除処理によってlump indexから外されたが
    // まだアロケータに通知して解放することができない
    // portionたちのバッファ
    pending_portions: Vec<DataPortion>,
}
impl<N> DataRegion<N>
where
    N: NonVolatileMemory,
{
    /// 新しい`DataRegion`インスタンスを生成する.
    pub fn new(metric_builder: &MetricBuilder, allocator: DataPortionAllocator, nvm: N) -> Self {
        let capacity = allocator.metrics().capacity_bytes;
        let block_size = allocator.metrics().block_size;
        let allocator_metrics = allocator.metrics().clone();
        DataRegion {
            allocator,
            nvm,
            block_size,
            metrics: DataRegionMetrics::new(metric_builder, capacity, allocator_metrics),
            safe_release_mode: false,
            pending_portions: Vec::new(),
        }
    }

    /// 安全にリソースを解放するモードに移行するためのメソッド。
    /// * `true`を渡すと、安全な解放モードに入る。
    ///    * 安全な解放については [wiki](https://github.com/frugalos/cannyls/wiki/Safe-Release-Mode) を参考のこと。
    /// * `false`を渡すと、従来の解放モードに入る。
    ///    * このモードでは、削除されたデータポーションが即座にアロケータから解放される。
    ///    * [issue28](https://github.com/frugalos/cannyls/issues28)があるため安全ではない。
    pub fn enable_safe_release_mode(&mut self, enabling: bool) {
        if !enabling && !self.pending_portions.is_empty() {
            // 削除対象ポーションが存在する状況で即時削除モードに切り替える場合は
            // この段階で削除を行う
            self.release_pending_portions();
        }
        self.safe_release_mode = enabling;
    }

    /// 安全にリソースを解放するモードに入っているかどうかを返す。
    /// * `true`なら安全な解放モード
    /// * `false`なら従来の解放モード
    pub fn is_in_safe_release_mode(&self) -> bool {
        self.safe_release_mode
    }

    /// データ領域のメトリクスを返す.
    pub fn metrics(&self) -> &DataRegionMetrics {
        &self.metrics
    }

    /// データを格納する.
    ///
    /// 格納場所は`DataRegion`が決定する.
    /// もし`data`を格納するだけの空きスペースがない場合には、`ErrorKind::StorageFull`エラーが返される.
    ///
    /// 成功した場合には、格納場所が返される.
    pub fn put(&mut self, data: &DataRegionLumpData) -> Result<DataPortion> {
        track_assert!(
            data.block_size().contains(self.block_size),
            ErrorKind::InvalidInput
        );
        let block_size = self.block_count(data.as_external_bytes().len() as u32) as u16;
        let portion =
            track_assert_some!(self.allocator.allocate(block_size), ErrorKind::StorageFull);

        let (offset, _size) = self.real_portion(&portion);
        track_io!(self.nvm.seek(SeekFrom::Start(offset)))?;
        track!(data.write_to(&mut self.nvm))?;

        // NOTE:
        // この後にジャーナルへの書き込みが行われ、
        // そこで(必要に応じて)`sync`メソッドが呼び出されるので、
        // この時点では`flush`のみに留める.
        track_io!(self.nvm.flush())?;

        Ok(portion)
    }

    /// 指定された領域に格納されているデータを取得する.
    ///
    /// `portion`で指定された領域が有効かどうかの判定は、このメソッド内では行われない.
    pub fn get(&mut self, portion: DataPortion) -> Result<DataRegionLumpData> {
        let (offset, size) = self.real_portion(&portion);
        track_io!(self.nvm.seek(SeekFrom::Start(offset)))?;

        let buf = AlignedBytes::new(size, self.block_size);
        let data = track!(DataRegionLumpData::read_from(&mut self.nvm, buf))?;
        Ok(data)
    }

    /// 指定された領域に格納されているデータを削除する.
    ///
    /// # パニック
    ///
    /// `portion`で未割当の領域が指定された場合には、
    /// 現在の実行スレッドがパニックする.
    pub fn delete(&mut self, portion: DataPortion) {
        if self.safe_release_mode {
            self.pending_portions.push(portion);
        } else {
            self.allocator.release(portion);
        }
    }

    /// 解放を遅延させているデータポーションをアロケータに送ることで全て解放する。
    ///
    /// # 安全解放モードで呼び出す前提条件
    /// 解放されるデータポーションに対応する削除レコードが、全て永続化されていること。
    ///
    /// # メモ
    /// 通常の解放モードで呼び出しても効果はない。
    pub fn release_pending_portions(&mut self) {
        if self.safe_release_mode {
            for p in ::std::mem::replace(&mut self.pending_portions, Vec::new()) {
                self.allocator.release(p);
            }
        }
    }

    /// 部分領域の単位をブロックからバイトに変換する.
    fn real_portion(&self, portion: &DataPortion) -> (u64, usize) {
        let offset = portion.start.as_u64() * u64::from(self.block_size.as_u16());
        let size = portion.len as usize * self.block_size.as_u16() as usize;
        (offset, size)
    }

    /// `size`分のデータをカバーするのに必要なブロック数.
    fn block_count(&self, size: u32) -> u32 {
        (size + u32::from(self.block_size.as_u16()) - 1) / u32::from(self.block_size.as_u16())
    }

    #[cfg(test)]
    pub fn is_allocated_portion(&self, portion: &DataPortion) -> bool {
        self.allocator.is_allocated_portion(portion)
    }
}

#[derive(Debug, Clone)]
pub struct DataRegionLumpData {
    bytes: AlignedBytes,
    data_size: usize,
}
impl DataRegionLumpData {
    pub fn new(data_size: usize, block_size: BlockSize) -> Self {
        let size = data_size + LUMP_DATA_TRAILER_SIZE;
        let mut bytes = AlignedBytes::new(size, block_size);
        bytes.align();

        let trailer_offset = bytes.len() - LUMP_DATA_TRAILER_SIZE;
        let padding_len = bytes.len() - size;
        debug_assert!(padding_len <= 0xFFFF);
        BigEndian::write_u16(&mut bytes[trailer_offset..], padding_len as u16);
        DataRegionLumpData { bytes, data_size }
    }

    pub fn block_size(&self) -> BlockSize {
        self.bytes.block_size()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.data_size]
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[..self.data_size]
    }

    /// 永続化用のバイト列を返す.
    fn as_external_bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    fn write_to<W: Write>(&self, mut writer: W) -> Result<()> {
        track_io!(writer.write_all(self.as_external_bytes()))
    }

    fn read_from<R: Read>(mut reader: R, mut buf: AlignedBytes) -> Result<Self> {
        track_assert!(buf.len() >= LUMP_DATA_TRAILER_SIZE, ErrorKind::InvalidInput);
        track_io!(reader.read_exact(&mut buf))?;

        let padding_len = BigEndian::read_u16(&buf[buf.len() - LUMP_DATA_TRAILER_SIZE..]) as usize;
        let data_size = buf
            .len()
            .checked_sub(LUMP_DATA_TRAILER_SIZE + padding_len)
            .unwrap_or(0);

        Ok(DataRegionLumpData {
            bytes: buf,
            data_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use prometrics::metrics::MetricBuilder;
    use std::iter;
    use trackable::result::TestResult;

    use super::super::allocator::DataPortionAllocator;
    use super::*;
    use block::BlockSize;
    use metrics::DataAllocatorMetrics;
    use nvm::MemoryNvm;

    macro_rules! make_data_region_on_memory {
        ($capacity:expr, $block_size:expr) => {{
            let metrics = MetricBuilder::new();
            let allocator = track!(DataPortionAllocator::build(
                DataAllocatorMetrics::new(&metrics, $capacity, $block_size),
                iter::empty(),
            ))?;
            let nvm = MemoryNvm::new(vec![0; $capacity as usize]);
            DataRegion::new(&metrics, allocator, nvm)
        }};
    }

    #[test]
    fn data_region_works() -> TestResult {
        let capacity = 10 * 1024;
        let block_size = BlockSize::min();
        let mut region = make_data_region_on_memory!(capacity, block_size);

        // put
        let mut data = DataRegionLumpData::new(3, block_size);
        data.as_bytes_mut().copy_from_slice("foo".as_bytes());
        let portion = track!(region.put(&data))?;

        // get
        assert_eq!(
            region.get(portion).ok().map(|d| d.as_bytes().to_owned()),
            Some("foo".as_bytes().to_owned())
        );
        Ok(())
    }

    #[test]
    fn enabling_and_confirming_safe_release_mode_work() -> TestResult {
        let capacity = 10 * 1024;
        let block_size = BlockSize::min();
        let mut region = make_data_region_on_memory!(capacity, block_size);

        // デフォルトでは安全解放モードを使わない。
        assert_eq!(region.is_in_safe_release_mode(), false);

        region.enable_safe_release_mode(true);
        assert_eq!(region.is_in_safe_release_mode(), true);

        Ok(())
    }

    #[test]
    fn delayed_releasing_works() -> TestResult {
        let capacity = 10 * 1024;
        let block_size = BlockSize::min();
        let mut region = make_data_region_on_memory!(capacity, block_size);

        region.enable_safe_release_mode(true);

        // put
        let mut data = DataRegionLumpData::new(3, block_size);
        data.as_bytes_mut().copy_from_slice("foo".as_bytes());
        let portion = track!(region.put(&data))?;

        // get
        assert_eq!(
            region.get(portion).ok().map(|d| d.as_bytes().to_owned()),
            Some("foo".as_bytes().to_owned())
        );

        region.delete(portion.clone());

        // まだ解放されていない。
        assert_eq!(region.allocator.is_allocated_portion(&portion), true);

        assert_eq!(region.pending_portions.len(), 1);

        region.release_pending_portions();

        // 解放された。
        assert_eq!(region.allocator.is_allocated_portion(&portion), false);

        Ok(())
    }
}
