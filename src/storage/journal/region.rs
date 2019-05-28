use prometrics::metrics::MetricBuilder;
use std::collections::VecDeque;
use std::io::Write;
use std::ops::Range;

use super::options::JournalRegionOptions;
use super::record::{JournalEntry, JournalRecord, EMBEDDED_DATA_OFFSET};
use super::ring_buffer::JournalRingBuffer;
use super::{JournalHeader, JournalHeaderRegion};
use block::BlockSize;
use lump::LumpId;
use metrics::JournalRegionMetrics;
use nvm::NonVolatileMemory;
use storage::index::LumpIndex;
use storage::portion::{DataPortion, JournalPortion, Portion};
use storage::Address;
use {ErrorKind, Result};

// 一回の空き時間処理で実行するGC回数
const GC_COUNT_IN_SIDE_JOB: usize = 64;

/// デバイスに操作を記録するためのジャーナル領域.
///
/// ジャーナル領域はリングバッファ形式で管理されている.
///
/// # 参考
///
/// - [ストレージフォーマット(v1.0)][format]
/// - [ストレージのジャーナル領域のGC方法][gc]
///
/// [format]: https://github.com/frugalos/cannyls/wiki/Storage-Format
/// [gc]: https://github.com/frugalos/cannyls/wiki/Journal-Region-GC
#[derive(Debug)]
pub struct JournalRegion<N: NonVolatileMemory> {
    header_region: JournalHeaderRegion<N>,
    ring_buffer: JournalRingBuffer<N>,
    metrics: JournalRegionMetrics,
    gc_queue: VecDeque<JournalEntry>,
    sync_countdown: usize, // `0`になったら`sync()`を呼び出す
    options: JournalRegionOptions,
    gc_after_append: bool,
}
impl<N> JournalRegion<N>
where
    N: NonVolatileMemory,
{
    pub fn journal_entries(&mut self) -> Result<(u64, u64, u64, Vec<JournalEntry>)> {
        self.ring_buffer.journal_entries()
    }

    /// ジャーナル領域の初期化を行う.
    ///
    /// 具体的には`nmヘッダと最初のエントリ(EndOfEntries)を書き込む
    pub fn initialize<W: Write>(mut writer: W, block_size: BlockSize) -> Result<()> {
        track!(JournalHeader::new().write_to(&mut writer, block_size))?;
        track!(JournalRecord::EndOfRecords::<[_; 0]>.write_to(&mut writer))?;
        Ok(())
    }

    /// ジャーナル領域を開く。
    ///
    /// この関数の中で`index`の再構築も行われる.
    pub fn open(
        nvm: N,
        index: &mut LumpIndex,
        metric_builder: &MetricBuilder,
        options: JournalRegionOptions,
    ) -> Result<JournalRegion<N>>
    where
        N: NonVolatileMemory,
    {
        track_assert!(
            options.block_size.contains(nvm.block_size()),
            ErrorKind::InvalidInput; options.block_size, nvm.block_size()
        );
        let block_size = options.block_size;

        let (header_nvm, ring_buffer_nvm) =
            track!(nvm.split(JournalHeader::region_size(block_size) as u64))?;

        let mut header_region = JournalHeaderRegion::new(header_nvm, block_size);
        let header = track!(header_region.read_header())?;
        let ring_buffer =
            JournalRingBuffer::new(ring_buffer_nvm, header.ring_buffer_head, metric_builder);

        let metrics = JournalRegionMetrics::new(metric_builder, ring_buffer.metrics().clone());
        let mut journal = JournalRegion {
            header_region,
            ring_buffer,
            metrics,
            gc_queue: VecDeque::new(),
            sync_countdown: options.sync_interval,
            options,
            gc_after_append: true,
        };
        track!(journal.restore(index))?;
        Ok(journal)
    }

    /// PUT操作をジャーナルに記録する.
    pub fn records_put(
        &mut self,
        index: &mut LumpIndex,
        lump_id: &LumpId,
        portion: DataPortion,
    ) -> Result<()> {
        let record = JournalRecord::Put(*lump_id, portion);
        track!(self.append_record_with_gc::<[_; 0]>(index, &record))?;
        Ok(())
    }

    /// 埋め込みPUT操作をジャーナルに記録する.
    pub fn records_embed(
        &mut self,
        index: &mut LumpIndex,
        lump_id: &LumpId,
        data: &[u8],
    ) -> Result<()> {
        let record = JournalRecord::Embed(*lump_id, data);
        track!(self.append_record_with_gc(index, &record))?;
        Ok(())
    }

    /// DELETE操作をジャーナルに記録する.
    pub fn records_delete(&mut self, index: &mut LumpIndex, lump_id: &LumpId) -> Result<()> {
        let record = JournalRecord::Delete(*lump_id);
        track!(self.append_record_with_gc::<[_; 0]>(index, &record))?;
        Ok(())
    }

    // RANGE-DELETE操作をジャーナルに記録する。
    pub fn records_delete_range(
        &mut self,
        index: &mut LumpIndex,
        range: Range<LumpId>,
    ) -> Result<()> {
        let record = JournalRecord::DeleteRange(range);
        track!(self.append_record_with_gc::<[_; 0]>(index, &record))?;
        Ok(())
    }

    /// ジャーナル領域に埋め込まれたデータを取得する.
    pub fn get_embedded_data(&mut self, portion: JournalPortion) -> Result<Vec<u8>> {
        let offset = portion.start.as_u64();
        let mut buf = vec![0; portion.len as usize];
        track!(self.ring_buffer.read_embedded_data(offset, &mut buf))?;
        Ok(buf)
    }

    /// 補助タスクを一単位実行する.
    pub fn run_side_job_once(&mut self, index: &mut LumpIndex) -> Result<()> {
        if self.gc_queue.is_empty() {
            track!(self.fill_gc_queue())?;
        } else if self.sync_countdown != self.options.sync_interval {
            track!(self.sync())?;
        } else {
            for _ in 0..GC_COUNT_IN_SIDE_JOB {
                track!(self.gc_once(index))?;
            }
            track!(self.try_sync())?;
        }
        Ok(())
    }

    /// ジャーナル領域用のメトリクスを返す.
    pub fn metrics(&self) -> &JournalRegionMetrics {
        &self.metrics
    }

    /// GC処理を一単位実行する.
    fn gc_once(&mut self, index: &mut LumpIndex) -> Result<()> {
        if self.gc_queue.is_empty() && self.ring_buffer.capacity() < self.ring_buffer.usage() * 2 {
            // 空き領域が半分を切った場合には、`run_side_job_once()`以外でもGCを開始する
            // ("半分"という閾値に深い意味はない)
            track!(self.fill_gc_queue())?;
        }
        while let Some(entry) = self.gc_queue.pop_front() {
            self.metrics.gc_dequeued_records.increment();
            if !self.is_garbage(index, &entry) {
                // まだ回収できない場合には、ジャーナル領域の「末尾に」追加する
                track!(self.append_record(index, &entry.record))?;
                break;
            }
        }

        Ok(())
    }

    fn between(x: u64, y: u64, z: u64) -> bool {
        (x <= y && y <= z) || (z <= x && x <= y) || (y <= z && z <= x)
    }

    fn gc_all_entries_in_queue(&mut self, index: &mut LumpIndex) -> Result<()> {
        while !self.gc_queue.is_empty() {
            track!(self.gc_once(index))?;
        }
        Ok(())
    }

    pub fn gc_all_entries(&mut self, index: &mut LumpIndex) -> Result<()> {
        let current_tail_position = self.ring_buffer.tail();

        loop {
            let before_head = self.ring_buffer.head();
            if self.gc_queue.is_empty() {
                track!(self.fill_gc_queue())?;
            }
            track!(self.gc_all_entries_in_queue(index))?;
            if Self::between(before_head, current_tail_position, self.ring_buffer.head()) {
                break;
            }
        }

        // `gc_all_entries_in_queue`は`gc_queue`が空になるまで処理を行うため、
        // 上のloopを抜けた後では
        // `unreleased_head`と`head`の間のエントリは全て再配置済みである。
        // そこで現在の`head`の値をジャーナルエントリ開始位置として永続化し、
        // `unreleased_head`も更新する。
        let ring_buffer_head = self.ring_buffer.head();
        track!(self.write_journal_header(ring_buffer_head))?;

        Ok(())
    }

    /// `ring_buffer_head`をジャーナルエントリ開始位置として永続化し、
    /// `unreleased_head`を`ring_buffer_head`に移動する。
    fn write_journal_header(&mut self, ring_buffer_head: u64) -> Result<()> {
        let header = JournalHeader { ring_buffer_head };
        track!(self.header_region.write_header(&header))?;
        self.ring_buffer.release_bytes_until(ring_buffer_head);
        Ok(())
    }

    pub fn set_automatic_gc_mode(&mut self, enable: bool) {
        self.gc_after_append = enable;
    }

    fn append_record_with_gc<B>(
        &mut self,
        index: &mut LumpIndex,
        record: &JournalRecord<B>,
    ) -> Result<()>
    where
        B: AsRef<[u8]>,
    {
        track!(self.append_record(index, record))?;
        if self.gc_after_append {
            track!(self.gc_once(index))?; // レコード追記に合わせてGCを一単位行うことでコストを償却する
        }
        track!(self.try_sync())?;
        Ok(())
    }

    fn append_record<B>(&mut self, index: &mut LumpIndex, record: &JournalRecord<B>) -> Result<()>
    where
        B: AsRef<[u8]>,
    {
        let embedded = track!(self.ring_buffer.enqueue(record))?;
        if let Some((lump_id, portion)) = embedded {
            index.insert(lump_id, Portion::Journal(portion));
        }
        Ok(())
    }

    fn try_sync(&mut self) -> Result<()> {
        if self.sync_countdown == 0 {
            track!(self.sync())?;
        } else {
            self.sync_countdown -= 1;
        }
        Ok(())
    }

    /// ジャーナルバッファをディスクへ確実に書き出すために
    /// 同期命令を発行する。
    ///
    /// FIXME: 最適化として、
    /// 既に同期済みで必要のない場合は、同期命令を発行しないようにする。
    pub fn sync(&mut self) -> Result<()> {
        track!(self.ring_buffer.sync())?;
        self.sync_countdown = self.options.sync_interval;
        self.metrics.syncs.increment();
        Ok(())
    }

    /// エントリが回収可能かどうかを判定する.
    fn is_garbage(&self, index: &LumpIndex, entry: &JournalEntry) -> bool {
        match entry.record {
            JournalRecord::Put(ref lump_id, ref portion) => {
                index.get(lump_id) != Some(Portion::Data(*portion))
            }
            JournalRecord::Embed(ref lump_id, ref data) => {
                let portion = JournalPortion {
                    start: entry.start + Address::from(EMBEDDED_DATA_OFFSET as u32),
                    len: data.len() as u16,
                };
                index.get(lump_id) != Some(Portion::Journal(portion))
            }
            _ => true,
        }
    }

    /// GC用のキューの内容を補填する.
    ///
    /// 必要に応じて、ジャーナルヘッダの更新も行う.
    fn fill_gc_queue(&mut self) -> Result<()> {
        assert!(self.gc_queue.is_empty());

        // GCキューが空 `gc_queue.is_empty() == true`
        // すなわち `unreleased_head` と `head` の間のレコード群は全て再配置済みであるため、
        // 現在のhead位置をジャーナルエントリの開始位置として永続化し、
        // `unreleased_head`の位置も更新する。
        let ring_buffer_head = self.ring_buffer.head();
        track!(self.write_journal_header(ring_buffer_head))?;

        if self.ring_buffer.is_empty() {
            return Ok(());
        }

        for result in track!(self.ring_buffer.dequeue_iter())?.take(self.options.gc_queue_size) {
            let entry = track!(result)?;
            self.gc_queue.push_back(entry);
        }

        self.metrics
            .gc_enqueued_records
            .add_u64(self.gc_queue.len() as u64);
        Ok(())
    }

    /// リングバッファおよびインデックスを前回の状態に復元する.
    fn restore(&mut self, index: &mut LumpIndex) -> Result<()> {
        /*
         * 次のattributeはclippy 0.0.212(265318db 2019-05-17)で生じる
         * false positiveな警告を抑えるためのもの。
         * (see: https://github.com/rust-lang/rust-clippy/issues/4133)
         * Stableのclippy versionが上がったタイミングで外す。
         */
        #![allow(clippy::identity_conversion)]
        for result in track!(self.ring_buffer.restore_entries())? {
            let JournalEntry { start, record } = track!(result)?;
            match record {
                JournalRecord::Put(lump_id, portion) => {
                    index.insert(lump_id, Portion::Data(portion));
                }
                JournalRecord::Embed(lump_id, data) => {
                    let portion = JournalPortion {
                        start: start + Address::from(EMBEDDED_DATA_OFFSET as u32),
                        len: data.len() as u16,
                    };
                    index.insert(lump_id, Portion::Journal(portion));
                }
                JournalRecord::Delete(lump_id) => {
                    index.remove(&lump_id);
                }
                JournalRecord::DeleteRange(range) => {
                    for lump_id in index.list_range(range) {
                        index.remove(&lump_id);
                    }
                }
                JournalRecord::EndOfRecords | JournalRecord::GoToFront => unreachable!(),
            }
        }
        Ok(())
    }
}
