use prometrics::metrics::MetricBuilder;
use std::io::{BufReader, Read, Seek, SeekFrom};

use super::record::{EMBEDDED_DATA_OFFSET, END_OF_RECORDS_SIZE};
use super::{JournalEntry, JournalNvmBuffer, JournalRecord};
use crate::lump::LumpId;
use crate::metrics::JournalQueueMetrics;
use crate::nvm::NonVolatileMemory;
use crate::storage::portion::JournalPortion;
use crate::storage::Address;
use crate::{ErrorKind, Result};

/// ジャーナル領域用のリングバッファ.
#[derive(Debug)]
pub struct JournalRingBuffer<N: NonVolatileMemory> {
    nvm: JournalNvmBuffer<N>,

    /// 未解放分の含めた場合の、リングバッファの始端位置.
    ///
    /// `unreleased_head`から`head`の間に位置するレコード群は、
    /// `JournalRegion`によってデキューはされているが、
    /// まだGCによる再配置は終わっていない可能性があるので、
    /// 安全に上書きすることができない.
    unreleased_head: u64,

    /// リングバッファの始端位置.
    head: u64,

    /// リングバッファの終端位置.
    ///
    /// ここが次の追記開始位置となる.
    ///
    /// 不変項: `unreleased_head <= head <= tail`
    tail: u64,

    metrics: JournalQueueMetrics,
}
impl<N: NonVolatileMemory> JournalRingBuffer<N> {
    pub fn head(&self) -> u64 {
        self.head
    }
    pub fn tail(&self) -> u64 {
        self.tail
    }

    pub fn journal_entries(&mut self) -> Result<(u64, u64, u64, Vec<JournalEntry>)> {
        track_io!(self.nvm.seek(SeekFrom::Start(self.head)))?;
        let result: Result<Vec<JournalEntry>> =
            ReadEntries::new(&mut self.nvm, self.head).collect();
        result.map(|r| (self.unreleased_head, self.head, self.tail, r))
    }

    /// `JournalRingBuffer`インスタンスを生成する.
    pub fn new(nvm: N, head: u64, metric_builder: &MetricBuilder) -> Self {
        let metrics = JournalQueueMetrics::new(metric_builder);
        metrics.capacity_bytes.set(nvm.capacity() as f64);
        JournalRingBuffer {
            nvm: JournalNvmBuffer::new(nvm),
            unreleased_head: head,
            head,
            tail: head,
            metrics,
        }
    }

    /// NVMから以前のエントリ群を復元し、それらを操作するためのイテレータを返す.
    ///
    /// インスタンス生成直後に一度だけ呼ばれることを想定.
    pub fn restore_entries(&mut self) -> Result<RestoredEntries<N>> {
        track!(RestoredEntries::new(self))
    }

    /// リングバッファ内に要素が存在するかどうかを判定する.
    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    /// リングバッファの使用量(バイト単位)を返す.
    pub fn usage(&self) -> u64 {
        if self.unreleased_head <= self.tail {
            self.tail - self.unreleased_head
        } else {
            (self.tail + self.capacity()) - self.unreleased_head
        }
    }

    /// リングバッファの容量(バイト単位)を返す.
    pub fn capacity(&self) -> u64 {
        self.nvm.capacity()
    }

    /// リングバッファのメトリクスを返す.
    pub fn metrics(&self) -> &JournalQueueMetrics {
        &self.metrics
    }

    /// 指定位置に埋め込まれたlumpデータの読み込みを行う.
    ///
    /// データの妥当性検証は`cannyls`内では行わない.
    pub fn read_embedded_data(&mut self, position: u64, buf: &mut [u8]) -> Result<()> {
        track_io!(self.nvm.seek(SeekFrom::Start(position)))?;
        track_io!(self.nvm.read_exact(buf))?;
        Ok(())
    }

    /// 物理デバイスに同期命令を発行する.
    pub fn sync(&mut self) -> Result<()> {
        track!(self.nvm.sync())
    }

    /// レコードをジャーナルの末尾に追記する.
    ///
    /// レコードが`JournalRecord::Embed`だった場合には、データを埋め込んだ位置を結果として返す.
    pub fn enqueue<B: AsRef<[u8]>>(
        &mut self,
        record: &JournalRecord<B>,
    ) -> Result<Option<(LumpId, JournalPortion)>> {
        // 1. 十分な空き領域が存在するかをチェック
        track!(self.check_free_space(record))?;

        // 2. リングバッファの終端チェック
        if self.will_overflow(record) {
            track_io!(self.nvm.seek(SeekFrom::Start(self.tail)))?;
            track!(JournalRecord::GoToFront::<[_; 0]>.write_to(&mut self.nvm))?;

            // 先頭に戻って再試行
            self.metrics
                .consumed_bytes_at_running
                .add_u64(self.nvm.capacity() - self.tail);
            self.tail = 0;
            debug_assert!(!self.will_overflow(record));
            return self.enqueue(record);
        }

        // 3. レコードを書き込む
        let prev_tail = self.tail;
        track_io!(self.nvm.seek(SeekFrom::Start(self.tail)))?;
        track!(record.write_to(&mut self.nvm))?;
        self.metrics.enqueued_records_at_running.increment(record);

        // 4. 終端を示すレコードも書き込む
        self.tail = self.nvm.position(); // 次回の追記開始位置を保存 (`EndOfRecords`の直前)
        self.metrics
            .consumed_bytes_at_running
            .add_u64(self.tail - prev_tail);
        track!(JournalRecord::EndOfRecords::<[_; 0]>.write_to(&mut self.nvm))?;

        // 5. 埋め込みPUTの場合には、インデックスに位置情報を返す
        if let JournalRecord::Embed(ref lump_id, ref data) = *record {
            let portion = JournalPortion {
                start: Address::from_u64(prev_tail + EMBEDDED_DATA_OFFSET as u64).unwrap(),
                len: data.as_ref().len() as u16,
            };
            Ok(Some((*lump_id, portion)))
        } else {
            Ok(None)
        }
    }

    /// リングバッファの先頭からエントリ群を取り出す.
    ///
    /// `EndOfRecords`に到達した時点で走査は終了する.
    ///
    /// `EndOfRecords`および`GoToFront`は、走査対象には含まれない.
    pub fn dequeue_iter(&mut self) -> Result<DequeuedEntries<N>> {
        track!(DequeuedEntries::new(self))
    }

    pub fn release_bytes_until(&mut self, point: u64) {
        let released_bytes = if self.unreleased_head <= point {
            point - self.unreleased_head
        } else {
            (point + self.nvm.capacity()) - self.unreleased_head
        };
        self.metrics.released_bytes.add_u64(released_bytes);

        self.unreleased_head = point;
    }

    /// `record`を書き込んだら、リングバッファ用の領域を超えてしまうかどうかを判定する.
    fn will_overflow<B: AsRef<[u8]>>(&self, record: &JournalRecord<B>) -> bool {
        let mut next_tail = self.tail + record.external_size() as u64;

        // `EndOfRecords`は常に末尾に書き込まれるので、その分のサイズも考慮する
        next_tail += END_OF_RECORDS_SIZE as u64;

        next_tail > self.nvm.capacity()
    }

    /// `record`の書き込みを行うことで、リングバッファのTAILがHEADを追い越してしまう危険性がないかを確認する.
    fn check_free_space<B: AsRef<[u8]>>(&mut self, record: &JournalRecord<B>) -> Result<()> {
        // 書き込みの物理的な終端位置を計算
        let write_end = self.tail + (record.external_size() + END_OF_RECORDS_SIZE) as u64;

        // 次のブロック境界までのデータは上書きされる
        let write_end = self.nvm.block_size().ceil_align(write_end);

        // 安全に書き込み可能な位置の終端
        let free_end = if self.tail < self.unreleased_head {
            self.unreleased_head
        } else {
            self.nvm.capacity() + self.unreleased_head
        };
        track_assert!(
            write_end <= free_end,
            ErrorKind::StorageFull,
            "journal region is full: unreleased_head={}, head={}, tail={}, write_end={}, free_end={}",
            self.unreleased_head,
            self.head,
            self.tail,
            write_end,
            free_end
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct RestoredEntries<'a, N: 'a + NonVolatileMemory> {
    entries: ReadEntries<'a, N>,
    head: u64,
    tail: &'a mut u64,
    capacity: u64,
    metrics: &'a JournalQueueMetrics,
}
impl<'a, N: 'a + NonVolatileMemory> RestoredEntries<'a, N> {
    #[allow(clippy::new_ret_no_self)]
    fn new(ring: &'a mut JournalRingBuffer<N>) -> Result<Self> {
        // 生成直後の呼び出しかどうかを簡易チェック
        track_assert_eq!(
            ring.unreleased_head,
            ring.head,
            ErrorKind::InconsistentState
        );
        track_assert_eq!(ring.head, ring.tail, ErrorKind::InconsistentState);

        track_io!(ring.nvm.seek(SeekFrom::Start(ring.head)))?;
        let capacity = ring.nvm.capacity();
        Ok(RestoredEntries {
            entries: ReadEntries::with_capacity(&mut ring.nvm, ring.head, 1024 * 1024),
            head: ring.head,
            tail: &mut ring.tail,
            capacity,
            metrics: &ring.metrics,
        })
    }
}
impl<'a, N: 'a + NonVolatileMemory> Iterator for RestoredEntries<'a, N> {
    type Item = Result<JournalEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.entries.next();
        match next {
            Some(Ok(ref entry)) => {
                self.metrics
                    .enqueued_records_at_starting
                    .increment(&entry.record);
                *self.tail = entry.end().as_u64();
            }
            None => {
                let size = if self.head <= *self.tail {
                    *self.tail - self.head
                } else {
                    (*self.tail + self.capacity) - self.head
                };
                self.metrics.consumed_bytes_at_starting.add_u64(size);
            }
            _ => {}
        }
        next
    }
}

#[derive(Debug)]
pub struct DequeuedEntries<'a, N: 'a + NonVolatileMemory> {
    entries: ReadEntries<'a, N>,
    head: &'a mut u64,
    metrics: &'a JournalQueueMetrics,
}
impl<'a, N: 'a + NonVolatileMemory> DequeuedEntries<'a, N> {
    #[allow(clippy::new_ret_no_self)]
    fn new(ring: &'a mut JournalRingBuffer<N>) -> Result<Self> {
        track_io!(ring.nvm.seek(SeekFrom::Start(ring.head)))?;
        Ok(DequeuedEntries {
            entries: ReadEntries::new(&mut ring.nvm, ring.head),
            head: &mut ring.head,
            metrics: &ring.metrics,
        })
    }
}
impl<'a, N: 'a + NonVolatileMemory> Iterator for DequeuedEntries<'a, N> {
    type Item = Result<JournalEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.entries.next();
        if let Some(Ok(ref entry)) = next {
            self.metrics.dequeued_records.increment(&entry.record);
            *self.head = entry.end().as_u64();
        }
        next
    }
}

#[derive(Debug)]
struct ReadEntries<'a, N: 'a + NonVolatileMemory> {
    reader: BufReader<&'a mut JournalNvmBuffer<N>>,
    current: u64,
    is_second_lap: bool,
}
impl<'a, N: 'a + NonVolatileMemory> ReadEntries<'a, N> {
    fn new(nvm: &'a mut JournalNvmBuffer<N>, head: u64) -> Self {
        ReadEntries {
            reader: BufReader::new(nvm),
            current: head,
            is_second_lap: false,
        }
    }
    fn with_capacity(nvm: &'a mut JournalNvmBuffer<N>, head: u64, capacity: usize) -> Self {
        ReadEntries {
            reader: BufReader::with_capacity(capacity, nvm),
            current: head,
            is_second_lap: false,
        }
    }
    fn read_record(&mut self) -> Result<Option<JournalRecord<Vec<u8>>>> {
        match track!(JournalRecord::read_from(&mut self.reader))? {
            JournalRecord::EndOfRecords => Ok(None),
            JournalRecord::GoToFront => {
                track_assert!(!self.is_second_lap, ErrorKind::StorageCorrupted);
                track_io!(self.reader.seek(SeekFrom::Start(0)))?;
                self.current = 0;
                self.is_second_lap = true;
                self.read_record()
            }
            record => Ok(Some(record)),
        }
    }
}
impl<'a, N: 'a + NonVolatileMemory> Iterator for ReadEntries<'a, N> {
    type Item = Result<JournalEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.read_record() {
            Err(e) => Some(Err(e)),
            Ok(None) => None,
            Ok(Some(record)) => {
                let start = Address::from_u64(self.current).expect("Never fails");
                self.current += record.external_size() as u64;
                let entry = JournalEntry { start, record };
                Some(Ok(entry))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prometrics::metrics::MetricBuilder;
    use trackable::result::TestResult;

    use super::*;
    use crate::nvm::MemoryNvm;
    use crate::storage::portion::DataPortion;
    use crate::storage::{Address, JournalRecord};
    use crate::ErrorKind;

    #[test]
    fn append_and_read_records() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024]);
        let mut ring = JournalRingBuffer::new(nvm, 0, &MetricBuilder::new());

        let records = vec![
            record_put("000", 30, 5),
            record_put("111", 100, 300),
            record_delete("222"),
            record_embed("333", b"foo"),
            record_delete("444"),
            record_delete_range("000", "999"),
        ];
        for record in &records {
            assert!(ring.enqueue(record).is_ok());
        }

        let mut position = Address::from(0);
        for (entry, record) in track!(ring.dequeue_iter())?.zip(records.iter()) {
            let entry = track!(entry)?;
            assert_eq!(entry.record, *record);
            assert_eq!(entry.start, position);
            position = position + Address::from(record.external_size() as u32);
        }

        assert_eq!(ring.unreleased_head, 0);
        assert_eq!(ring.head, position.as_u64());
        assert_eq!(ring.tail, position.as_u64());

        assert_eq!(track!(ring.dequeue_iter())?.count(), 0);
        Ok(())
    }

    #[test]
    fn read_embedded_data() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024]);
        let mut ring = JournalRingBuffer::new(nvm, 0, &MetricBuilder::new());

        track!(ring.enqueue(&record_put("000", 30, 5)))?;
        track!(ring.enqueue(&record_delete("111")))?;

        let (lump_id, portion) =
            track!(ring.enqueue(&record_embed("222", b"foo")))?.expect("Some(_)");
        assert_eq!(lump_id, track_any_err!("222".parse())?);

        let mut buf = vec![0; portion.len as usize];
        track!(ring.read_embedded_data(portion.start.as_u64(), &mut buf))?;
        assert_eq!(buf, b"foo");
        Ok(())
    }

    #[test]
    fn go_round_ring_buffer() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024]);
        let mut ring = JournalRingBuffer::new(nvm, 512, &MetricBuilder::new());
        assert_eq!(ring.head, 512);
        assert_eq!(ring.tail, 512);

        let record = record_delete("000");
        for _ in 0..(512 / record.external_size()) {
            track!(ring.enqueue(&record))?;
        }
        assert_eq!(ring.tail, 1016);

        track!(ring.enqueue(&record))?;
        assert_eq!(ring.tail, 21);
        Ok(())
    }

    #[test]
    fn full() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024]);
        let mut ring = JournalRingBuffer::new(nvm, 0, &MetricBuilder::new());

        let record = record_put("000", 1, 2);
        while ring.tail <= 1024 - record.external_size() as u64 {
            track!(ring.enqueue(&record))?;
        }
        assert_eq!(ring.tail, 1008);

        assert_eq!(
            ring.enqueue(&record).err().map(|e| *e.kind()),
            Some(ErrorKind::StorageFull)
        );
        assert_eq!(ring.tail, 1008);

        ring.unreleased_head = 511;
        ring.head = 511;
        assert_eq!(
            ring.enqueue(&record).err().map(|e| *e.kind()),
            Some(ErrorKind::StorageFull)
        );

        ring.unreleased_head = 512;
        ring.head = 512;
        assert!(ring.enqueue(&record).is_ok());
        assert_eq!(ring.tail, record.external_size() as u64);
        Ok(())
    }

    #[test]
    fn too_large_record() {
        let nvm = MemoryNvm::new(vec![0; 1024]);
        let mut ring = JournalRingBuffer::new(nvm, 0, &MetricBuilder::new());

        let record = record_embed("000", &[0; 997]);
        assert_eq!(record.external_size(), 1020);
        assert_eq!(
            ring.enqueue(&record).err().map(|e| *e.kind()),
            Some(ErrorKind::StorageFull)
        );

        let record = record_embed("000", &[0; 996]);
        assert_eq!(record.external_size(), 1019);
        assert!(ring.enqueue(&record).is_ok());
        assert_eq!(ring.tail, 1019);
    }

    fn record_put(lump_id: &str, start: u32, len: u16) -> JournalRecord<Vec<u8>> {
        JournalRecord::Put(
            lump_id.parse().unwrap(),
            DataPortion {
                start: Address::from(start),
                len,
            },
        )
    }

    fn lump_id(id: &str) -> LumpId {
        id.parse().unwrap()
    }

    fn record_embed(id: &str, data: &[u8]) -> JournalRecord<Vec<u8>> {
        JournalRecord::Embed(lump_id(id), data.to_owned())
    }

    fn record_delete(id: &str) -> JournalRecord<Vec<u8>> {
        JournalRecord::Delete(lump_id(id))
    }

    fn record_delete_range(start: &str, end: &str) -> JournalRecord<Vec<u8>> {
        use std::ops::Range;
        JournalRecord::DeleteRange(Range {
            start: lump_id(start),
            end: lump_id(end),
        })
    }
}
