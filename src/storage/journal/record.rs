use adler32::RollingAdler32;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::ops::Range;

use crate::lump::LumpId;
use crate::storage::portion::DataPortion;
use crate::storage::Address;
use crate::{ErrorKind, Result};

pub const TAG_SIZE: usize = 1;
pub const CHECKSUM_SIZE: usize = 4;
pub const LENGTH_SIZE: usize = 2;
pub const PORTION_SIZE: usize = 5;
pub const END_OF_RECORDS_SIZE: usize = CHECKSUM_SIZE + TAG_SIZE;
pub const EMBEDDED_DATA_OFFSET: usize = CHECKSUM_SIZE + TAG_SIZE + LumpId::SIZE + LENGTH_SIZE;

const TAG_END_OF_RECORDS: u8 = 0;
const TAG_GO_TO_FRONT: u8 = 1;
const TAG_PUT: u8 = 3;
const TAG_EMBED: u8 = 4;
const TAG_DELETE: u8 = 5;
const TAG_DELETE_RANGE: u8 = 6;

/// ジャーナル領域のリングバッファのエントリ.
#[derive(Debug)]
pub struct JournalEntry {
    /// ジャーナル内でのレコードの開始位置.
    pub start: Address,

    /// レコード.
    pub record: JournalRecord<Vec<u8>>,
}
impl JournalEntry {
    /// ジャーナル内でのレコードの終端位置を返す.
    pub fn end(&self) -> Address {
        self.start + Address::from(self.record.external_size() as u32)
    }
}

/// ジャーナル領域のリングバッファに追記されていくレコード.
#[allow(missing_docs)]
#[derive(Debug, PartialEq, Eq)]
pub enum JournalRecord<T> {
    EndOfRecords,
    GoToFront,
    Put(LumpId, DataPortion),
    Embed(LumpId, T),
    Delete(LumpId),
    DeleteRange(Range<LumpId>),
}
impl<T: AsRef<[u8]>> JournalRecord<T> {
    /// 読み書き時のサイズ（バイト数）を返す.
    pub(crate) fn external_size(&self) -> usize {
        let record_size = match *self {
            JournalRecord::EndOfRecords | JournalRecord::GoToFront => 0,
            JournalRecord::Put(..) => LumpId::SIZE + LENGTH_SIZE + PORTION_SIZE,
            JournalRecord::Embed(_, ref data) => LumpId::SIZE + LENGTH_SIZE + data.as_ref().len(),
            JournalRecord::Delete(..) => LumpId::SIZE,
            JournalRecord::DeleteRange(..) => LumpId::SIZE * 2,
        };
        CHECKSUM_SIZE + TAG_SIZE + record_size
    }

    /// `writer`にレコードを書き込む.
    pub(crate) fn write_to<W: Write>(&self, mut writer: W) -> Result<()> {
        track_io!(writer.write_u32::<BigEndian>(self.checksum()))?;
        match *self {
            JournalRecord::EndOfRecords => {
                track_io!(writer.write_u8(TAG_END_OF_RECORDS))?;
            }
            JournalRecord::GoToFront => {
                track_io!(writer.write_u8(TAG_GO_TO_FRONT))?;
            }
            JournalRecord::Put(ref lump_id, portion) => {
                track_io!(writer.write_u8(TAG_PUT))?;
                track_io!(writer.write_u128::<BigEndian>(lump_id.as_u128()))?;
                track_io!(writer.write_u16::<BigEndian>(portion.len))?;
                track_io!(writer.write_uint::<BigEndian>(portion.start.as_u64(), PORTION_SIZE))?;
            }
            JournalRecord::Embed(ref lump_id, ref data) => {
                debug_assert!(data.as_ref().len() <= 0xFFFF);
                track_io!(writer.write_u8(TAG_EMBED))?;
                track_io!(writer.write_u128::<BigEndian>(lump_id.as_u128()))?;
                track_io!(writer.write_u16::<BigEndian>(data.as_ref().len() as u16))?;
                track_io!(writer.write_all(data.as_ref()))?;
            }
            JournalRecord::Delete(ref lump_id) => {
                track_io!(writer.write_u8(TAG_DELETE))?;
                track_io!(writer.write_u128::<BigEndian>(lump_id.as_u128()))?;
            }
            JournalRecord::DeleteRange(ref range) => {
                track_io!(writer.write_u8(TAG_DELETE_RANGE))?;
                track_io!(writer.write_u128::<BigEndian>(range.start.as_u128()))?;
                track_io!(writer.write_u128::<BigEndian>(range.end.as_u128()))?;
            }
        }
        Ok(())
    }

    fn checksum(&self) -> u32 {
        let mut adler32 = RollingAdler32::new();
        match *self {
            JournalRecord::EndOfRecords => {
                adler32.update(TAG_END_OF_RECORDS);
            }
            JournalRecord::GoToFront => {
                adler32.update(TAG_GO_TO_FRONT);
            }
            JournalRecord::Put(ref lump_id, portion) => {
                adler32.update(TAG_PUT);
                adler32.update_buffer(&lump_id_to_u128(lump_id)[..]);
                let mut buf = [0; 7];
                BigEndian::write_u16(&mut buf, portion.len);
                BigEndian::write_uint(&mut buf[2..], portion.start.as_u64(), PORTION_SIZE);
                adler32.update_buffer(&buf);
            }
            JournalRecord::Embed(ref lump_id, ref data) => {
                debug_assert!(data.as_ref().len() <= 0xFFFF);
                adler32.update(TAG_EMBED);
                adler32.update_buffer(&lump_id_to_u128(lump_id)[..]);
                let mut buf = [0; 2];
                BigEndian::write_u16(&mut buf, data.as_ref().len() as u16);
                adler32.update_buffer(&buf);
                adler32.update_buffer(data.as_ref());
            }
            JournalRecord::Delete(ref lump_id) => {
                adler32.update(TAG_DELETE);
                adler32.update_buffer(&lump_id_to_u128(lump_id)[..]);
            }
            JournalRecord::DeleteRange(ref range) => {
                adler32.update(TAG_DELETE_RANGE);
                adler32.update_buffer(&lump_id_to_u128(&range.start)[..]);
                adler32.update_buffer(&lump_id_to_u128(&range.end)[..]);
            }
        }
        adler32.hash()
    }
}
impl JournalRecord<Vec<u8>> {
    /// `reader`からレコードを読み込む.
    pub(crate) fn read_from<R: Read>(mut reader: R) -> Result<Self> {
        let checksum = track_io!(reader.read_u32::<BigEndian>())?;
        let tag = track_io!(reader.read_u8())?;
        let record = match tag {
            TAG_END_OF_RECORDS => JournalRecord::EndOfRecords,
            TAG_GO_TO_FRONT => JournalRecord::GoToFront,
            TAG_PUT => {
                let lump_id = track!(read_lump_id(&mut reader))?;
                let data_len = track_io!(reader.read_u16::<BigEndian>())?;
                let data_offset = track_io!(reader.read_uint::<BigEndian>(PORTION_SIZE))?;
                let portion = DataPortion {
                    start: Address::from_u64(data_offset).unwrap(),
                    len: data_len,
                };
                JournalRecord::Put(lump_id, portion)
            }
            TAG_EMBED => {
                let lump_id = track!(read_lump_id(&mut reader))?;
                let data_len = track_io!(reader.read_u16::<BigEndian>())?;
                let mut data = vec![0; data_len as usize];
                track_io!(reader.read_exact(&mut data))?;
                JournalRecord::Embed(lump_id, data)
            }
            TAG_DELETE => {
                let lump_id = track!(read_lump_id(&mut reader))?;
                JournalRecord::Delete(lump_id)
            }
            TAG_DELETE_RANGE => {
                let start = track!(read_lump_id(&mut reader))?;
                let end = track!(read_lump_id(&mut reader))?;
                JournalRecord::DeleteRange(Range { start, end })
            }
            _ => track_panic!(
                ErrorKind::StorageCorrupted,
                "Unknown journal record tag: {}",
                tag
            ),
        };
        track_assert_eq!(record.checksum(), checksum, ErrorKind::StorageCorrupted);
        Ok(record)
    }
}

fn read_lump_id<R: Read>(reader: &mut R) -> Result<LumpId> {
    let id = track_io!(reader.read_u128::<BigEndian>())?;
    Ok(LumpId::new(id))
}

fn lump_id_to_u128(id: &LumpId) -> [u8; LumpId::SIZE] {
    let mut bytes = [0; LumpId::SIZE];
    BigEndian::write_u128(&mut bytes, id.as_u128());
    bytes
}

#[cfg(test)]
mod tests {
    use trackable::result::TestResult;

    use super::*;
    use crate::lump::LumpId;
    use crate::storage::portion::DataPortion;
    use crate::storage::Address;

    #[test]
    fn read_write_works() -> TestResult {
        let records = vec![
            JournalRecord::EndOfRecords,
            JournalRecord::GoToFront,
            JournalRecord::Put(
                lump_id("000"),
                DataPortion {
                    start: Address::from(0),
                    len: 10,
                },
            ),
            JournalRecord::Put(
                lump_id("000"),
                DataPortion {
                    start: Address::from_u64((1 << 40) - 1).unwrap(),
                    len: 0xFFFF,
                },
            ),
            JournalRecord::Embed(lump_id("111"), b"222".to_vec()),
            JournalRecord::Embed(lump_id("111"), vec![0; 0xFFFF]),
            JournalRecord::Delete(lump_id("333")),
            JournalRecord::DeleteRange(Range {
                start: lump_id("123"),
                end: lump_id("456"),
            }),
        ];
        for e0 in records {
            let mut buf = Vec::new();
            track!(e0.write_to(&mut buf))?;
            let e1 = track!(JournalRecord::read_from(&buf[..]))?;
            assert_eq!(e1, e0);
        }
        Ok(())
    }

    #[test]
    fn checksum_works() -> TestResult {
        let e: JournalRecord<Vec<u8>> = JournalRecord::Put(
            lump_id("000"),
            DataPortion {
                start: Address::from(0),
                len: 10,
            },
        );
        let mut buf = Vec::new();
        track!(e.write_to(&mut buf))?;
        buf[6] += 1; // Tampers a byte

        let result = JournalRecord::read_from(&buf[..]);
        assert!(result.is_err());
        Ok(())
    }

    fn lump_id(id: &str) -> LumpId {
        id.parse().unwrap()
    }
}
