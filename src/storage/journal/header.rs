use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, SeekFrom, Write};

use Result;

use block::{AlignedBytes, BlockSize};
use nvm::NonVolatileMemory;

/// ジャーナルのヘッダ.
#[derive(Debug, PartialEq, Eq)]
pub struct JournalHeader {
    /// ジャーナルのリングバッファの始端位置.
    pub ring_buffer_head: u64,
}
impl JournalHeader {
    /// ストレージ初期化時のヘッダを生成する.
    pub fn new() -> Self {
        JournalHeader {
            ring_buffer_head: 0,
        }
    }

    /// ヘッダを書き込む.
    pub fn write_to<W: Write>(&self, mut writer: W, block_size: BlockSize) -> Result<()> {
        let padding = vec![0; JournalHeader::region_size(block_size) - 8];
        track_io!(writer.write_u64::<BigEndian>(self.ring_buffer_head))?;
        track_io!(writer.write_all(&padding))?;
        Ok(())
    }

    /// ヘッダを読み込む.
    pub fn read_from<R: Read>(mut reader: R, block_size: BlockSize) -> Result<Self> {
        let mut padding = vec![0; JournalHeader::region_size(block_size) - 8];
        let ring_buffer_head = track_io!(reader.read_u64::<BigEndian>())?;
        track_io!(reader.read_exact(&mut padding))?;
        Ok(JournalHeader { ring_buffer_head })
    }

    /// ヘッダ領域のサイズ（バイト数）.
    pub fn region_size(block_size: BlockSize) -> usize {
        block_size.as_u16() as usize
    }
}

/// ジャーナルのヘッダ領域.
///
/// 先頭の一ブロックがヘッダ用に割り当てられる.
#[derive(Debug)]
pub struct JournalHeaderRegion<N> {
    /// ヘッダ用の領域.
    nvm: N,

    /// ストレージが採用しているブロックサイズ.
    block_size: BlockSize,
}
impl<N: NonVolatileMemory> JournalHeaderRegion<N> {
    /// ヘッダ領域管理用のインスタンスを生成する.
    pub fn new(nvm: N, block_size: BlockSize) -> Self {
        JournalHeaderRegion { nvm, block_size }
    }

    /// ヘッダを書き込む.
    pub fn write_header(&mut self, header: &JournalHeader) -> Result<()> {
        let mut buf =
            AlignedBytes::new(JournalHeader::region_size(self.block_size), self.block_size);
        track!(header.write_to(&mut buf[..], self.block_size))?;

        track_io!(self.nvm.seek(SeekFrom::Start(0)))?;
        track_io!(self.nvm.write_all(&buf))?;
        track!(self.nvm.sync())?;
        Ok(())
    }

    /// ヘッダを読み込む.
    pub fn read_header(&mut self) -> Result<JournalHeader> {
        let mut buf =
            AlignedBytes::new(JournalHeader::region_size(self.block_size), self.block_size);
        track_io!(self.nvm.seek(SeekFrom::Start(0)))?;
        track_io!(self.nvm.read_exact(&mut buf))?;

        let header = track!(JournalHeader::read_from(&buf[..], self.block_size))?;
        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use trackable::result::TestResult;

    use super::*;
    use block::BlockSize;

    #[test]
    fn it_works() -> TestResult {
        let block_size = BlockSize::min();
        let header = JournalHeader {
            ring_buffer_head: 1234,
        };

        let mut buf = Vec::new();
        track!(header.write_to(&mut buf, block_size))?;
        assert_eq!(
            JournalHeader::read_from(&buf[..], block_size).ok(),
            Some(header)
        );
        Ok(())
    }
}
