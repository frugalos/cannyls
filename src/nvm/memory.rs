use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};

use block::BlockSize;
use nvm::NonVolatileMemory;
use {ErrorKind, Result};

type Memory = Cursor<Vec<u8>>;

/// メモリベースの`NonVolatileMemory`の実装.
///
/// # 注意
///
/// これは主にテストや性能計測用途を意図した実装であり、
/// `NonVolatileMemory`が本来要求する"不揮発性"は満たしていない.
#[derive(Debug)]
pub struct MemoryNvm {
    memory: Memory,
}
impl MemoryNvm {
    /// 新しい`MemoryNvm`インスタンスを生成する.
    pub fn new(memory: Vec<u8>) -> Self {
        MemoryNvm {
            memory: Cursor::new(memory),
        }
    }

    #[cfg(test)]
    pub fn as_bytes(&self) -> &[u8] {
        self.memory.get_ref()
    }

    fn seek_impl(&mut self, position: u64) -> Result<()> {
        track_assert!(
            self.block_size().is_aligned(position),
            ErrorKind::InvalidInput
        );
        self.memory.set_position(position);
        Ok(())
    }
    fn read_impl(&mut self, buf: &mut [u8]) -> Result<usize> {
        track_assert!(
            self.block_size().is_aligned(buf.len() as u64),
            ErrorKind::InvalidInput
        );
        track_io!(self.memory.read(buf))
    }
    fn write_impl(&mut self, buf: &[u8]) -> Result<()> {
        track_assert!(
            self.block_size().is_aligned(buf.len() as u64),
            ErrorKind::InvalidInput
        );

        // アライメントを維持するためには`write_all`を使う必要がある
        track_io!(self.memory.write_all(buf))
    }
}
impl NonVolatileMemory for MemoryNvm {
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
    fn position(&self) -> u64 {
        self.memory.position()
    }
    fn capacity(&self) -> u64 {
        self.memory.get_ref().len() as u64
    }
    fn block_size(&self) -> BlockSize {
        BlockSize::min()
    }
    fn split(mut self, position: u64) -> Result<(Self, Self)> {
        track_assert_eq!(
            position,
            self.block_size().ceil_align(position),
            ErrorKind::InvalidInput
        );
        track_assert!(position <= self.capacity(), ErrorKind::InvalidInput);
        let left = self.memory.get_mut().drain(..position as usize).collect();
        let left = MemoryNvm::new(left);

        self.memory.set_position(0);
        Ok((left, self))
    }
}
impl Seek for MemoryNvm {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position = self.convert_to_offset(pos)?;
        track!(self.seek_impl(position))?;
        Ok(position)
    }
}
impl Read for MemoryNvm {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_size = track!(self.read_impl(buf))?;
        Ok(read_size)
    }
}
impl Write for MemoryNvm {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        track!(self.write_impl(buf))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use trackable::result::TestResult;

    use super::*;
    use nvm::NonVolatileMemory;

    #[test]
    fn it_works() -> TestResult {
        let mut nvm = MemoryNvm::new(vec![0; 1024]);
        assert_eq!(nvm.capacity(), 1024);
        assert_eq!(nvm.position(), 0);

        // read, write, seek
        let mut buf = vec![0; 512];
        track_io!(nvm.read_exact(&mut buf))?;
        assert_eq!(buf, vec![0; 512]);
        assert_eq!(nvm.position(), 512);

        track_io!(nvm.write(&[1; 512][..]))?;
        assert_eq!(nvm.position(), 1024);

        track_io!(nvm.seek(SeekFrom::Start(512)))?;
        assert_eq!(nvm.position(), 512);

        track_io!(nvm.read_exact(&mut buf))?;
        assert_eq!(buf, vec![1; 512]);
        assert_eq!(nvm.position(), 1024);

        // split
        let (mut left, mut right) = track!(nvm.split(512))?;

        assert_eq!(left.capacity(), 512);
        track_io!(left.seek(SeekFrom::Start(0)))?;
        track_io!(left.read_exact(&mut buf))?;
        assert_eq!(buf, vec![0; 512]);
        assert_eq!(left.position(), 512);
        assert!(left.read_exact(&mut buf).is_err());

        assert_eq!(right.capacity(), 512);
        track_io!(right.seek(SeekFrom::Start(0)))?;
        track_io!(right.read_exact(&mut buf))?;
        assert_eq!(buf, vec![1; 512]);
        assert_eq!(right.position(), 512);
        assert!(right.read_exact(&mut buf).is_err());
        Ok(())
    }
}
