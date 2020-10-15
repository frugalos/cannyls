use std::cmp;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use crate::block::BlockSize;
use crate::nvm::NonVolatileMemory;
use crate::{Error, ErrorKind, Result};

/// インスタンスを共有可能な、メモリベースの`NonVolatileMemory`の実装.
///
/// # 注意
///
/// これはテスト用途のみを意図した実装であり、
/// `NonVolatileMemory`が本来要求する"不揮発性"は満たしていない.
#[derive(Debug, Clone)]
pub struct SharedMemoryNvm {
    memory: Arc<Mutex<Vec<u8>>>,
    memory_start: usize,
    memory_end: usize,
    block_size: BlockSize,
    position: usize,
}
impl SharedMemoryNvm {
    /// 新しい`SharedMemoryNvm`インスタンスを生成する.
    ///
    /// `SharedMemoryNvm::with_block_size(memory, BlockSize::min())`と等しい。
    pub fn new(memory: Vec<u8>) -> Self {
        Self::with_block_size(memory, BlockSize::min())
    }

    #[cfg(test)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let lock = self.memory.lock().unwrap();
        lock.clone()
    }

    /// ブロックサイズを指定して`SharedMemoryNvm`インスタンスを生成する.
    pub fn with_block_size(memory: Vec<u8>, block_size: BlockSize) -> Self {
        let memory_end = memory.len();
        SharedMemoryNvm {
            memory: Arc::new(Mutex::new(memory)),
            block_size,
            memory_start: 0,
            memory_end,
            position: 0,
        }
    }

    /// ブロックサイズを変更する.
    pub fn set_block_size(&mut self, block_size: BlockSize) {
        self.block_size = block_size;
    }

    fn with_bytes_mut<F, T>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut [u8]) -> T,
    {
        match self.memory.lock() {
            Ok(mut lock) => Ok(f(&mut lock[self.position..self.memory_end])),
            Err(error) => Err(track!(Error::from(error))),
        }
    }

    fn seek_impl(&mut self, position: u64) -> Result<()> {
        track_assert!(
            self.block_size().is_aligned(position),
            ErrorKind::InvalidInput
        );
        self.position = self.memory_start + position as usize;
        track_assert!(self.position <= self.memory_end, ErrorKind::InvalidInput);
        Ok(())
    }

    fn read_impl(&mut self, buf: &mut [u8]) -> Result<usize> {
        track_assert!(
            self.block_size().is_aligned(buf.len() as u64),
            ErrorKind::InvalidInput
        );

        let size = track!(self.with_bytes_mut(|memory| {
            let len = cmp::min(memory.len(), buf.len());
            (&mut buf[..len]).copy_from_slice(&memory[..len]);
            len
        }))?;
        self.position += size;
        Ok(size)
    }

    fn write_impl(&mut self, buf: &[u8]) -> Result<()> {
        track_assert!(
            self.block_size().is_aligned(buf.len() as u64),
            ErrorKind::InvalidInput
        );

        let size = track!(self.with_bytes_mut(|memory| {
            let len = cmp::min(memory.len(), buf.len());
            (&mut memory[..len]).copy_from_slice(&buf[..len]);
            len
        }))?;
        self.position += size;
        Ok(())
    }
}
impl NonVolatileMemory for SharedMemoryNvm {
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
    fn position(&self) -> u64 {
        (self.position - self.memory_start) as u64
    }
    fn capacity(&self) -> u64 {
        (self.memory_end - self.memory_start) as u64
    }
    fn block_size(&self) -> BlockSize {
        self.block_size
    }
    fn split(self, position: u64) -> Result<(Self, Self)> {
        track_assert_eq!(
            position,
            self.block_size().ceil_align(position),
            ErrorKind::InvalidInput
        );
        track_assert!(position <= self.capacity(), ErrorKind::InvalidInput);
        let mut left = self.clone();
        let mut right = self;

        left.memory_end = left.memory_start + position as usize;
        right.memory_start = left.memory_end;

        left.position = left.memory_start;
        right.position = right.memory_start;

        Ok((left, right))
    }
}
impl Seek for SharedMemoryNvm {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position = self.convert_to_offset(pos)?;
        track!(self.seek_impl(position))?;
        Ok(position)
    }
}
impl Read for SharedMemoryNvm {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_size = track!(self.read_impl(buf))?;
        Ok(read_size)
    }
}
impl Write for SharedMemoryNvm {
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
    use crate::nvm::NonVolatileMemory;

    #[test]
    fn it_works() -> TestResult {
        let mut nvm = SharedMemoryNvm::new(vec![0; 1024]);
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
