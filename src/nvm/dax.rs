use std::io::{self, Read, Seek, SeekFrom, Write};

use block::BlockSize;
use nvm::NonVolatileMemory;
use {ErrorKind, Result};
use libc::{c_int, size_t, ssize_t};

pub enum DaxHandle {}

#[link(name="devdax", kind="static")]
extern "C" {
    fn nvm_open(path: *const u8, size: u64) -> *mut DaxHandle;
    fn nvm_split(handle: *mut DaxHandle, pos: u64) -> *mut DaxHandle;
    fn nvm_close(handle: *mut DaxHandle);
    fn nvm_lseek(handle: *mut DaxHandle, off: size_t, whence: c_int) -> ssize_t;
    fn nvm_write(handle: *mut DaxHandle, buf: *const u8, len: size_t) -> ssize_t;
    fn nvm_read(handle: *mut DaxHandle, buf: *mut u8, len: size_t) -> ssize_t;
    fn nvm_position(handle: *mut DaxHandle) -> ssize_t;
    fn nvm_size(handle: *mut DaxHandle) -> ssize_t;
}

/// メモリベースの`NonVolatileMemory`の実装.
///
#[derive(Debug)]
pub struct MemoryNvm {
    handle: *mut DaxHandle,
}
impl MemoryNvm {
    pub fn new(size: u64) -> Self {
        MemoryNvm {
            handle: unsafe { nvm_open("".as_ptr(), size) }
        }
    }
    pub fn split(handle: &mut MemoryNvm, pos: u64) -> Self {
        MemoryNvm {
            handle: unsafe { nvm_split(handle.handle, pos) }
        }
    }

    fn seek_impl(&mut self, position: u64) -> Result<()> {
        track_assert!(
            self.block_size().is_aligned(position),
            ErrorKind::InvalidInput
        );
        unsafe { nvm_lseek(self.handle, position as usize, 0); }
        Ok(())
    }
    fn read_impl(&mut self, buf: &mut [u8]) -> Result<usize> {
        track_assert!(
            self.block_size().is_aligned(buf.len() as u64),
            ErrorKind::InvalidInput
        );
        let x = unsafe {nvm_read(self.handle, buf.as_mut_ptr(), buf.len())};
        track_assert!(x >= 0, ErrorKind::InvalidInput);
        Ok(x as usize)
    }
    fn write_impl(&mut self, buf: &[u8]) -> Result<()> {
        track_assert!(
            self.block_size().is_aligned(buf.len() as u64),
            ErrorKind::InvalidInput
        );

        // アライメントを維持するためには`write_all`を使う必要がある
        let x = unsafe {nvm_write(self.handle, buf.as_ptr(), buf.len())};
        track_assert!(x >= 0, ErrorKind::InvalidInput);
        Ok(())
    }
}
impl NonVolatileMemory for MemoryNvm {
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
    fn position(&self) -> u64 {
        unsafe { nvm_position(self.handle) as u64 }
    }
    fn capacity(&self) -> u64 {
        unsafe { nvm_size(self.handle) as u64 }
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
        let right = MemoryNvm::split(&mut self, position);

        unsafe { nvm_lseek(self.handle, 0, 0); }
        Ok((self, right))
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
        let mut nvm = MemoryNvm::new(1024);
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
