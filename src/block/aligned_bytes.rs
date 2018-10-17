use std;

use block::BlockSize;

/// 指定のブロック境界に開始位置および終端位置が揃えられたバイト列.
///
/// 内部的なメモリ管理の方法が異なるだけで、基本的には通常のバイト列(e.g., `&[u8]`)と同様に扱うことが可能.
#[derive(Debug)]
pub struct AlignedBytes {
    buf: Vec<u8>,
    offset: usize,
    len: usize,
    block_size: BlockSize,
}
unsafe impl Send for AlignedBytes {}
impl AlignedBytes {
    /// 新しい`AlignedBytes`インスタンスを生成する.
    ///
    /// 結果のバイト列の初期値は未定義.
    pub fn new(size: usize, block_size: BlockSize) -> Self {
        // バッファの前後をブロック境界に合わせて十分なだけの領域を確保しておく
        let capacity =
            block_size.ceil_align(size as u64) as usize + block_size.as_u16() as usize - 1;

        // ゼロ埋めのコストを省くためにunsafeを使用
        let mut buf = Vec::with_capacity(capacity);
        unsafe {
            buf.set_len(capacity);
        }

        let offset = alignment_offset(&buf, block_size);
        AlignedBytes {
            buf,
            offset,
            len: size,
            block_size,
        }
    }

    /// `bytes`と等しい内容を持つ`AlignedBytes`インスタンスを生成する.
    pub fn from_bytes(bytes: &[u8], block_size: BlockSize) -> Self {
        let mut aligned = Self::new(bytes.len(), block_size);
        aligned.as_mut().copy_from_slice(bytes);
        aligned
    }

    /// このバイト列のブロックサイズを返す.
    pub fn block_size(&self) -> BlockSize {
        self.block_size
    }

    /// 長さを次のブロック境界に揃える.
    ///
    /// 既に揃っているなら何もしない.
    ///
    /// なお、このメソッドの呼び出し有無に関わらず、内部的なメモリレイアウト上は、
    /// 常に適切なアライメントが行われている.
    pub fn align(&mut self) {
        self.len = self.block_size.ceil_align(self.len as u64) as usize;
    }

    /// 指定サイズに切り詰める.
    ///
    /// `size`が、現在のサイズを超えている場合には何も行わない.
    pub fn truncate(&mut self, size: usize) {
        if size < self.len {
            self.len = size;
        }
    }

    /// `new_min_len`の次のブロック境界へのリサイズを行う.
    ///
    /// サイズ拡大時には、必要に応じて内部バッファの再アロケートが行われる.
    pub fn aligned_resize(&mut self, new_min_len: usize) {
        self.resize(new_min_len);
        self.align();
    }

    /// リサイズを行う.
    ///
    /// サイズ拡大時には、必要に応じて内部バッファの再アロケートが行われる.
    pub fn resize(&mut self, new_len: usize) {
        let new_capacity = self.block_size.ceil_align(new_len as u64) as usize;
        if new_capacity > self.buf.len() - self.offset {
            let mut new_buf = vec![0; new_capacity + self.block_size.as_u16() as usize - 1];
            let new_offset = alignment_offset(&new_buf, self.block_size);
            (&mut new_buf[new_offset..][..self.len]).copy_from_slice(self.as_ref());

            self.buf = new_buf;
            self.offset = new_offset;
        }
        self.len = new_len;
    }

    /// バッファのキャパシティを返す.
    pub fn capacity(&self) -> usize {
        self.block_size.floor_align(self.buf.len() as u64) as usize
    }
}
impl std::ops::Deref for AlignedBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buf[self.offset..][..self.len]
    }
}
impl std::ops::DerefMut for AlignedBytes {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.offset..][..self.len]
    }
}
impl AsRef<[u8]> for AlignedBytes {
    fn as_ref(&self) -> &[u8] {
        &*self
    }
}
impl AsMut<[u8]> for AlignedBytes {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}
impl Clone for AlignedBytes {
    fn clone(&self) -> Self {
        AlignedBytes::from_bytes(self.as_ref(), self.block_size)
    }
}

fn alignment_offset(buf: &[u8], block_size: BlockSize) -> usize {
    unsafe {
        let ptr_usize: usize = std::mem::transmute(buf.as_ptr());
        let aligned_ptr_usize = block_size.ceil_align(ptr_usize as u64) as usize;
        aligned_ptr_usize - ptr_usize
    }
}

#[cfg(test)]
mod tests {
    use trackable::result::TestResult;

    use super::super::BlockSize;
    use super::*;

    #[test]
    fn new_works() -> TestResult {
        let bytes = AlignedBytes::new(10, BlockSize::new(512)?);
        assert_eq!(bytes.len(), 10);
        assert_eq!(bytes.capacity(), 512);
        Ok(())
    }

    #[test]
    fn from_bytes_works() -> TestResult {
        let bytes = AlignedBytes::from_bytes(b"foo", BlockSize::new(512)?);
        assert_eq!(bytes.as_ref(), b"foo");
        assert_eq!(bytes.capacity(), 512);
        Ok(())
    }

    #[test]
    fn align_works() -> TestResult {
        let mut bytes = AlignedBytes::new(10, BlockSize::new(512)?);
        assert_eq!(bytes.len(), 10);

        bytes.align();
        assert_eq!(bytes.len(), 512);

        bytes.align();
        assert_eq!(bytes.len(), 512);
        Ok(())
    }

    #[test]
    fn truncate_works() -> TestResult {
        let mut bytes = AlignedBytes::new(10, BlockSize::new(512)?);
        assert_eq!(bytes.len(), 10);

        bytes.truncate(2);
        assert_eq!(bytes.len(), 2);

        bytes.truncate(3);
        assert_eq!(bytes.len(), 2);
        Ok(())
    }

    #[test]
    fn aligned_resize_works() -> TestResult {
        let mut bytes = AlignedBytes::new(10, BlockSize::new(512)?);
        assert_eq!(bytes.len(), 10);

        bytes.aligned_resize(100);
        assert_eq!(bytes.len(), 512);
        Ok(())
    }

    #[test]
    fn resize_works() -> TestResult {
        let mut bytes = AlignedBytes::new(10, BlockSize::new(512)?);
        assert_eq!(bytes.len(), 10);
        assert_eq!(bytes.capacity(), 512);

        bytes.resize(700);
        assert_eq!(bytes.len(), 700);
        assert_eq!(bytes.capacity(), 1024);

        bytes.resize(2);
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes.capacity(), 1024);
        Ok(())
    }
}
