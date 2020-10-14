//! 不揮発性メモリのインターフェース定義と実装群.
//!
//! このモジュールは[Storage](../storage/struct.Storage.html)がデータの読み書きに使用する
//! 永続化領域を提供する.
use std::io::{Read, Seek, SeekFrom, Write};

pub use self::file::{FileNvm, FileNvmBuilder};
pub use self::memory::MemoryNvm;
pub use self::shared_memory::SharedMemoryNvm;

use crate::block::{AlignedBytes, BlockSize};
use crate::{ErrorKind, Result};

mod file;
mod memory;
mod shared_memory;

/// 不揮発性メモリを表すトレイト.
///
/// "不揮発性メモリ"は「永続化可能なバイト列(領域)」を意味し、lump群を保存するために使用される.
///
/// 読み書きの際には、位置およびサイズ、がブロック境界にアライメントされている必要がある.
///
/// このトレイト自体は汎用的なインタフェースを提供しているが、
/// 実装は、読み書きされるデータは[`lusf`]形式に即したものである、ということを想定しても構わない.
/// (e.g., `lusf`のヘッダからキャパシティ情報を取得する)
///
/// [`lusf`]: https://github.com/frugalos/cannyls/wiki/Storage-Format
pub trait NonVolatileMemory: Sized + Read + Write + Seek {
    /// メモリの内容を、物理デバイスに同期する.
    ///
    /// 内部的にバッファ管理等を行っておらず、常に内容が同期されている場合には、
    /// このメソッド内で特に何かを行う必要はない。
    fn sync(&mut self) -> Result<()>;

    /// 読み書き用カーソルの現在位置を返す.
    fn position(&self) -> u64;

    /// メモリの容量(バイト単位)を返す.
    fn capacity(&self) -> u64;

    /// このインスタンスのブロックサイズを返す.
    ///
    /// 利用者は、ブロックサイズに揃うように、読み書き時のアライメントを行う必要がある.
    fn block_size(&self) -> BlockSize;

    /// メモリを指定位置で分割する.
    ///
    /// # Errors
    ///
    /// 以下の場合には、種類が`ErrorKind::InvalidInput`のエラーが返される:
    ///
    /// - 指定位置が容量を超えている
    /// - `position`がブロック境界ではない
    fn split(self, position: u64) -> Result<(Self, Self)>;

    /// `SeekFrom`形式で指定された位置を、開始地点からのオフセットに変換する.
    ///
    /// # Errors
    ///
    /// 「指定位置が容量を超えている」ないし「`0`未満」の場合には、
    /// 種類が`ErrorKind::InvalidInput`のエラーが返される.
    fn convert_to_offset(&self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                track_assert!(offset <= self.capacity(), ErrorKind::InvalidInput);
                Ok(offset)
            }
            SeekFrom::End(delta) => {
                let offset = self.capacity() as i64 + delta;
                track_assert!(0 <= offset, ErrorKind::InvalidInput);
                Ok(offset as u64)
            }
            SeekFrom::Current(delta) => {
                let offset = self.position() as i64 + delta;
                track_assert!(0 <= offset, ErrorKind::InvalidInput);
                Ok(offset as u64)
            }
        }
    }

    /// このインスタンスが指定するブロック境界へのアライメントを保証した書き込みを行う.
    ///
    /// `f`の引数(一時バッファ)に対して書き込まれたデータは、その後`AlignedBytes`にコピーされた上で、
    /// 実際のNVMに書き出される.
    ///
    /// なお一時バッファの末尾から、次のブロック境界までは、
    /// 任意のバイト列で埋められるので、注意が必要(i.e., 書き込み先の既存データは上書きされる).
    fn aligned_write_all<F>(&mut self, f: F) -> Result<()>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<()>,
    {
        let mut buf = Vec::new();
        track!(f(&mut buf))?;

        let mut aligned_bytes = AlignedBytes::from_bytes(&buf, self.block_size());
        aligned_bytes.align();
        track_io!(self.write_all(&aligned_bytes))?;
        Ok(())
    }

    /// このインスタンスが指定するブロック境界へのアライメントを保証した上で、指定サイズ分のバイト列を読み込む.
    fn aligned_read_bytes(&mut self, size: usize) -> Result<AlignedBytes> {
        let mut buf = AlignedBytes::new(size, self.block_size());
        buf.align();
        track_io!(self.read_exact(&mut buf))?;
        buf.truncate(size);
        Ok(buf)
    }
}
