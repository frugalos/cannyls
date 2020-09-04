use std::cmp;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ptr;

use block::{AlignedBytes, BlockSize};
use nvm::NonVolatileMemory;
use {ErrorKind, Result};

/// ジャーナル領域用のバッファ.
///
/// 内部の`NonVolatileMemory`実装のアライメント制約(i.e., ストレージのブロック境界に揃っている)を満たしつつ、
/// ジャーナル領域への追記を効率化するのが目的.
#[derive(Debug)]
pub struct JournalNvmBuffer<N: NonVolatileMemory> {
    // ジャーナル領域のデータを、実際に永続化するために使用される内部のNVMインスタンス
    inner: N,

    // 現在の読み書きカーソルの位置
    position: u64,

    // 書き込みバッファ
    //
    // ジャーナル領域から発行された書き込み要求は、
    // 以下のいずれかの条件を満たすまでは、メモリ上の本バッファに保持されており、
    // 内部NVMには反映されないままとなる:
    // - `sync`メソッドが呼び出された:
    //   - ジャーナル領域は定期的に本メソッドを呼び出す
    // - 書き込みバッファのカバー範囲に重複する領域に対して、読み込み要求が発行された場合:
    //   - 書き込みバッファの内容をフラッシュして、内部NVMに同期した後に、該当読み込み命令を処理
    // - 書き込みバッファのカバー範囲に重複しない領域に対して、書き込み要求が発行された場合:
    //   - 現状の書き込みバッファのデータ構造では、ギャップ(i.e., 連続しない複数部分領域)を表現することはできない
    //   - そのため、一度古いバッファの内容をフラッシュした後に、該当書き込み要求を処理するためのバッファを作成する
    //
    // ジャーナル領域が発行した書き込み要求を、
    // 内部NVMのブロック境界に合うようにアライメントする役目も担っている。
    write_buf: AlignedBytes,

    // `write_buf`の始端が、内部NVM上のどの位置に対応するかを保持するためのフィールド
    //
    // 「内部NVM上での位置を指す」という点では`position`フィールドと似ているが、
    // `position`は読み書きやシーク操作の度に値が更新されるのに対して、
    // `write_buf_offset`は、書き込みバッファの内容がフラッシュされるまでは、
    // 固定の値が使用され続ける。
    write_buf_offset: u64,

    // 書き込みバッファ内にデータが溜まっているかどうかを判定するためのフラグ
    //
    // 一度でも書き込みバッファにデータが書かれたら`true`に設定され、
    // 内部NVMにバッファ内のデータがフラッシュされた後は`false`に設定される。
    maybe_dirty: bool,

    // 読み込みバッファ
    //
    // ジャーナル領域が発行した読み込み要求を、
    // 内部NVMのブロック境界に合うようにアライメントするために使用される。
    read_buf: AlignedBytes,

    // バッファの安全なflushを行うかどうかを意味するフラグ
    //
    // trueの場合は、バッファの先頭から512バイト以降を書き出してsyncした後に、
    // 先頭から512バイトをatomicに書き出す。
    // 
    // falseの場合は、バッファ全体をdiskに向けて単にflushする
    safe_flush: bool,
}
impl<N: NonVolatileMemory> JournalNvmBuffer<N> {
    /// 新しい`JournalNvmBuffer`インスタンスを生成する.
    ///
    /// これは実際に読み書きには`nvm`を使用する.
    ///
    /// なお`nvm`へのアクセス時に、それが`nvm`が要求するブロック境界にアライメントされていることは、
    /// `JournalNvmBuffer`が保証するため、利用者が気にする必要はない.
    ///
    /// ただし、シーク時には、シーク地点を含まない次のブロック境界までのデータは
    /// 上書きされてしまうので注意が必要.
    pub fn new(nvm: N, safe_flush: bool) -> Self {
        let block_size = nvm.block_size();
        JournalNvmBuffer {
            inner: nvm,
            position: 0,
            maybe_dirty: false,
            write_buf_offset: 0,
            write_buf: AlignedBytes::new(0, block_size),
            read_buf: AlignedBytes::new(0, block_size),
            safe_flush,
        }
    }

    #[cfg(test)]
    pub fn nvm(&self) -> &N {
        &self.inner
    }

    fn is_dirty_area(&self, offset: u64, length: usize) -> bool {
        if !self.maybe_dirty || length == 0 || self.write_buf.is_empty() {
            return false;
        }
        if self.write_buf_offset < offset {
            let buf_end = self.write_buf_offset + self.write_buf.len() as u64;
            offset < buf_end
        } else {
            let end = offset + length as u64;
            self.write_buf_offset < end
        }
    }

    /*
     * バッファの内容をdiskに書き出す。
     *
     * 関数名が表すように、flushを意図したものであってdiskへの同期までは考慮していない。
     * ただし、safe_syncがtrueの場合は、書き出し順をコントロールするために、
     * 内部的にパラメタ化されているNVMのsyncメソッドを呼び出すことになる。
     * ただしその場合でも、実装の都合により、メモリバッファ全体がdiskへ永続化されるとは限らない。
     */
    fn flush_write_buf(&mut self) -> Result<()> {
        if self.write_buf.is_empty() || !self.maybe_dirty {
            return Ok(());
        }

        if self.safe_flush {
            /*
             * issue 27(https://github.com/frugalos/cannyls/issues/27)を考慮した
             * 順序づいた書き込みを行う。
             *
             * ここで順序とは次を意味する
             * 1. 書き込みバッファの512バイト以降を全て書き出す。
             * 2. Diskへの同期命令を発行する。
             * 3. 書き込みバッファの先頭512バイトを書き出す。
             *
             * 3.のステップで既存のEORを上書きするため、
             * これを最後に行うことにより、DiskからEORが消えた状態になることを避ける。
             *
             * パフォーマンスに関する問題点:
             *  a. Diskへの同期命令はミリセカンド単位でのブロックを生じる。
             *  b. ステップ3でシークが生じるのでシーケンシャルwriteでなくなってしまう。
             *
             * 先頭512バイトについて:
             *  先頭は512*nバイトであれば、DIRECT_IOとの兼ね合いとしては問題がない。
             *  ただし、「多くのHDDについては」512バイト=セクタサイズであり
             *  先頭部分の書き出しがatomicな書き出しになるという利点がある。
             *  ただし用いているファイルシステムの実装によっては
             *  実際には512バイトが分断されて書き出される可能性もあり、常にこの利点を享受できるとは限らない。
             */
            let buf: &[u8] = &self.write_buf;
            assert!(buf.as_ptr() as usize % 512 == 0);
            assert!(buf.len() % 512 == 0);
            if buf.len() > 512 {
                track_io!(self
                    .inner
                    .seek(SeekFrom::Start(self.write_buf_offset + 512)))?;
                track_io!(self.inner.write(&buf[512..]))?;
                track!(self.inner.sync())?;
            }
            track_io!(self.inner.seek(SeekFrom::Start(self.write_buf_offset)))?;
            track_io!(self.inner.write(&buf[..512]))?;
        } else {
            track_io!(self.inner.seek(SeekFrom::Start(self.write_buf_offset)))?;
            track_io!(self.inner.write(&self.write_buf))?;
        }

        if self.write_buf.len() > self.block_size().as_u16() as usize {
            // このif節では、
            // バッファに末端のalignmentバイト分(= new_len)の情報を残す。
            // write_buf_offsetは、write_buf.len() - new_len(= drop_len)分だけ進められる。
            //
            // write_buf_offsetを、書き出しに成功したwrite_buf.len()分だけ進めて、
            // write_bufをクリアすることもできるが、
            // ブロック長でしか書き出すことができないため、その場合は次回の書き込み時に
            // NVMに一度アクセスしてブロック全体を取得しなくてはならない。
            // この読み込みを避けるため、現在の実装の形をとっている。
            let new_len = self.block_size().as_u16() as usize;
            let drop_len = self.write_buf.len() - new_len;
            unsafe {
                // This nonoverlappingness is guranteed by the callers.
                ptr::copy(
                    self.write_buf.as_ptr().add(drop_len), // src
                    self.write_buf.as_mut_ptr(),           // dst
                    new_len,
                );
            }
            self.write_buf.truncate(new_len);

            self.write_buf_offset += drop_len as u64;
        }
        self.maybe_dirty = false;
        Ok(())
    }

    fn check_overflow(&self, write_len: usize) -> Result<()> {
        let next_position = self.position() + write_len as u64;
        track_assert!(
            next_position <= self.capacity(),
            ErrorKind::InconsistentState,
            "self.position={}, write_len={}, self.len={}",
            self.position(),
            write_len,
            self.capacity()
        );
        Ok(())
    }
}
impl<N: NonVolatileMemory> NonVolatileMemory for JournalNvmBuffer<N> {
    fn sync(&mut self) -> Result<()> {
        track!(self.flush_write_buf())?;
        self.inner.sync()
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn capacity(&self) -> u64 {
        self.inner.capacity()
    }

    fn block_size(&self) -> BlockSize {
        self.inner.block_size()
    }

    fn split(self, _: u64) -> Result<(Self, Self)> {
        unreachable!()
    }
}
impl<N: NonVolatileMemory> Drop for JournalNvmBuffer<N> {
    fn drop(&mut self) {
        let _ = self.sync();
    }
}
impl<N: NonVolatileMemory> Seek for JournalNvmBuffer<N> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let offset = track!(self.convert_to_offset(pos))?;
        self.position = offset;
        Ok(offset)
    }
}
impl<N: NonVolatileMemory> Read for JournalNvmBuffer<N> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.is_dirty_area(self.position, buf.len()) {
            track!(self.flush_write_buf())?;
        }

        let aligned_start = self.block_size().floor_align(self.position);
        let aligned_end = self
            .block_size()
            .ceil_align(self.position + buf.len() as u64);

        self.read_buf
            .aligned_resize((aligned_end - aligned_start) as usize);
        self.inner.seek(SeekFrom::Start(aligned_start))?;
        let inner_read_size = self.inner.read(&mut self.read_buf)?;

        let start = (self.position - aligned_start) as usize;
        let end = cmp::min(inner_read_size, start + buf.len());
        let read_size = end - start;
        (&mut buf[..read_size]).copy_from_slice(&self.read_buf[start..end]);
        self.position += read_size as u64;
        Ok(read_size)
    }
}
impl<N: NonVolatileMemory> Write for JournalNvmBuffer<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        track!(self.check_overflow(buf.len()))?;

        let write_buf_start = self.write_buf_offset;
        let write_buf_end = write_buf_start + self.write_buf.len() as u64;
        if write_buf_start <= self.position && self.position <= write_buf_end {
            // 領域が重複しており、バッファの途中から追記可能
            // (i.e., 書き込みバッファのフラッシュが不要)
            let start = (self.position - self.write_buf_offset) as usize;
            let end = start + buf.len();
            self.write_buf.aligned_resize(end);
            (&mut self.write_buf[start..end]).copy_from_slice(buf);
            self.position += buf.len() as u64;
            self.maybe_dirty = true;
            Ok(buf.len())
        } else {
            // 領域に重複がないので、一度バッファの中身を書き戻す
            track!(self.flush_write_buf())?;

            if self.block_size().is_aligned(self.position) {
                self.write_buf_offset = self.position;
                self.write_buf.aligned_resize(0);
            } else {
                // シーク位置より前方の既存データが破棄されてしまわないように、一度読み込みを行う.
                let size = self.block_size().as_u16();
                self.write_buf_offset = self.block_size().floor_align(self.position);
                self.write_buf.aligned_resize(size as usize);
                self.inner.seek(SeekFrom::Start(self.write_buf_offset))?;
                self.inner.read_exact(&mut self.write_buf)?;
            }
            self.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        track!(self.flush_write_buf())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use trackable::result::TestResult;

    use super::*;
    use nvm::MemoryNvm;

    #[test]
    fn write_write_flush() -> TestResult {
        // 連続領域の書き込みは`flush`するまでバッファに残り続ける
        let mut buffer = new_buffer();
        track_io!(buffer.write_all(b"foo"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);

        track_io!(buffer.write_all(b"bar"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);
        assert_eq!(&buffer.nvm().as_bytes()[3..6], &[0; 3][..]);

        track_io!(buffer.flush())?;
        assert_eq!(&buffer.nvm().as_bytes()[0..6], b"foobar");
        Ok(())
    }

    #[test]
    fn write_seek_write_flush() -> TestResult {
        // "連続"の判定は、ブロック単位で行われる
        // (シークしてもブロックを跨がないと"連続していない"と判定されない)
        let mut buffer = new_buffer();
        track_io!(buffer.write_all(b"foo"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);

        track_io!(buffer.seek(SeekFrom::Current(1)))?;
        track_io!(buffer.write_all(b"bar"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);
        assert_eq!(&buffer.nvm().as_bytes()[4..7], &[0; 3][..]);

        track_io!(buffer.flush())?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], b"foo");
        assert_eq!(&buffer.nvm().as_bytes()[4..7], b"bar");

        // シーク先を遠くした場合でも、連続するブロック内に収まっているなら同様
        let mut buffer = new_buffer();
        track_io!(buffer.write_all(b"foo"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);

        track_io!(buffer.seek(SeekFrom::Start(512)))?;
        track_io!(buffer.write_all(b"bar"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);
        assert_eq!(&buffer.nvm().as_bytes()[512..515], &[0; 3][..]);

        track_io!(buffer.flush())?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], b"foo");
        assert_eq!(&buffer.nvm().as_bytes()[512..515], b"bar");

        // 書き込み領域が重なっている場合も同様
        let mut buffer = new_buffer();
        track_io!(buffer.write_all(b"foo"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);

        track_io!(buffer.seek(SeekFrom::Current(-1)))?;
        track_io!(buffer.write_all(b"bar"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);
        assert_eq!(&buffer.nvm().as_bytes()[2..5], &[0; 3][..]);

        track_io!(buffer.flush())?;
        assert_eq!(&buffer.nvm().as_bytes()[0..5], b"fobar");
        Ok(())
    }

    #[test]
    fn write_seek_write() -> TestResult {
        // 書き込み先が（ブロック単位で）隣接しなくなった場合は、現在のバッファの中身がNVMに書き戻される
        let mut buffer = new_buffer();
        track_io!(buffer.write_all(b"foo"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);

        track_io!(buffer.seek(SeekFrom::Start(513)))?;
        track_io!(buffer.write_all(b"bar"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], b"foo");
        assert_eq!(&buffer.nvm().as_bytes()[513..516], &[0; 3][..]);
        Ok(())
    }

    #[test]
    fn write_seek_read() -> TestResult {
        // 読み込み先が、書き込みバッファと重なっている場合には、バッファの中身がNVMに書き戻される
        let mut buffer = new_buffer();
        track_io!(buffer.write_all(b"foo"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);

        track_io!(buffer.read_exact(&mut [0; 1][..]))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], b"foo");

        // 読み込み先が、書き込みバッファと重なっていない場合には、書き戻されない
        let mut buffer = new_buffer();
        track_io!(buffer.write_all(b"foo"))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);

        track_io!(buffer.seek(SeekFrom::Start(512)))?;
        track_io!(buffer.read_exact(&mut [0; 1][..]))?;
        assert_eq!(&buffer.nvm().as_bytes()[0..3], &[0; 3][..]);
        Ok(())
    }

    #[test]
    fn overwritten() -> TestResult {
        // シーク地点よりも前方のデータは保持される.
        // (後方の、次のブロック境界までのデータがどうなるかは未定義)
        let mut buffer = new_buffer();
        track_io!(buffer.write_all(&[b'a'; 512]))?;
        track_io!(buffer.flush())?;
        assert_eq!(&buffer.nvm().as_bytes()[0..512], &[b'a'; 512][..]);

        track_io!(buffer.seek(SeekFrom::Start(256)))?;
        track_io!(buffer.write_all(&[b'b'; 1]))?;
        track_io!(buffer.flush())?;
        assert_eq!(&buffer.nvm().as_bytes()[0..256], &[b'a'; 256][..]);
        assert_eq!(buffer.nvm().as_bytes()[256], b'b');
        Ok(())
    }

    fn new_buffer() -> JournalNvmBuffer<MemoryNvm> {
        let nvm = MemoryNvm::new(vec![0; 10 * 1024]);
        JournalNvmBuffer::new(nvm, false)
    }
}
