#[cfg(unix)]
use libc;
use std::cmp;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use block::BlockSize;
use nvm::NonVolatileMemory;
use storage::StorageHeader;
use {ErrorKind, Result};

/// `FileNvm`のビルダ
///
/// 二つのメンバを持ち、それぞれ次の意味を表す:
/// direct_io: バッファリングなしI/Oを行うかどうか（Linux: O_DIRECT, Mac: F_NOCACHE）
/// exclusive_lock: `FileNvm`の実体ファイルに対するアクセスの排他制御を行うかどうか
pub struct FileNvmBuilder {
    direct_io: bool,
    exclusive_lock: bool,
}

impl FileNvmBuilder {
    /// デフォルト設定で`FileNvmBuilder`インスタンスを作成する
    ///
    /// デフォルトでは、direct_io = trueかつexclusive_lock = trueとなる
    pub fn new() -> Self {
        FileNvmBuilder {
            direct_io: true,
            exclusive_lock: true,
        }
    }

    #[cfg(target_os = "linux")]
    fn open_options(&self) -> fs::OpenOptions {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(false);

        if self.direct_io {
            options.custom_flags(libc::O_DIRECT);
        }
        options
    }
    #[cfg(not(target_os = "linux"))]
    fn open_options(&self) -> fs::OpenOptions {
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(false);
        options
    }

    #[cfg(target_os = "macos")]
    fn set_fnocache_if_flag_is_on(&self, file: &File) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        if self.direct_io {
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } != 0 {
                track_io!(Err(io::Error::last_os_error()))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
    #[cfg(not(target_os = "macos"))]
    fn set_fnocache_if_flag_is_on(&self, _file: &File) -> Result<()> {
        Ok(())
    }

    #[cfg(unix)]
    fn set_exclusive_file_lock_if_flag_is_on(&self, file: &File) -> Result<()> {
        use std::os::unix::io::AsRawFd;
        if self.exclusive_lock {
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                track_io!(Err(io::Error::last_os_error()))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
    #[cfg(not(unix))]
    fn set_exclusive_file_lock_if_flag_is_on(&self, _file: &File) -> Result<()> {
        Ok(())
    }

    /// direct_ioのon/offを設定する
    pub fn direct_io(&mut self, enabled: bool) -> &mut Self {
        self.direct_io = enabled;
        self
    }

    /// exclusive_lockのon/offを設定する
    pub fn exclusive_lock(&mut self, enabled: bool) -> &mut Self {
        self.exclusive_lock = enabled;
        self
    }

    /// 新しい`FileNvm`インスタンスを生成する.
    ///
    /// `filepath`が既に存在する場合にはそれを開き、存在しない場合には新規にファイルを作成する.
    ///
    /// 返り値のタプルの二番目の値は、ファイルが新規作成されたかどうか (`true`なら新規作成).
    pub fn create_if_absent<P: AsRef<Path>>(
        &mut self,
        filepath: P,
        capacity: u64,
    ) -> Result<(FileNvm, bool)> {
        if filepath.as_ref().exists() {
            track!(self.open(filepath)).map(|s| (s, false))
        } else {
            track!(self.create(filepath, capacity)).map(|s| (s, true))
        }
    }

    /// ファイルを新規に作成して`FileNvm`インスタンスを生成する.
    pub fn create<P: AsRef<Path>>(&mut self, filepath: P, capacity: u64) -> Result<FileNvm> {
        if let Some(dir) = filepath.as_ref().parent() {
            track_io!(fs::create_dir_all(dir))?;
        }
        let mut options = self.open_options();
        options.create(true);
        let file = track_io!(options.open(filepath))?;
        track!(self.set_exclusive_file_lock_if_flag_is_on(&file))?;
        track!(self.set_fnocache_if_flag_is_on(&file))?;
        Ok(FileNvm::with_range(file, 0, capacity))
    }

    /// 既存のファイルを開いて`FileNvm`インスタンスを生成する。
    ///
    /// ここでいう既存のファイルとは、以前に `Storage::create` 等で
    /// 作成済みのlusfファイルを指す。
    ///
    /// lusfファイルにはcapacity情報が埋め込まれているので
    /// createとは異なりcapacity引数を要求しない。
    pub fn open<P: AsRef<Path>>(&mut self, filepath: P) -> Result<FileNvm> {
        let saved_header = track!(StorageHeader::read_from_file(&filepath))?;
        let capacity = saved_header.storage_size();
        let options = self.open_options();
        let file = track_io!(options.open(filepath))?;
        track!(self.set_exclusive_file_lock_if_flag_is_on(&file))?;
        track!(self.set_fnocache_if_flag_is_on(&file))?;
        Ok(FileNvm::with_range(file, 0, capacity))
    }
}

/// ファイルベースの`NonVolatileMemory`の実装.
///
/// 現状の実装ではブロックサイズは`BlockSize::min()`に固定.
///
/// UNIX環境であれば、ファイルは`O_DIRECT`フラグ付きでオープンされる.
///
/// # 参考
///
/// `O_DIRECT`と`O_SYNC/O_DSYNC`に関して:
///
/// - [http://stackoverflow.com/questions/5055859/](http://stackoverflow.com/questions/5055859/)
/// - [https://lwn.net/Articles/457667/](https://lwn.net/Articles/457667/)
#[derive(Debug)]
pub struct FileNvm {
    file: File,
    cursor_position: u64,
    view_start: u64,
    view_end: u64,
}
impl FileNvm {
    /// デフォルト設定で新しい`FileNvm`インスタンスを生成する.
    ///
    /// デフォルト設定では、O_DIRECT (MacではF_NOCACHE）でのバッファリングなしI/Oを行い
    /// ファイルアクセスに対する排他制御を行う。
    ///
    /// `filepath`が既に存在する場合にはそれを開き、存在しない場合には新規にファイルを作成する.
    ///
    /// 返り値のタプルの二番目の値は、ファイルが新規作成されたかどうか (`true`なら新規作成).
    pub fn create_if_absent<P: AsRef<Path>>(filepath: P, capacity: u64) -> Result<(Self, bool)> {
        FileNvmBuilder::new().create_if_absent(filepath, capacity)
    }

    /// デフォルト設定でファイルを新規に作成して`FileNvm`インスタンスを生成する.
    ///
    /// デフォルト設定では、O_DIRECT (MacではF_NOCACHE）でのバッファリングなしI/Oを行い
    /// ファイルアクセスに対する排他制御を行う。
    pub fn create<P: AsRef<Path>>(filepath: P, capacity: u64) -> Result<Self> {
        FileNvmBuilder::new().create(filepath, capacity)
    }

    /// デフォルト設定で既存のファイルを開き`FileNvm`インスタンスを生成する。
    ///
    /// デフォルト設定では、O_DIRECT (MacではF_NOCACHE）でのバッファリングなしI/Oを行い
    /// ファイルアクセスに対する排他制御を行う。
    ///
    /// ここでいう既存のファイルとは、以前に `Storage::create` 等で
    /// 作成済みのlusfファイルを指す。
    ///
    /// lusfファイルにはcapacity情報が埋め込まれているので
    /// createとは異なりcapacity引数を要求しない。
    pub fn open<P: AsRef<Path>>(filepath: P) -> Result<Self> {
        FileNvmBuilder::new().open(filepath)
    }

    fn with_range(file: File, start: u64, end: u64) -> FileNvm {
        FileNvm {
            file,
            cursor_position: start,
            view_start: start,
            view_end: end,
        }
    }

    fn seek_impl(&mut self, position: u64) -> Result<()> {
        track_assert!(
            self.block_size().is_aligned(position),
            ErrorKind::InvalidInput
        );

        let file_position = self.view_start + position;
        track_io!(self.file.seek(io::SeekFrom::Start(file_position)))?;
        self.cursor_position = file_position;
        Ok(())
    }
    fn read_impl(&mut self, buf: &mut [u8]) -> Result<usize> {
        track_assert!(
            self.block_size().is_aligned(buf.len() as u64),
            ErrorKind::InvalidInput
        );

        let max_len = (self.capacity() - self.position()) as usize;
        let len = cmp::min(max_len, buf.len());
        let new_cursor_position = self.cursor_position + len as u64;

        let read_size = track_io!(self.file.read(&mut buf[..len]))?;
        if read_size < len {
            // まだ未書き込みの末尾部分から読み込みを行った場合には、
            // カーソルの位置がズレないように明示的にシークを行う.
            track!(self.seek_impl(new_cursor_position))?;
        }
        self.cursor_position = new_cursor_position;
        Ok(len)
    }
    fn write_impl(&mut self, buf: &[u8]) -> Result<usize> {
        track_assert!(
            self.block_size().is_aligned(buf.len() as u64),
            ErrorKind::InvalidInput
        );

        let max_len = (self.capacity() - self.position()) as usize;
        let len = cmp::min(max_len, buf.len());
        let new_cursor_position = self.cursor_position + len as u64;
        track_io!(self.file.write_all(&buf[..len]))?;
        self.cursor_position = new_cursor_position;
        Ok(len)
    }

    #[test]
    fn inner(&self) -> &File {
        &self.file
    }
}
impl NonVolatileMemory for FileNvm {
    fn sync(&mut self) -> Result<()> {
        track_io!(self.file.sync_data())?;
        Ok(())
    }
    fn position(&self) -> u64 {
        self.cursor_position - self.view_start
    }
    fn capacity(&self) -> u64 {
        self.view_end - self.view_start
    }
    fn block_size(&self) -> BlockSize {
        BlockSize::min()
    }
    fn split(self, position: u64) -> Result<(Self, Self)> {
        track_assert_eq!(
            position,
            self.block_size().ceil_align(position),
            ErrorKind::InvalidInput
        );
        track_assert!(position <= self.capacity(), ErrorKind::InvalidInput);
        let left_file = track_io!(self.file.try_clone())?;
        let left_start = self.view_start;
        let left_end = left_start + position;
        let left = Self::with_range(left_file, left_start, left_end);

        let right_start = left_end;
        let right_end = self.view_end;
        let right = Self::with_range(self.file, right_start, right_end);
        Ok((left, right))
    }
}
impl Seek for FileNvm {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position = self.convert_to_offset(pos)?;
        track!(self.seek_impl(position))?;
        Ok(position)
    }
}
impl Read for FileNvm {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_size = track!(self.read_impl(buf))?;
        Ok(read_size)
    }
}
impl Write for FileNvm {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written_size = track!(self.write_impl(buf))?;
        Ok(written_size)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::mem;
    use tempdir::TempDir;
    use trackable::result::TestResult;
    use uuid::Uuid;

    use super::*;
    use block::{AlignedBytes, BlockSize};
    use storage::{StorageHeader, MAJOR_VERSION, MINOR_VERSION};

    #[test]
    fn open_and_create_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let capacity = 10 * 1024;

        // 存在しないファイルは開けない
        assert!(FileNvm::open(dir.path().join("foo")).is_err());

        // ファイル作成
        let mut file = track!(FileNvm::create(dir.path().join("foo"), capacity))?;

        // `FileNvm`はオープン時にlusfのヘッダがあることを想定しているので先頭に適当なヘッダを書き込む
        let mut data = Vec::new();
        track!(storage_header().write_to(&mut data))?;
        let header_len = data.len();

        data.extend_from_slice(b"bar");
        track_io!(file.write_all(&aligned_bytes(&data[..])))?;

        // 同じファイルを同時に開くことはできない
        assert!(FileNvm::open(dir.path().join("foo")).is_err());
        assert!(FileNvm::create(dir.path().join("foo"), capacity).is_err());

        // 一度閉じれば、オープン可能
        mem::drop(file);
        let mut file = track!(FileNvm::open(dir.path().join("foo")))?;
        let mut buf = aligned_bytes_with_size(header_len + 3);
        track_io!(file.read_exact(&mut buf[..]))?;
        assert_eq!(&buf[header_len..][..3], b"bar");
        Ok(())
    }

    #[test]
    fn create_if_absent_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let capacity = 10 * 1024;

        // 作成
        assert!(!dir.path().join("foo").exists());
        let (mut file, created) =
            track!(FileNvm::create_if_absent(dir.path().join("foo"), capacity))?;
        assert!(created);

        // `FileNvm`はオープン時にlusfのヘッダがあることを想定しているので先頭に適当なヘッダを書き込む
        let mut data = Vec::new();
        track!(storage_header().write_to(&mut data))?;
        data.resize(512, 7);

        track_io!(file.write_all(&data))?;
        mem::drop(file);

        // オープン
        assert!(dir.path().join("foo").exists());
        let (mut file, created) =
            track!(FileNvm::create_if_absent(dir.path().join("foo"), capacity))?;
        assert!(!created);
        let mut buf = vec![0; 512];
        track_io!(file.read_exact(&mut buf[..]))?;
        assert_eq!(buf, data);
        Ok(())
    }

    #[test]
    fn error_handlings_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let capacity = 1024;

        let mut file = track!(FileNvm::create(dir.path().join("foo"), capacity))?;
        assert!(file.write_all(&aligned_bytes(&[2; 2048][..])).is_err()); // キャパシティ超過
        assert!(
            file.write_all(&aligned_bytes(&[3; 500][..])[..500])
                .is_err()
        ); // アライメントが不正
        Ok(())
    }

    #[test]
    fn nvm_operations_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;

        let mut nvm = track!(FileNvm::create(dir.path().join("foo"), 1024))?;
        assert_eq!(nvm.capacity(), 1024);
        assert_eq!(nvm.position(), 0);

        // read, write, seek
        let mut buf = aligned_bytes_with_size(512);
        track_io!(nvm.read_exact(&mut buf))?;
        assert_eq!(&buf[..], &[0; 512][..]);
        assert_eq!(nvm.position(), 512);

        track_io!(nvm.write(&aligned_bytes(&[1; 512][..])))?;
        assert_eq!(nvm.position(), 1024);

        track_io!(nvm.seek(SeekFrom::Start(512)))?;
        assert_eq!(nvm.position(), 512);

        track_io!(nvm.read_exact(&mut buf))?;
        assert_eq!(&buf[..], &[1; 512][..]);
        assert_eq!(nvm.position(), 1024);

        // split
        let (mut left, mut right) = track!(nvm.split(512))?;

        assert_eq!(left.capacity(), 512);
        track_io!(left.seek(SeekFrom::Start(0)))?;
        track_io!(left.read_exact(&mut buf))?;
        assert_eq!(&buf[..], &[0; 512][..]);
        assert_eq!(left.position(), 512);
        assert!(left.read_exact(&mut buf).is_err());

        assert_eq!(right.capacity(), 512);
        track_io!(right.seek(SeekFrom::Start(0)))?;
        track_io!(right.read_exact(&mut buf))?;
        assert_eq!(&buf[..], &[1; 512][..]);
        assert_eq!(right.position(), 512);
        assert!(right.read_exact(&mut buf).is_err());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn direct_io_flag() -> i32 {
        libc::O_DIRECT
    }
    #[cfg(target_os = "macos")]
    fn direct_io_flag() -> i32 {
        // The following value comes from the following URL
        // https://github.com/apple/darwin-xnu/blob/master/bsd/sys/fcntl.h#L162
        0x4_0000
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn direct_io_flags() -> i32 {
        panic!("Please add an adequate value for your environment");
    }

    #[test]
    fn direct_io_works() -> TestResult {
        use std::os::unix::io::AsRawFd;
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let nvm = track!(FileNvm::create(dir.path().join("foo"), 1024))?;

        let file = nvm.inner();
        let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL, 0) };

        let direct_io_flag = direct_io_flag();

        assert_eq!(status & direct_io_flag, direct_io_flag);
        Ok(())
    }

    #[test]
    fn disabling_direct_io_works() -> TestResult {
        use std::os::unix::io::AsRawFd;
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let nvm = track!(
            FileNvmBuilder::new()
                .direct_io(false)
                .create(dir.path().join("foo"), 1024)
        )?;

        let file = nvm.inner();
        let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL, 0) };

        let direct_io_flag = direct_io_flag();

        assert_eq!(status & direct_io_flag, 0);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn lock_flag() -> i32 {
        libc::O_SHLOCK | libc::O_EXLOCK
    }
    #[cfg(target_os = "macos")]
    fn lock_flag() -> i32 {
        // The following constant comes from
        // https://github.com/apple/darwin-xnu/blob/master/bsd/sys/fcntl.h#L133
        0x4_000
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn lock_flag() -> i32 {
        panic!("Please add an adequate value for your environment");
    }

    #[test]
    fn exclusive_lock_works() -> TestResult {
        use std::os::unix::io::AsRawFd;
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let nvm = track!(FileNvm::create(dir.path().join("foo"), 1024))?;

        let file = nvm.inner();
        let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL, 0) };

        let lock_flag = lock_flag();

        assert_eq!(status & lock_flag, lock_flag);

        Ok(())
    }

    #[test]
    fn disabling_exclusive_lock_works() -> TestResult {
        use std::os::unix::io::AsRawFd;
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let nvm = track!(
            FileNvmBuilder::new()
                .exclusive_lock(false)
                .create(dir.path().join("foo"), 1024)
        )?;

        let file = nvm.inner();
        let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL, 0) };

        let lock_flag = lock_flag();

        assert_eq!(status & lock_flag, 0);

        Ok(())
    }

    fn aligned_bytes<T: AsRef<[u8]>>(b: T) -> AlignedBytes {
        let mut buf = AlignedBytes::from_bytes(b.as_ref(), BlockSize::min());
        buf.align();
        buf
    }

    fn aligned_bytes_with_size(size: usize) -> AlignedBytes {
        aligned_bytes(&vec![0; size][..])
    }

    fn storage_header() -> StorageHeader {
        StorageHeader {
            major_version: MAJOR_VERSION,
            minor_version: MINOR_VERSION,
            block_size: BlockSize::min(),
            instance_uuid: Uuid::new_v4(),
            journal_region_size: 1024,
            data_region_size: 4096,
        }
    }
}
