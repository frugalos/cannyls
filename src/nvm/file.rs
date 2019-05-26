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
/// `FileNvm`には二つのオプション`direct_io`と`exclusive_lock`が存在する。  
/// デフォルトでは`direct_io=true`かつ`exclusive_lock=true`の振る舞いをする。  
/// それぞれのオプション内容については個別のメソッドを参照せよ。
pub struct FileNvmBuilder {
    direct_io: bool,
    exclusive_lock: bool,
}

impl Default for FileNvmBuilder {
    fn default() -> Self {
        FileNvmBuilder {
            direct_io: true,
            exclusive_lock: true,
        }
    }
}

impl FileNvmBuilder {
    /// デフォルト設定で`FileNvmBuilder`インスタンスを作成する
    pub fn new() -> Self {
        FileNvmBuilder::default()
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

    /// Direct I/O（バッファリングなしIO）を行うかどうかを設定する。  
    /// デフォルトではDirect I/Oを行う。
    /// - `enabled=true`でDirect I/Oを行う。
    /// - `enabled=false`でDirect I/Oを行わない。
    ///
    /// 現状ではLinuxとMacのみで有効なオプションで、それぞれ次を意味する:
    /// - Linux: O_DIRECTオプションでファイルを開く。
    /// - Mac: ファイルを開いた後にF_NOCACHEオプションを付与する。
    pub fn direct_io(&mut self, enabled: bool) -> &mut Self {
        self.direct_io = enabled;
        self
    }

    /// ファイルに対する排他ロックを行うかどうかを設定する。  
    /// デフォルトでは排他ロックを行う。
    /// - `enabled=true`で排他ロックを行う。
    /// - `enabled=false`で排他ロックを行わない。
    ///
    /// 現状ではUnix系で有効なオプションで、次を意味する:  
    /// - `LOCK_EX`と`LOCK_NB`オプションを用いて`flock`システムコールを呼び出す。
    pub fn exclusive_lock(&mut self, enabled: bool) -> &mut Self {
        self.exclusive_lock = enabled;
        self
    }

    #[cfg(target_os = "linux")]
    fn file_open_with_error_info<P: AsRef<Path>>(
        &self,
        do_create: bool, // This denoes whether or not the next argument `options` allows file creating.
        options: &fs::OpenOptions,
        filepath: &P,
    ) -> Result<File> {
        use std::os::unix::fs::OpenOptionsExt;

        let open_result = track_io!(options.open(&filepath));

        // If we succeed on opening the file `filepath`, we return it.
        if open_result.is_ok() {
            return open_result;
        }

        // Otherwise, we check why opening the file `filepath` failed.
        // First, we check the existence of the file `filepath`.
        if !std::path::Path::new(filepath.as_ref()).exists() {
            if do_create {
                // failed to file open
                return track!(
                    open_result,
                    "We failed to create the file {:?}.",
                    filepath.as_ref()
                );
            } else {
                // `do_create == false` means to open an existing file;
                // however, now the file `filepath` does not exist.
                return track!(
                    open_result,
                    "The file {:?} does not exist and failed to open it.",
                    filepath.as_ref()
                );
            }
        }

        // Next, we check if the file `filepath` can be opened without `O_DIRECT` option.
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(false);

        let file = track_io!(options.open(&filepath));
        if file.is_err() {
            return track!(file, "We cannot open the file {:?}.", filepath.as_ref());
        }

        // Finally, we check if the file `filepath` can be opened with `O_DIRECT` option.
        if self.direct_io {
            options.custom_flags(libc::O_DIRECT);
            let file = track_io!(options.open(&filepath));
            if file.is_err() {
                return track!(
                    file,
                    "We cannot open the file {:?} with O_DIRECT.",
                    filepath.as_ref()
                );
            }
        }

        // Strange case; so, we return the originanl error information.
        open_result
    }

    #[cfg(not(target_os = "linux"))]
    fn file_open_with_error_info<P: AsRef<Path>>(
        &self,
        _do_create: bool,
        options: &fs::OpenOptions,
        filepath: &P,
    ) -> Result<File> {
        track_io!(options.open(&filepath))
    }

    /// 新しい`FileNvm`インスタンスを生成する.
    ///
    /// `filepath`が既に存在する場合にはそれを開き、存在しない場合には新規にファイルを作成する.
    ///
    /// 返り値のタプルの二番目のbool値は、次を意味する
    /// - `true`: ファイルが新規作成された、または既にファイルが存在したがそれが0バイトの空ファイルである。
    /// - `false`: 非空なファイルが存在していた。
    pub fn create_if_absent<P: AsRef<Path>>(
        &mut self,
        filepath: P,
        capacity: u64,
    ) -> Result<(FileNvm, bool)> {
        create_parent_directories(&filepath)?;
        let mut options = self.open_options();
        // OpenOptions::createはファイルが既に存在する場合はそれを開き
        // 存在しない場合は作成する
        options.create(true);
        let file = track!(self.file_open_with_error_info(true, &options, &filepath))?;

        // metadataのファイルサイズの非ゼロ検査で
        // 新規作成されたファイルかどうかを判断する
        let metadata = track_io!(fs::metadata(&filepath))?;
        if metadata.len() == 0 {
            // ファイルが新しく作成された
            self.initialize(file, capacity).map(|s| (s, true))
        } else {
            // 既に存在するファイルなので、格納されているcapacity値を使う
            let saved_header = track!(StorageHeader::read_from_file(&filepath))?;
            let capacity = saved_header.storage_size();
            self.initialize(file, capacity).map(|s| (s, false))
        }
    }

    /// ファイルを新規に作成して`FileNvm`インスタンスを生成する.
    ///
    /// `filepath`にファイルが存在する場合にはエラーを返す。  
    /// `filepath`に（非零バイト）ファイルが存在する場合にそれを開きたいならば、
    /// このメソッドの代わりに`create_if_absent`を用いる。
    pub fn create<P: AsRef<Path>>(&mut self, filepath: P, capacity: u64) -> Result<FileNvm> {
        create_parent_directories(&filepath)?;
        let mut options = self.open_options();
        // OpenOptions::create_newはファイルが存在しない場合だけ作成し
        // 存在しない場合はエラーとなる。
        options.create_new(true);
        let file = self.file_open_with_error_info(true, &options, &filepath)?;
        self.initialize(file, capacity)
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
        let file = self.file_open_with_error_info(false, &options, &filepath)?;
        self.initialize(file, capacity)
    }

    fn initialize(&self, file: File, capacity: u64) -> Result<FileNvm> {
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

    fn with_range(file: File, start: u64, end: u64) -> Self {
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

    #[cfg(test)]
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

/// 親ディレクトリの作成が必要な場合は作成する。
fn create_parent_directories<P: AsRef<Path>>(filepath: P) -> Result<()> {
    if let Some(dir) = filepath.as_ref().parent() {
        track_io!(fs::create_dir_all(dir))?;
    }
    Ok(())
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
    fn create_parent_directories_is_idempotent() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let filepath = dir.path().join("dir1").join("file1");

        assert!(create_parent_directories(&filepath).is_ok());
        assert!(create_parent_directories(&filepath).is_ok());

        Ok(())
    }

    #[test]
    fn create_parent_directories_creates_parent_directories() -> TestResult {
        let root = track_io!(TempDir::new("cannyls_test1"))?.into_path();
        let sub_dirs = vec!["dir1", "dir2", "dir3"];
        let filepath = root.join("dir1").join("dir2").join("dir3").join("1.lusf");

        // 作成前は存在しない
        let mut dir = root.clone();
        for sub_dir in &sub_dirs {
            dir = dir.join(sub_dir);
            assert!(!dir.exists());
        }

        create_parent_directories(filepath)?;

        // 作成後は存在する
        let mut dir = root;
        for sub_dir in &sub_dirs {
            dir = dir.join(sub_dir);
            assert!(dir.exists());
        }

        Ok(())
    }

    #[test]
    fn create_parent_directories_does_nothing_if_there_is_no_parent() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test1"))?;
        let mut parent = dir.as_ref();
        while let Some(p) = parent.parent() {
            parent = p;
        }
        assert!(create_parent_directories(parent).is_ok());
        Ok(())
    }

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
        // 最小ブロックサイズに切り上げる。
        // 切り上げるために定数7をpaddingする。
        data.resize(BlockSize::MIN as usize, 7);

        track_io!(file.write_all(&aligned_bytes(&data[..])))?;
        mem::drop(file);

        // オープン
        assert!(dir.path().join("foo").exists());
        let (mut file, created) =
            track!(FileNvm::create_if_absent(dir.path().join("foo"), capacity))?;
        assert!(!created);
        let mut buf = aligned_bytes_with_size(data.len());
        track_io!(file.read_exact(&mut buf[..]))?;
        assert_eq!(buf.as_ref(), &data[..]);
        Ok(())
    }

    #[test]
    fn create_if_absent_must_create_parent_directories() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let capacity = 10 * 1024;
        let filepath = dir.path().join("foo").join("bar").join("buzz");
        let parent = track_io!(filepath.parent().ok_or(io::Error::new(
            io::ErrorKind::NotFound,
            "Parent directory must be present"
        )))?;
        assert!(!parent.exists());
        assert!(!filepath.exists());
        let (_, created) = track!(FileNvm::create_if_absent(&filepath, capacity))?;
        assert!(created);
        assert!(parent.exists());
        Ok(())
    }

    #[test]
    fn error_handlings_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let capacity = 1024;

        let mut file = track!(FileNvm::create(dir.path().join("foo"), capacity))?;
        assert!(file.write_all(&aligned_bytes(&[2; 2048][..])).is_err()); // キャパシティ超過
        assert!(file
            .write_all(&aligned_bytes(&[3; 500][..])[..500])
            .is_err()); // アライメントが不正
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

    #[cfg_attr(any(target_os = "linux", target_os = "macos"), test)]
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

    #[cfg_attr(any(target_os = "linux", target_os = "macos"), test)]
    fn disabling_direct_io_works() -> TestResult {
        use std::os::unix::io::AsRawFd;
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let nvm = track!(FileNvmBuilder::new()
            .direct_io(false)
            .create(dir.path().join("foo"), 1024))?;

        let file = nvm.inner();
        let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL, 0) };

        let direct_io_flag = direct_io_flag();

        assert_eq!(status & direct_io_flag, 0);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn is_file_lock<F: AsRef<Path>>(_file: &File, path: F) -> bool {
        use std::process::Command;

        // try to get an exclusive file lock
        let status = Command::new("/bin/bash")
            .arg("-c")
            .arg(format!(
                "/usr/bin/flock -e -n {} -c echo",
                path.as_ref().to_str().unwrap()
            ))
            .status()
            .expect("failed to execute process");

        !status.success()
    }
    #[cfg(target_os = "macos")]
    fn is_file_lock<F: AsRef<Path>>(file: &File, _path: F) -> bool {
        use std::os::unix::io::AsRawFd;
        let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL, 0) };

        // The following constant comes from
        // https://github.com/apple/darwin-xnu/blob/master/bsd/sys/fcntl.h#L133
        let lock_flag = 0x4_000;

        (status & lock_flag) == lock_flag
    }

    #[cfg_attr(any(target_os = "linux", target_os = "macos"), test)]
    fn exclusive_lock_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let file_path = dir.path().join("foo");
        let nvm = track!(FileNvm::create(&file_path, 1024))?;
        let file = nvm.inner();

        assert!(is_file_lock(file, file_path));

        Ok(())
    }

    #[cfg_attr(any(target_os = "linux", target_os = "macos"), test)]
    fn disabling_exclusive_lock_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let file_path = dir.path().join("bar");
        let nvm = track!(FileNvmBuilder::new()
            .exclusive_lock(false)
            .create(&file_path, 1024))?;
        let file = nvm.inner();

        assert!(!is_file_lock(file, file_path));

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
