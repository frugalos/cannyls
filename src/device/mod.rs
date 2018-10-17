//! Lump群を格納用のデバイス.
//!
//! "デバイス"は[ストレージ]の管理を目的とした構成要素であり、
//! 典型的には、一つの物理デバイス(e.g., HDD)に対して一つの[Device]インスタンスが存在することになる.
//!
//! 一つのデバイス(i.e., ストレージ)には、一つの管理スレッドが割り当てられて、
//! そのデバイスに対するリクエストは全て直列化されて処理される.
//!
//! 並行するリクエスト群が存在する場合には、指定された優先順位(デッドライン)に基づいて
//! スケジューリングが行われる.
//!
//! [ストレージ]: ../storage/index.html
//! [Device]: struct.Device.html
use futures::{Async, Future, Poll};
use std::sync::Arc;

pub use self::builder::DeviceBuilder;
pub use self::request::DeviceRequest;

pub(crate) use self::command::Command; // `metrics`モジュール用に公開されている

use self::thread::{DeviceThreadHandle, DeviceThreadMonitor};
use deadline::Deadline;
use lump::{LumpData, LumpId};
use metrics::DeviceMetrics;
use nvm::NonVolatileMemory;
use storage::Storage;
use {Error, Result};

mod builder;
mod command;
mod queue;
mod request;
mod thread;

/// [Lump]群を格納するためのデバイス.
///
/// [モジュールドキュメント](index.html)も参照のこと.
///
/// # Future実装
///
/// `Device`は[Future]を実装している.
///
/// 実際の処理は、別スレッドで実行されるため`Future::poll`を呼び出さなくても進行上は支障はないが、
/// このメソッドによりデバイス(スレッド)の終了(正常ないし異常)を検知することが可能となる.
///
/// なお`Device`インスタンスが破棄されると、裏で動いているデバイス用のOSスレッドも停止させられるので、
/// `Future::poll`を呼び出さない場合でも、インスタンス自体は保持しておく必要がある.
///
/// [Lump]: ../lump/index.html
/// [Future]: https://docs.rs/futures/0.1/futures/future/trait.Future.html
#[must_use]
#[derive(Debug)]
pub struct Device {
    monitor: DeviceThreadMonitor,
    handle: DeviceHandle,
    is_stopped: bool,
}
impl Device {
    /// デフォルト設定でデバイスを起動する.
    ///
    /// 設定を変更したい場合には`DeviceBuilder`を使用すること.
    pub fn spawn<F, N>(&self, init_storage: F) -> Device
    where
        F: FnOnce() -> Result<Storage<N>> + Send + 'static,
        N: NonVolatileMemory + Send + 'static,
    {
        DeviceBuilder::new().spawn(init_storage)
    }

    /// デバイスを操作するためのハンドルを返す.
    pub fn handle(&self) -> DeviceHandle {
        self.handle.clone()
    }

    /// デバイスに停止リクエストを発行する.
    ///
    /// このメソッドが返った時点でデバイスが停止している保証はないので、
    /// 確実に終了を検知したい場合には`Future::poll`メソッド経由で知る必要がある.
    ///
    /// なお`Device`インスタンスのドロップ時点で、そのデバイスがまだ稼働中の場合には
    /// `stop(Deadline::Immediate)`が自動で呼び出される.
    /// ただし、その後にデバイスの終了を待機したりはしないので注意は必要.
    /// 例えば「`Device`インスタンスをドロップして、直後に同じ設定でデバイスを起動」といったことを
    /// 行った場合には、停止中の旧インスタンスと起動した新インスタンスでリソース(e.g., ファイル)が
    /// 衝突し、エラーが発生するかもしれない.
    /// 確実な終了検知が必要なら、アプリケーションが明示的に`Device::stop`を呼び出す必要がある.
    pub fn stop(&self, deadline: Deadline) {
        self.handle().request().deadline(deadline).stop();
    }

    /// デバイスの起動を待機するための`Future`を返す.
    pub fn wait_for_running(self) -> impl Future<Item = Self, Error = Error> {
        let handle = self.handle();
        let future = handle.request().wait_for_running().head(LumpId::new(0)); // IDは何でも良い
        track_err!(future.map(move |_| self))
    }

    pub(crate) fn new(monitor: DeviceThreadMonitor, handle: DeviceHandle) -> Self {
        Device {
            monitor,
            handle,
            is_stopped: false,
        }
    }
}
impl Future for Device {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = track!(self.monitor.poll());
        if let Ok(Async::NotReady) = result {
        } else {
            self.is_stopped = true;
        }
        result
    }
}
impl Drop for Device {
    fn drop(&mut self) {
        if !self.is_stopped {
            self.stop(Deadline::Immediate);
        }
    }
}

/// デバイスを操作するためのハンドル.
#[derive(Debug, Clone)]
pub struct DeviceHandle(DeviceThreadHandle);
impl DeviceHandle {
    /// デバイスの発行するリクエストのビルダを返す.
    pub fn request(&self) -> DeviceRequest {
        DeviceRequest::new(&self.0)
    }

    /// デバイスのメトリクスを返す.
    pub fn metrics(&self) -> &Arc<DeviceMetrics> {
        self.0.metrics()
    }

    /// ストレージのブロック境界にアライメントされたメモリ領域を保持する`LumpData`インスタンスを返す.
    ///
    /// `LumpData::new`関数に比べて、このメソッドが返した`LumpData`インスタンスは、
    /// デバイスが管理しているストレージのブロック境界に合わせたアライメントが行われているため、
    /// ストレージへのPUT時に余計なメモリコピーが発生することがなく、より効率的となる.
    ///
    /// # 注意
    ///
    /// このメソッドが返した`LumpData`インスタンスを、別の(ブロックサイズが異なる)ストレージに
    /// 保存しようとした場合には、エラーが発生する.
    ///
    /// # Errors
    ///
    /// 指定されたサイズが`MAX_SIZE`を超えている場合は、`ErrorKind::InvalidInput`エラーが返される.
    pub fn allocate_lump_data(&self, size: usize) -> Result<LumpData> {
        if let Some(storage) = self.metrics().storage() {
            track!(LumpData::aligned_allocate(
                size,
                storage.header().block_size
            ))
        } else {
            // 「デバイスが起動中」ないし「デバイスが停止済み」の場合には、
            // ストレージのメトリクスが取得できずに、ここに来ることがある.
            //
            // その場合には`LumpData::new`を使って、アライメントされていない
            // メモリ領域が割り当てられることになるが、上記のケースでは、
            // 生成された`LumpData`インスタンスは、いずれにせよ
            // (たいていは)ストレージに保存されずに単に捨てられるだけであり、
            // 追加のアライメント処理が走ることもないので、
            // 事前にアライメントを行っていなくても問題ない.
            let mut data = Vec::with_capacity(size);
            unsafe {
                data.set_len(size);
            }
            track!(LumpData::new(data))
        }
    }

    /// `allocate_lump_data`メソッドにデータの初期化を加えたメソッド.
    ///
    /// このメソッドの呼び出しは、以下のコードと等価となる:
    /// ```ignore
    /// let mut data = track!(self.allocate_lump_data(bytes.len()))?;
    /// data.as_bytes_mut().copy_from_slice(bytes);
    /// ```
    ///
    /// 詳細な挙動に関しては`allocate_lump_data`のドキュメントを参照のこと.
    pub fn allocate_lump_data_with_bytes(&self, bytes: &[u8]) -> Result<LumpData> {
        let mut data = track!(self.allocate_lump_data(bytes.len()))?;
        data.as_bytes_mut().copy_from_slice(bytes);
        Ok(data)
    }
}

/// デバイスの稼働状態.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeviceStatus {
    /// デバイスは起動中.
    ///
    /// 具体的には、デバイスの管理スレッドがストレージの初期化(生成)関数を呼び出しているところ.
    Starting = 1,

    /// デバイスは稼働中.
    ///
    /// デバイスに対して発行された各種要求を処理可能な状態.
    Running = 2,

    /// デバイスは停止済.
    ///
    /// デバイスが正常ないし異常に終了し、管理スレッドも回収されている.
    Stopped = 0,
}

#[cfg(test)]
mod tests {
    use fibers_global::execute;
    use std::ops::Range;
    use trackable::result::TestResult;

    use super::*;
    use lump::{LumpData, LumpId};
    use nvm::{MemoryNvm, SharedMemoryNvm};
    use storage::StorageBuilder;

    #[test]
    fn device_works() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024 * 1024]);
        let storage = track!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm))?;
        let device = DeviceBuilder::new().spawn(|| Ok(storage));
        let d = device.handle();
        let _ = execute(d.request().wait_for_running().list()); // デバイスの起動を待機

        track!(execute(d.request().put(id(0), data(b"foo"))))?;
        track!(execute(d.request().put(id(1), data(b"bar"))))?;
        track!(execute(d.request().put(id(2), data(b"baz"))))?;
        assert_eq!(
            track!(execute(d.request().list()))?,
            vec![id(0), id(1), id(2)]
        );

        assert_eq!(track!(execute(d.request().delete(id(1))))?, true);
        assert_eq!(track!(execute(d.request().delete(id(1))))?, false);
        assert_eq!(track!(execute(d.request().list()))?, vec![id(0), id(2)]);
        Ok(())
    }

    #[test]
    fn delete_range_all_data_works() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024 * 1024]);
        let storage = track!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm))?;
        let device = DeviceBuilder::new().spawn(|| Ok(storage));
        let d = device.handle();
        let _ = execute(d.request().wait_for_running().list()); // デバイスの起動を待機

        track!(execute(d.request().put(id(0), data(b"foo"))))?;
        track!(execute(d.request().put(id(1), data(b"bar"))))?;
        track!(execute(d.request().put(id(2), data(b"baz"))))?;
        assert_eq!(
            track!(execute(d.request().list()))?,
            vec![id(0), id(1), id(2)]
        );

        assert_eq!(
            track!(execute(d.request().delete_range(Range {
                start: id(0),
                end: id(3)
            })))?,
            vec![id(0), id(1), id(2)]
        );
        assert_eq!(track!(execute(d.request().list()))?, vec![]);
        Ok(())
    }

    #[test]
    fn delete_range_no_data_works() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024 * 1024]);
        let storage = track!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm))?;
        let device = DeviceBuilder::new().spawn(|| Ok(storage));
        let d = device.handle();
        let _ = execute(d.request().wait_for_running().list()); // デバイスの起動を待機

        track!(execute(d.request().put(id(0), data(b"foo"))))?;
        track!(execute(d.request().put(id(1), data(b"bar"))))?;
        track!(execute(d.request().put(id(2), data(b"baz"))))?;
        assert_eq!(
            track!(execute(d.request().list()))?,
            vec![id(0), id(1), id(2)]
        );

        assert_eq!(
            track!(execute(d.request().delete_range(Range {
                start: id(3),
                end: id(9)
            })))?,
            vec![]
        );
        assert_eq!(
            track!(execute(d.request().list()))?,
            vec![id(0), id(1), id(2)]
        );
        Ok(())
    }

    #[test]
    fn delete_range_partial_data_works() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024 * 1024]);
        let storage = track!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm))?;
        let device = DeviceBuilder::new().spawn(|| Ok(storage));
        let d = device.handle();
        let _ = execute(d.request().wait_for_running().list()); // デバイスの起動を待機

        track!(execute(d.request().put(id(0), data(b"foo"))))?;
        track!(execute(d.request().put(id(1), data(b"bar"))))?;
        track!(execute(d.request().put(id(2), data(b"baz"))))?;
        track!(execute(d.request().put(id(3), data(b"hoge"))))?;
        assert_eq!(
            track!(execute(d.request().list()))?,
            vec![id(0), id(1), id(2), id(3)]
        );

        assert_eq!(
            track!(execute(d.request().delete_range(Range {
                start: id(1),
                end: id(3)
            })))?,
            vec![id(1), id(2)]
        );
        assert_eq!(track!(execute(d.request().list()))?, vec![id(0), id(3)]);
        Ok(())
    }

    #[test]
    fn list_range_works() -> TestResult {
        let nvm = MemoryNvm::new(vec![0; 1024 * 1024]);
        let storage = track!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm))?;
        let device = DeviceBuilder::new().spawn(|| Ok(storage));
        let d = device.handle();
        let _ = execute(d.request().wait_for_running().list()); // デバイスの起動を待機

        // PUT
        for i in 2..7 {
            track!(execute(
                d.request().put(id(i), data(i.to_string().as_bytes()))
            ))?;
        }
        assert_eq!(
            track!(execute(d.request().list()))?,
            vec![id(2), id(3), id(4), id(5), id(6)]
        );

        // 範囲取得: 重複範囲無し
        assert_eq!(
            track!(execute(d.request().list_range(Range {
                start: id(0),
                end: id(2)
            })))?,
            vec![]
        );

        // 範囲取得: 部分一致
        assert_eq!(
            track!(execute(d.request().list_range(Range {
                start: id(1),
                end: id(5)
            })))?,
            vec![id(2), id(3), id(4)]
        );

        // 範囲取得: 部分集合
        assert_eq!(
            track!(execute(d.request().list_range(Range {
                start: id(3),
                end: id(4)
            })))?,
            vec![id(3)]
        );

        // 範囲取得: 上位集合 (全lump取得)
        assert_eq!(
            track!(execute(d.request().list_range(Range {
                start: id(0),
                end: id(10000)
            })))?,
            vec![id(2), id(3), id(4), id(5), id(6)]
        );

        Ok(())
    }

    fn id(id: usize) -> LumpId {
        LumpId::new(id as u128)
    }

    fn data(data: &[u8]) -> LumpData {
        LumpData::new(Vec::from(data)).unwrap()
    }

    fn embedded_data(data: &[u8]) -> LumpData {
        LumpData::new_embedded(Vec::from(data)).unwrap()
    }

    #[test]
    fn journal_sync_works() -> TestResult {
        {
            let nvm = SharedMemoryNvm::new(vec![0; 1024 * 1024]);
            let storage = track!(
                StorageBuilder::new()
                    .journal_region_ratio(0.99)
                    .create(nvm.clone())
            )?;
            let v = nvm.to_bytes();
            let device = DeviceBuilder::new().spawn(|| Ok(storage));
            let d = device.handle();
            let _ = execute(d.request().wait_for_running().list());
            track!(execute(d.request().put(id(1234), embedded_data(b"hoge"))));
            assert_eq!(v, nvm.to_bytes()); // ジャーナルバッファ上に値があり、実際に書き込まれていない
        }

        {
            let nvm = SharedMemoryNvm::new(vec![0; 4 * 1024]);
            let storage = track!(
                StorageBuilder::new()
                    .journal_region_ratio(0.5)
                    .create(nvm.clone())
            )?;
            let v = nvm.to_bytes();
            let device = DeviceBuilder::new().spawn(|| Ok(storage));
            let d = device.handle();
            let _ = execute(d.request().wait_for_running().list()); // デバイスの起動を待機
            track!(execute(
                d.request()
                    .journal_sync()
                    .put(id(1234), embedded_data(b"hoge"))
            ));
            assert_ne!(v, nvm.to_bytes()); // `journal_sync` により、実際に書き込まれている
        }

        Ok(())
    }
}
