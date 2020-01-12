use futures::Future;
use std::ops::Range;
use trackable::error::ErrorKindExt;

use super::thread::DeviceThreadHandle;
use deadline::Deadline;
use device::command::{self, Command};
use device::DeviceStatus;
use lump::{LumpData, LumpHeader, LumpId};
use storage::ApproximateUsage;
use {Error, ErrorKind, Result};

/// デバイスに対してリクエストを発行するためのビルダ.
///
/// # 注意
///
/// リクエストを発行した結果返される`Future`を効率的にポーリングするためには
/// [`fibers`]を使用する必要がある。
///
/// [`fibers`]: https://github.com/dwango/fibers-rs
#[derive(Debug)]
pub struct DeviceRequest<'a> {
    device: &'a DeviceThreadHandle,
    deadline: Option<Deadline>,
    max_queue_len: Option<usize>,
    wait_for_running: bool,
    enforce_journal_sync: bool,
}
impl<'a> DeviceRequest<'a> {
    pub(crate) fn new(device: &'a DeviceThreadHandle) -> Self {
        DeviceRequest {
            device,
            deadline: None,
            max_queue_len: None,
            wait_for_running: false,
            enforce_journal_sync: false,
        }
    }

    /// Lumpを格納する.
    ///
    /// 新規追加の場合には`true`が、上書きの場合は`false`が、結果として返される.
    ///
    /// # 性能上の注意
    ///
    /// 引数に渡される`LumpData`が、`LumpData::new`関数経由で生成されている場合には、
    /// デバイスが管理しているストレージへの書き込み時に、
    /// データをストレージのブロック境界にアライメントするためのメモリコピーが余分に発生してしまう.
    /// それを避けたい場合には、`DeviceHandle::allocate_lump_data`メソッドを使用して`LumpData`を生成すると良い.
    pub fn put(
        &self,
        lump_id: LumpId,
        lump_data: LumpData,
    ) -> impl Future<Item = bool, Error = Error> {
        let deadline = self.deadline.unwrap_or_default();
        let (command, response) =
            command::PutLump::new(lump_id, lump_data, deadline, self.enforce_journal_sync);
        self.send_command(Command::Put(command));
        response
    }

    /// Lumpを取得する.
    pub fn get(&self, lump_id: LumpId) -> impl Future<Item = Option<LumpData>, Error = Error> {
        let deadline = self.deadline.unwrap_or_default();
        let (command, response) = command::GetLump::new(lump_id, deadline);
        self.send_command(Command::Get(command));
        response
    }

    /// Lumpのヘッダを取得する.
    pub fn head(&self, lump_id: LumpId) -> impl Future<Item = Option<LumpHeader>, Error = Error> {
        let deadline = self.deadline.unwrap_or_default();
        let (command, response) = command::HeadLump::new(lump_id, deadline);
        self.send_command(Command::Head(command));
        response
    }

    /// Lumpを削除する.
    ///
    /// 指定されたlumpが存在した場合には`true`が、しなかった場合には`false`が、結果として返される.
    pub fn delete(&self, lump_id: LumpId) -> impl Future<Item = bool, Error = Error> {
        let deadline = self.deadline.unwrap_or_default();
        let (command, response) =
            command::DeleteLump::new(lump_id, deadline, self.enforce_journal_sync);
        self.send_command(Command::Delete(command));
        response
    }

    /// Lumpを範囲オブジェクトを用いて削除する.
    ///
    /// 返り値のvectorは、引数rangeに含まれるlump idのうち、
    /// 対応するlump dataが存在して実際に削除されたもの全体を表す。
    pub fn delete_range(
        &self,
        range: Range<LumpId>,
    ) -> impl Future<Item = Vec<LumpId>, Error = Error> {
        let deadline = self.deadline.unwrap_or_default();
        let (command, response) =
            command::DeleteLumpRange::new(range, deadline, self.enforce_journal_sync);
        self.send_command(Command::DeleteRange(command));
        response
    }

    /// 保存されているlump一覧を取得する.
    ///
    /// # 注意
    ///
    /// 例えば巨大なHDDを使用している場合には、lumpの数が数百万以上になることもあるため、
    /// このメソッドは呼び出す際には注意が必要.
    pub fn list(&self) -> impl Future<Item = Vec<LumpId>, Error = Error> {
        let deadline = self.deadline.unwrap_or_default();
        let (command, response) = command::ListLump::new(deadline);
        self.send_command(Command::List(command));
        response
    }

    /// 範囲を指定してlump一覧を取得する.
    ///
    pub fn list_range(
        &self,
        range: Range<LumpId>,
    ) -> impl Future<Item = Vec<LumpId>, Error = Error> {
        let deadline = self.deadline.unwrap_or_default();
        let (command, response) = command::ListLumpRange::new(range, deadline);
        self.send_command(Command::ListRange(command));
        response
    }

    /// 範囲を指定してlump数を取得する.
    ///
    pub fn usage_range(
        &self,
        range: Range<LumpId>,
    ) -> impl Future<Item = ApproximateUsage, Error = Error> {
        let deadline = self.deadline.unwrap_or_default();
        let (command, response) = command::UsageLumpRange::new(range, deadline);
        self.send_command(Command::UsageRange(command));
        response
    }

    /// デバイスを停止する.
    ///
    /// 停止は重要な操作であり、実行は`Device`インスタンスの保持者に制限したいので、
    /// このメソッドは`crate`のみを公開範囲とする.
    pub(crate) fn stop(&self) {
        let deadline = self.deadline.unwrap_or_default();
        let command = command::StopDevice::new(deadline);
        self.send_command(Command::Stop(command));
    }

    /// 要求のデッドラインを設定する.
    ///
    /// デフォルト値は`Deadline::Infinity`.
    pub fn deadline(&mut self, deadline: Deadline) -> &mut Self {
        self.deadline = Some(deadline);
        self
    }

    /// [ジャーナルバッファ]をディスクへ書き出す。
    ///
    /// [ジャーナルバッファ]は[journal_sync_interval]に基づき
    /// 自動でディスク上に書き出されるが、
    /// このメソッドを呼ぶことで自動書き出しを待たずに
    /// その場での書き出しを強制することができる。
    ///
    /// [journal_sync_interval]: ../storage/struct.StorageBuilder.html#method.journal_sync_interval
    /// [ジャーナルバッファ]: https://github.com/frugalos/cannyls/wiki/Journal-Memory-Buffer
    pub fn journal_sync(&mut self) -> &mut Self {
        self.enforce_journal_sync = true;
        self
    }

    /// デバイスのキューの最大長を指定する.
    ///
    /// もし要求発行時に、デバイスのキューの長さがこの値を超えている場合には、
    /// `ErrorKind::DeviceBusy`エラーが返される.
    ///
    /// デフォルトは無制限.
    pub fn max_queue_len(&mut self, max: usize) -> &mut Self {
        self.max_queue_len = Some(max);
        self
    }

    /// デバイスが起動処理中の場合には、その完了を待つように指示する.
    ///
    /// デフォルトでは、起動処理中にリクエストが発行された場合には、
    /// 即座に`ErrorKind::DeviceBusy`エラーが返される.
    ///
    /// `wait_for_running()`が呼び出された場合には、
    /// リクエストはキューに追加され、デバイス起動後に順次処理される.
    pub fn wait_for_running(&mut self) -> &mut Self {
        self.wait_for_running = true;
        self
    }

    fn send_command(&self, command: Command) {
        if !self.wait_for_running && self.device.metrics().status() == DeviceStatus::Starting {
            let e = track!(ErrorKind::DeviceBusy.cause("The device is starting up"));
            command.failed(e.into());
            return;
        }
        if let Err(e) = track!(self.check_limit()) {
            self.device.metrics().busy_commands.increment(&command);
            command.failed(e)
        } else {
            self.device.send_command(command);
        }
    }

    fn check_limit(&self) -> Result<()> {
        let metrics = self.device.metrics();
        if let Some(max) = self.max_queue_len {
            track_assert!(
                metrics.queue_len() <= max,
                ErrorKind::DeviceBusy,
                "value={}, max={}",
                metrics.queue_len(),
                max
            );
        }
        Ok(())
    }
}
