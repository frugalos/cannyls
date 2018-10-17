use std;
use trackable;
use trackable::error::ErrorKindExt;

/// crate固有のエラー型.
#[derive(Debug, Clone, TrackableError)]
pub struct Error(trackable::error::TrackableError<ErrorKind>);
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        if let Some(e) = e.get_ref().and_then(|e| e.downcast_ref::<Error>()).cloned() {
            e
        } else if e.kind() == std::io::ErrorKind::InvalidInput {
            ErrorKind::InvalidInput.cause(e).into()
        } else {
            ErrorKind::Other.cause(e).into()
        }
    }
}
impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        if *e.kind() == ErrorKind::InvalidInput {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
        } else {
            std::io::Error::new(std::io::ErrorKind::Other, e)
        }
    }
}
impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        ErrorKind::Other
            .cause(std::error::Error::description(&e))
            .into()
    }
}

/// 発生し得るエラーの種別.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// リクエストキューが詰まっている、等の過負荷状態.
    ///
    /// また、初期化処理中の場合にも、このエラーが返される.
    ///
    /// # 典型的な対応策
    ///
    /// - 利用者が時間をおいてリトライする
    /// - 優先度が低いリクエストの新規発行をしばらく控える
    DeviceBusy,

    /// デバイス(の管理スレッド)が停止しており、利用不可能.
    ///
    /// 正常・異常に関わらず、停止後のデバイスにリクエストが
    /// 発行された場合には、このエラーが返される.
    ///
    /// # 典型的な対応策
    ///
    /// - デバイスを再起動する
    DeviceTerminated,

    /// ストレージに空き容量がない.
    ///
    /// # 典型的な対応策
    ///
    /// - 利用者が不要なlumpを削除する
    /// - ストレージの容量を増やした上で、初期化・再構築を行う
    StorageFull,

    /// ストレージが破損している.
    ///
    /// ジャーナル領域のチェックサム検証が失敗した場合等にこのエラーが返される.
    ///
    /// # 典型的な対応策
    ///
    /// - もし人手で復旧可能な場合には復旧する
    /// - それが無理であれば、諦めて初期化(全削除)を行う
    StorageCorrupted,

    /// 入力が不正.
    ///
    /// # 典型的な対応策
    ///
    /// - 利用者側のプログラムを修正して入力を正しくする
    InvalidInput,

    /// 内部状態が不整合に陥っている.
    ///
    /// プログラムにバグがあることを示している.
    ///
    /// # 典型的な対応策
    ///
    /// - バグ修正を行ってプログラムを更新する
    InconsistentState,

    /// その他エラー.
    ///
    /// E.g., I/Oエラー
    ///
    /// # 典型的な対応策
    ///
    /// - 利用者側で（指数バックオフ等を挟みつつ）何度かリトライ
    ///   - それでもダメなら、致命的な異常が発生していると判断
    Other,
}
impl trackable::error::ErrorKind for ErrorKind {}
