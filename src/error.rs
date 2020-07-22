use std::{self, str::FromStr};
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
        ErrorKind::Other.cause(e.to_string()).into()
    }
}

/// 発生し得るエラーの種別.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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

    /// 過負荷のため、リクエストはドロップされた.
    ///
    /// # 典型的な対応策
    ///
    /// - 負荷の高い時間を避けてもう一度試す
    RequestDropped,

    /// 過負荷のため、リクエストは拒否された.
    ///
    /// # 典型的な対応策
    ///
    /// - 負荷の高い時間を避けてもう一度試す
    RequestRefused,

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

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::StorageFull => write!(f, "StorageFull"),
            ErrorKind::StorageCorrupted => write!(f, "StorageCorrupted"),
            ErrorKind::DeviceBusy => write!(f, "DeviceBusy"),
            ErrorKind::DeviceTerminated => write!(f, "DeviceTerminated"),
            ErrorKind::InvalidInput => write!(f, "InvalidInput"),
            ErrorKind::InconsistentState => write!(f, "InconsistentState"),
            ErrorKind::RequestDropped => write!(f, "RequestDropped"),
            ErrorKind::RequestRefused => write!(f, "RequestRefused"),
            ErrorKind::Other => write!(f, "Other"),
        }
    }
}

impl FromStr for ErrorKind {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s {
            "StorageFull" => ErrorKind::StorageFull,
            "StorageCorrupted" => ErrorKind::StorageCorrupted,
            "DeviceBusy" => ErrorKind::DeviceBusy,
            "DeviceTerminated" => ErrorKind::DeviceTerminated,
            "InvalidInput" => ErrorKind::InvalidInput,
            "RequestDropped" => ErrorKind::RequestDropped,
            "RequestRefused" => ErrorKind::RequestRefused,
            "InconsistentState" => ErrorKind::InconsistentState,
            "Other" => ErrorKind::Other,
            _ => return Err(()),
        };
        Ok(kind)
    }
}
