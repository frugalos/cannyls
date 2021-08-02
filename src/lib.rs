//! Canny Lump Storage.
//!
//! `cannyls`は、予測可能なレイテンシの提供を目的として設計された、ローカル用のkey-valueストレージ.
//!
//! # 特徴
//!
//! - 128bitのIDを有する[lump]群を保持するためのストレージ(ローカルKVS)
//!   - メモリ使用量を抑えるために固定長のIDを採用
//! - (主に)HTTPのGET/PUT/DELETE相当の操作を[lump]に対して実行可能
//! - 各操作には[deadline]という時間軸ベースの優先順位を指定可能
//! - 一つの物理デバイス(e.g., HDD)に対して、一つの[device]管理スレッドが割り当てられて、リクエストがスケジューリングされる
//!   - 一つの物理デバイスに対するI/O命令は、全てこの管理スレッド上で直列化されて処理される
//!   - **直列化** という特性上、HDDと相性が良い (逆にSSDの場合には性能が活用しきれない可能性がある)
//! - キャッシュ層を備えず、各操作で発行されるディスクI/O回数が(ほぼ)正確に予測可能:
//!   - GET/DELETE: 一回
//!   - PUT: 最大二回
//!   - ※ 実際にはバックグランド処理(e.g., GC)用のI/Oが発行されることがあるので、上記の値は償却された回数となる
//! - "lusf"という[ストレージフォーマット(v1.0)][format]を定義および使用している
//! - 最大で512TBの容量の物理デバイス(e.g., HDD)をサポート
//! - 冗長化やデータの整合性保証等は行わない
//!
//! # モジュールの依存関係
//!
//! ```text
//! device => storage => nvm
//! ```
//!
//! - [device]モジュール:
//!   - 主に[Device]構造体を提供
//!   - `cannyls`の利用者が直接触るのはこの構造体
//!   - [Storage]を制御するための管理スレッドを起動し、それに対するリクエスト群のスケジューリング等を担当する
//! - [storage]モジュール:
//!   - 主に[Storage]構造体を提供
//!   - [nvm]を永続化層として利用し、その上に[ストレージフォーマット(v1.0)][format]を実装している
//! - [nvm]モジュール:
//!   - 主に[NonVolatileMemory]トレイトとその実装である[FileNvm]を提供
//!   - [storage]に対して永続化層を提供するのが目的
//!   - 現時点では未実装だが、ブロックデバイスを直接操作する[NonVolatileMemory]実装を用意することで、
//!     OS層を完全にバイパスすることも可能
//!
//! # アーキテクチャの詳細
//!
//! [Wiki]を参照のこと。
//!
//! [lump]: ./lump/index.html
//! [deadline]: ./deadline/index.html
//! [device]: ./device/index.html
//! [Device]: ./device/struct.Device.html
//! [storage]: ./storage/index.html
//! [Storage]: ./storage/struct.Storage.html
//! [nvm]: ./nvm/index.html
//! [NonVolatileMemory]: ./nvm/trait.NonVolatileMemory.html
//! [FileNvm]: ./nvm/struct.FileNvm.html
//! [format]: https://github.com/frugalos/cannyls/wiki/Storage-Format
//! [Wiki]: https://github.com/frugalos/cannyls/wiki/
#![warn(missing_docs)]
extern crate adler32;
extern crate byteorder;
extern crate futures;
extern crate libc;
extern crate prometrics;
#[cfg(test)]
extern crate tempdir;
#[macro_use]
extern crate trackable;
extern crate uuid;
#[macro_use]
extern crate slog;

pub use crate::error::{Error, ErrorKind};

macro_rules! track_io {
    ($expr:expr) => {
        $expr.map_err(|e: ::std::io::Error| track!(crate::Error::from(e)))
    };
}

pub mod block;
pub mod deadline;
pub mod device;
pub mod lump;
pub mod metrics;
pub mod nvm;
pub mod storage;

mod error;

/// crate固有の`Result`型.
pub type Result<T> = std::result::Result<T, Error>;
