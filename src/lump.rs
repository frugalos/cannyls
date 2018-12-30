//! Lump関連のデータ構造群.
//!
//! "lump"とは、`cannyls`におけるデータの格納単位.
//! 各lumpは「128bit幅のID」と「最大30MB程度のデータ(任意のバイト列)」から構成される.
//!
//! `cannyls`のレイヤでは、保存されているlumpの整合性の保証や検証は行わないため、
//! 必要であれば、利用側で冗長化やチェックサム検証等を施す必要がある.
use std::cmp;
use std::fmt;
use std::str::FromStr;
use std::u128;
use trackable::error::ErrorKindExt;

use block::BlockSize;
use storage::DataRegionLumpData;
use {Error, ErrorKind, Result};

/// Lumpの識別子(128bit幅).
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct LumpId(u128);
impl LumpId {
    /// 識別子のバイト幅.
    pub const SIZE: usize = 16;

    /// 新しい`LumpId`インスタンスを生成する.
    ///
    /// # Examples
    ///
    /// ```
    /// use cannyls::lump::LumpId;
    ///
    /// assert_eq!(LumpId::new(0x12_3456).to_string(), "00000000000000000000000000123456");
    ///
    /// // 16進数文字列からも生成可能
    /// assert_eq!("123456".parse::<LumpId>().unwrap(), LumpId::new(0x12_3456));
    /// ```
    pub fn new(id: u128) -> Self {
        LumpId(id)
    }

    /// 識別子の値(128bit整数)を返す.
    pub fn as_u128(&self) -> u128 {
        self.0
    }
}
impl FromStr for LumpId {
    type Err = Error;

    /// 16進数表記の数値から`LumpId`を生成する.
    ///
    /// 数値の"128bit整数"として扱われ、先頭のゼロは省略可能（`"ab12"`と`"00ab12"`は等価）.
    ///
    /// # Errors
    ///
    /// 以下のいずれかの場合には、種類が`ErrorKind::InvalidInput`のエラーが返される:
    ///
    /// - 文字列が16進数表記の整数値を表していない
    /// - 文字列の長さが32文字を超えている
    ///
    /// # Examples
    ///
    /// ```
    /// use std::str::{self, FromStr};
    /// use cannyls::ErrorKind;
    /// use cannyls::lump::LumpId;
    ///
    /// assert_eq!(LumpId::from_str("00ab12").ok(),
    ///            Some(LumpId::new(0xab12)));
    ///
    /// assert_eq!(LumpId::from_str("foo_bar").err().map(|e| *e.kind()),
    ///            Some(ErrorKind::InvalidInput));
    ///
    /// let large_input = str::from_utf8(&[b'a', 33][..]).unwrap();
    /// assert_eq!(LumpId::from_str(large_input).err().map(|e| *e.kind()),
    ///            Some(ErrorKind::InvalidInput));
    /// ```
    fn from_str(s: &str) -> Result<Self> {
        let id = track!(u128::from_str_radix(s, 16).map_err(|e| ErrorKind::InvalidInput.cause(e)))?;
        Ok(LumpId::new(id))
    }
}
impl fmt::Debug for LumpId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, r#"LumpId("{}")"#, self)
    }
}
impl fmt::Display for LumpId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in (0..Self::SIZE).rev() {
            let b = (self.0 >> (8 * i)) as u8;
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

/// Lumpのデータ.
///
/// 最大で`MAX_SIZE`までのバイト列を保持可能.
#[derive(Clone)]
pub struct LumpData(LumpDataInner);
impl LumpData {
    /// データの最大長（バイト単位）.
    ///
    /// 最小ブロックサイズを用いた場合に表現可能な最大サイズまでのデータが保持可能.
    /// 最後の`-2`は、内部的に付与されるメタ情報のサイズ分.
    ///
    /// # 蛇足
    ///
    /// 現状は簡単のために、最小のブロックサイズに合わせた最大サイズ、となっている。
    ///
    /// ただし、仕組み上は、ストレージが採用したブロックサイズがそれよりも大きい場合には、
    /// 保存可能なデータサイズを比例して大きくすることは可能.
    ///
    /// 一つ一つのlumpのサイズをあまり巨大にしてしまうと、
    /// 一つのlumpの読み書き処理が全体のレイテンシを阻害してしまう可能性もあるので、
    /// 現状くらいの制限でちょうど良いのではないかとも思うが、
    /// もし最大サイズをどうしても上げたい場合には、それも不可能ではない、
    /// ということは記しておく.
    pub const MAX_SIZE: usize = 0xFFFF * (BlockSize::MIN as usize) - 2;

    /// ジャーナル領域に埋め込み可能なデータの最大長（バイト単位）.
    pub const MAX_EMBEDDED_SIZE: usize = 0xFFFF;

    /// 引数で指定されたデータを保持する`LumpData`インスタンスを生成する.
    ///
    /// # 性能上の注意
    ///
    /// この関数で生成された`LumpData`インスタンスは、保存先ストレージのブロック境界に合わせた
    /// アライメントが行われていないため、PUTによる保存時に必ず一度、
    /// アライメント用にデータサイズ分のメモリコピーが発生してしまう.
    ///
    /// 保存時のコピーを避けたい場合には[`Storage`]ないし[`DeviceHandle`]が提供している
    /// `allocate_lump_data_with_bytes`メソッドを使用して、
    /// アライメント済みの`LumpData`インスタンスを生成すると良い.
    ///
    /// [`Storage`]: ../storage/struct.Storage.html
    /// [`DeviceHandle`]: ../device/struct.DeviceHandle.html
    ///
    /// # Errors
    ///
    /// データのサイズが`MAX_SIZE`を超えている場合は、`ErrorKind::InvalidInput`エラーが返される.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(data: Vec<u8>) -> Result<Self> {
        track_assert!(
            data.len() <= LumpData::MAX_SIZE,
            ErrorKind::InvalidInput,
            "Too large lump data: {} bytes",
            data.len()
        );
        Ok(LumpData(LumpDataInner::DataRegionUnaligned(data)))
    }

    /// ジャーナル領域埋め込み用の`LumpData`インスタンスを生成する.
    ///
    /// # Errors
    ///
    /// `data`の長さが`MAX_EMBEDDED_SIZE`を超えている場合は、`ErrorKind::InvalidInput`エラーが返される.
    ///
    pub fn new_embedded(data: Vec<u8>) -> Result<Self> {
        track_assert!(
            data.len() <= LumpData::MAX_EMBEDDED_SIZE,
            ErrorKind::InvalidInput,
            "Too large embedded lump data: {} bytes",
            data.len()
        );
        Ok(LumpData(LumpDataInner::JournalRegion(data)))
    }

    /// データを表すバイト列への参照を返す.
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }

    /// データを表すバイト列への破壊的な参照を返す.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }

    /// 所有権を放棄して、内部のバイト列を返す.
    ///
    /// なお、このインスタンスが確保しているのがアライメントされたメモリ領域の場合には、
    /// それを通常の`Vec<u8>`に変換するためのメモリコピーが、本メソッド呼び出し時に発生することになる.
    pub fn into_bytes(self) -> Vec<u8> {
        match self.0 {
            LumpDataInner::JournalRegion(d) => d,
            LumpDataInner::DataRegion(d) => Vec::from(d.as_bytes()),
            LumpDataInner::DataRegionUnaligned(d) => d,
        }
    }

    /// アライメント済みの`LumpData`インスタンス用のメモリ領域を割り当てる.
    ///
    /// この関数によって割当てられたメモリ領域の初期値は未定義であり、
    /// 利用者は事前にゼロ埋めされていることを仮定してはいけない.
    ///
    /// # Errors
    ///
    /// 指定されたサイズが`MAX_SIZE`を超えている場合は、`ErrorKind::InvalidInput`エラーが返される.
    pub(crate) fn aligned_allocate(data_len: usize, block_size: BlockSize) -> Result<Self> {
        track_assert!(
            data_len <= LumpData::MAX_SIZE,
            ErrorKind::InvalidInput,
            "Too large lump data: {} bytes",
            data_len
        );
        Ok(LumpData::from(DataRegionLumpData::new(
            data_len, block_size,
        )))
    }

    pub(crate) fn as_inner(&self) -> &LumpDataInner {
        &self.0
    }
}
impl AsRef<[u8]> for LumpData {
    fn as_ref(&self) -> &[u8] {
        match self.0 {
            LumpDataInner::JournalRegion(ref d) => d,
            LumpDataInner::DataRegion(ref d) => d.as_bytes(),
            LumpDataInner::DataRegionUnaligned(ref d) => d,
        }
    }
}
impl AsMut<[u8]> for LumpData {
    fn as_mut(&mut self) -> &mut [u8] {
        match self.0 {
            LumpDataInner::JournalRegion(ref mut d) => d,
            LumpDataInner::DataRegion(ref mut d) => d.as_bytes_mut(),
            LumpDataInner::DataRegionUnaligned(ref mut d) => d,
        }
    }
}
impl fmt::Debug for LumpData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let block_size = if let LumpDataInner::DataRegion(ref x) = self.0 {
            Some(x.block_size())
        } else {
            None
        };

        let len = cmp::min(128, self.as_bytes().len());
        let bytes = &self.as_bytes()[0..len];
        let omitted = if len < self.as_ref().len() {
            format!("({} bytes omitted)", self.as_ref().len() - len)
        } else {
            "".to_owned()
        };
        write!(
            f,
            "LumpData {{ block_size: {:?}, bytes: {:?}{} }}",
            block_size, bytes, omitted
        )
    }
}
impl PartialEq for LumpData {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}
impl Eq for LumpData {}
impl From<DataRegionLumpData> for LumpData {
    fn from(f: DataRegionLumpData) -> Self {
        LumpData(LumpDataInner::DataRegion(f))
    }
}

#[derive(Clone)]
pub(crate) enum LumpDataInner {
    JournalRegion(Vec<u8>),
    DataRegion(DataRegionLumpData),
    DataRegionUnaligned(Vec<u8>),
}

/// Lumpの概要情報.
///
/// "ヘッダ"という用語は若干不正確だが、
/// `HEAD`リクエストの結果として取得可能なものなので、
/// このような名前にしている.
#[derive(Debug, Clone)]
pub struct LumpHeader {
    /// データサイズの近似値(バイト単位).
    ///
    /// 実際のデータサイズに管理用のメタ情報を追加した上で、
    /// 次のブロックサイズ境界への切り上げたサイズ、となるため、
    /// 最大でストレージのブロックサイズ二つ分だけ、
    /// 実際よりも大きな値となることがある.
    ///
    /// なお、対象lumpのデータがジャーナル領域に埋め込まれている場合には、
    /// 常に正確なサイズが返される.
    pub approximate_data_size: u32,
}
