//! ストレージやNVMのブロック(読み書きの際の最小単位)関連の構成要素.
use {ErrorKind, Result};

pub(crate) use self::aligned_bytes::AlignedBytes;

mod aligned_bytes;

/// [`Storage`]や[`NonVolatileMemory`]のブロックサイズを表現するための構造体.
///
/// "ブロック"は、I/Oの最小単位であり、読み書き対象の領域およびその際に使用するバッファは、
/// `BlockSize`によって指定された境界にアライメントされている必要がある.
///
/// 指定されたサイズのブロック境界にアライメントを行うための補助メソッド群も提供している.
///
/// [`Storage`]: ../storage/struct.Storage.html
/// [`NonVolatileMemory`]: ../nvm/trait.NonVolatileMemory.html
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockSize(u16);
impl BlockSize {
    /// 許容されるブロックサイズの最小値.
    ///
    /// 全てのブロックサイズは、この値の倍数である必要がある.
    ///
    /// また`BlockSize::default()`で使われる値でもある.
    pub const MIN: u16 = 512;

    /// 許容可能な最小のブロックサイズを持つ`BlockSize`インスタンスを返す.
    ///
    /// # Examples
    ///
    /// ```
    /// use cannyls::block::BlockSize;
    ///
    /// assert_eq!(BlockSize::min().as_u16(), BlockSize::MIN);
    /// ```
    pub fn min() -> Self {
        BlockSize(Self::MIN)
    }

    /// 指定された値のブロックサイズを表現する`BlockSize`インスタンスを生成する.
    ///
    /// # Errors
    ///
    /// 以下の場合には、種類が`ErrorKind::InvalidInput`のエラーが返される:
    ///
    /// - `block_size`が`BlockSize::MIN`未満
    /// - `block_size`が`BlockSize::MIN`の倍数ではない
    ///
    /// # Examples
    ///
    /// ```
    /// use cannyls::ErrorKind;
    /// use cannyls::block::BlockSize;
    ///
    /// assert_eq!(BlockSize::new(512).ok().map(|a| a.as_u16()), Some(512));
    /// assert_eq!(BlockSize::new(4096).ok().map(|a| a.as_u16()), Some(4096));
    ///
    /// assert_eq!(BlockSize::new(256).err().map(|e| *e.kind()), Some(ErrorKind::InvalidInput));
    /// assert_eq!(BlockSize::new(513).err().map(|e| *e.kind()), Some(ErrorKind::InvalidInput));
    /// ```
    #[allow(clippy::new_ret_no_self)]
    pub fn new(block_size: u16) -> Result<Self> {
        track_assert!(block_size >= Self::MIN, ErrorKind::InvalidInput);
        track_assert_eq!(block_size % Self::MIN, 0, ErrorKind::InvalidInput);
        Ok(BlockSize(block_size))
    }

    /// 指定位置より後方の最初のブロックサイズ位置を返す.
    ///
    /// # Examples
    ///
    /// ```
    /// use cannyls::block::BlockSize;
    ///
    /// let block_size = BlockSize::new(512).unwrap();
    /// assert_eq!(block_size.ceil_align(0), 0);
    /// assert_eq!(block_size.ceil_align(1), 512);
    /// assert_eq!(block_size.ceil_align(512), 512);
    /// ```
    pub fn ceil_align(self, position: u64) -> u64 {
        let block_size = u64::from(self.0);
        (position + block_size - 1) / block_size * block_size
    }

    /// 指定位置より前方の最初のブロックサイズ位置を返す.
    ///
    /// # Examples
    ///
    /// ```
    /// use cannyls::block::BlockSize;
    ///
    /// let block_size = BlockSize::new(512).unwrap();
    /// assert_eq!(block_size.floor_align(0), 0);
    /// assert_eq!(block_size.floor_align(1), 0);
    /// assert_eq!(block_size.floor_align(512), 512);
    /// ```
    pub fn floor_align(self, position: u64) -> u64 {
        let block_size = u64::from(self.0);
        (position / block_size) * block_size
    }

    /// ブロックサイズ値を`u16`に変換して返す.
    pub fn as_u16(self) -> u16 {
        self.0
    }

    /// このブロックサイズが`other`を包含しているかを確認する.
    ///
    /// "包含している"とは「`self`のブロックサイズが`other`のブロックサイズの倍数」であることを意味する.
    ///
    /// # Examples
    ///
    /// ```
    /// use cannyls::block::BlockSize;
    ///
    /// let block_size = BlockSize::new(2048).unwrap();
    /// assert!(block_size.contains(BlockSize::new(512).unwrap()));
    /// assert!(block_size.contains(BlockSize::new(1024).unwrap()));
    /// assert!(!block_size.contains(BlockSize::new(1536).unwrap()));
    /// ```
    pub fn contains(self, other: BlockSize) -> bool {
        self.0 >= other.0 && self.0 % other.0 == 0
    }

    /// 指定位置がブロックサイズ境界に沿っているかどうかを判定する.
    ///
    /// # Examples
    ///
    /// ```
    /// use cannyls::block::BlockSize;
    ///
    /// let block_size = BlockSize::new(512).unwrap();
    /// assert!(block_size.is_aligned(0));
    /// assert!(block_size.is_aligned(512));
    /// assert!(block_size.is_aligned(1024));
    ///
    /// assert!(!block_size.is_aligned(511));
    /// assert!(!block_size.is_aligned(513));
    /// ```
    pub fn is_aligned(self, position: u64) -> bool {
        (position % u64::from(self.0)) == 0
    }
}
impl Default for BlockSize {
    fn default() -> Self {
        Self::min()
    }
}
