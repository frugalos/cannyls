//! Data Portion, Journal Portion, and Portion

use block::BlockSize;
use storage::Address;

/// データ領域内の部分領域を示すための構造体.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataPortion {
    /// 部分領域の開始位置（ブロック単位）
    pub start: Address,

    /// 部分領域の長さ（ブロック単位）
    pub len: u16,
}
impl DataPortion {
    /// 部分領域の終端位置を返す.  
    /// **注意**: DataPortionは [start, end) の領域を用いるため、
    /// end部には書き込みは行われていない。
    pub fn end(&self) -> Address {
        self.start + Address::from(u32::from(self.len))
    }
}

/// ジャーナル領域内の部分領域を示すための構造体.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JournalPortion {
    /// 部分領域の開始位置（バイト単位）
    pub start: Address,

    /// 部分領域の長さ（バイト単位）
    pub len: u16,
}

/// 部分領域.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Portion {
    /// ジャーナル領域内の部分領域.
    Journal(JournalPortion),

    /// データ領域内の部分領域.
    Data(DataPortion),
}
impl Portion {
    /// 部分領域の長さをバイト単位で返す.
    pub fn len(&self, block_size: BlockSize) -> u32 {
        match *self {
            Portion::Journal(ref p) => u32::from(p.len),
            Portion::Data(ref p) => u32::from(p.len) * u32::from(block_size.as_u16()),
        }
    }
}

/// `Portion`の内部表現のサイズを64bitにした構造体.
///
/// `LumpIndex`のような、数百万～数千万オーダーの部分領域を保持する
/// データ構造では、各要素のメモリ使用量を節約することが
/// 重要となるので、そのような目的でこの構造体が提供されている.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortionU64(u64);
impl From<Portion> for PortionU64 {
    fn from(f: Portion) -> Self {
        let (kind, offset, len) = match f {
            Portion::Journal(p) => (0, p.start.as_u64(), u64::from(p.len)),
            Portion::Data(p) => (1, p.start.as_u64(), u64::from(p.len)),
        };
        PortionU64(offset | (len << 40) | (kind << 63))
    }
}
impl From<PortionU64> for Portion {
    fn from(f: PortionU64) -> Self {
        let is_journal = (f.0 >> 63) == 0;
        let len = (f.0 >> 40) as u16;
        let start = Address::from_u64(f.0 & Address::MAX).unwrap();
        if is_journal {
            Portion::Journal(JournalPortion { start, len })
        } else {
            Portion::Data(DataPortion { start, len })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;
    use storage::Address;

    #[test]
    fn it_works() {
        // DataPortion
        let p0 = Portion::Data(DataPortion {
            start: Address::from(10),
            len: 30,
        });
        let p1 = PortionU64::from(p0);
        assert_eq!(mem::size_of_val(&p1), 8);

        let p2 = Portion::from(p1);
        assert_eq!(p0, p2);

        // JournalPortion
        let p0 = Portion::Journal(JournalPortion {
            start: Address::from(10),
            len: 30,
        });
        let p1 = PortionU64::from(p0);
        assert_eq!(mem::size_of_val(&p1), 8);

        let p2 = Portion::from(p1);
        assert_eq!(p0, p2);
    }
}
