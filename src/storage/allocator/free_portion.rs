//! Free Portion

use std::cmp;

use super::U24;
use storage::portion::DataPortion;
use storage::Address;

/// 空き(割当可能)領域を表現するための構造体.
///
/// メモリを節約するために、内部的には64bit整数にエンコードして情報を保持している.
/// その制約上、一つのインスタンスで表現可能な長さは24bit幅の範囲の制限されている.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct FreePortion(u64);

#[cfg_attr(feature = "cargo-clippy", allow(len_without_is_empty))]
impl FreePortion {
    /// add comment
    pub fn new(offset: Address, len: U24) -> Self {
        FreePortion(offset.as_u64() | (u64::from(len) << 40))
    }

    /// add comment
    pub fn start(self) -> Address {
        Address::from_u64(self.0 & Address::MAX).unwrap()
    }

    /// add comment
    pub fn end(self) -> Address {
        self.start() + Address::from(self.len())
    }

    /// add comment
    pub fn len(self) -> U24 {
        (self.0 >> 40) as U24
    }

    /// `size`分だけ長さを増やす.
    ///
    /// ただし、それによって`U24`の範囲を超過してしまう場合には、更新は行わず、関数の結果として`false`を返す.
    pub fn checked_extend(&mut self, size: U24) -> bool {
        let new_len = u64::from(self.len()) + u64::from(size);
        if new_len <= 0xFF_FFFF {
            *self = FreePortion::new(self.start(), new_len as U24);
            true
        } else {
            false
        }
    }

    /// 先頭から`size`分だけ割り当てを行う.
    ///
    /// # Panics
    ///
    /// `size`が`self.len()`を超えている場合には、現在のスレッドがパニックする.
    pub fn allocate(&mut self, size: u16) -> DataPortion {
        assert!(U24::from(size) <= self.len());
        let allocated = DataPortion {
            start: self.start(),
            len: size,
        };
        *self = Self::new(
            self.start() + Address::from(u32::from(size)),
            self.len() - U24::from(size),
        );
        allocated
    }
}
impl From<DataPortion> for FreePortion {
    fn from(f: DataPortion) -> Self {
        FreePortion::new(f.start, U24::from(f.len))
    }
}

/// 比較が"空き領域のサイズ順"で行われる`FreePortion`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeBasedFreePortion(pub FreePortion);
impl PartialOrd for SizeBasedFreePortion {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SizeBasedFreePortion {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match self.0.len().cmp(&other.0.len()) {
            cmp::Ordering::Equal => self.0.start().cmp(&other.0.start()),
            not_equal => not_equal,
        }
    }
}

/// 比較が"終端位置が小さい順"で行われる`FreePortion`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndBasedFreePortion(pub FreePortion);
impl PartialOrd for EndBasedFreePortion {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for EndBasedFreePortion {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.0.end().cmp(&other.0.end())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::Address;

    #[test]
    fn it_works() {
        let mut p = FreePortion::new(Address::from(100), 50);
        assert_eq!(p.start(), Address::from(100));
        assert_eq!(p.end(), Address::from(150));
        assert_eq!(p.len(), 50);

        assert!(!p.checked_extend(0xFF_FFFF));
        assert!(p.checked_extend(100));
        assert_eq!(p.start(), Address::from(100));
        assert_eq!(p.len(), 150);

        let allocated = p.allocate(30);
        assert_eq!(allocated.start, Address::from(100));
        assert_eq!(allocated.len, 30);
        assert_eq!(p.start(), Address::from(130));
        assert_eq!(p.len(), 120);

        let allocated = p.allocate(120);
        assert_eq!(allocated.start, Address::from(130));
        assert_eq!(allocated.len, 120);
        assert_eq!(p.start(), Address::from(250));
        assert_eq!(p.len(), 00);
    }

    #[test]
    #[should_panic]
    fn underflow() {
        let mut p = FreePortion::new(Address::from(100), 50);
        p.allocate(51);
    }
}
