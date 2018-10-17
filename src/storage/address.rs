use std::ops::{Add, Sub};

/// ストレージ内のアドレス表現に使われている40bit幅の整数値.
///
/// アドレスの単位は、以下のように使用箇所によって異なっている:
///
/// - ジャーナル領域: **バイト単位**
///-  データ領域: **ブロック単位**
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Address(u64);
impl Address {
    /// 取り得るアドレスの最大値.
    pub const MAX: u64 = (1 << 40) - 1;

    /// アドレスの値を返す.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// `value`を対応する位置のアドレスに変換する.
    ///
    /// `value`の値が40bit以内に収まらない場合には`None`が返される.
    pub fn from_u64(value: u64) -> Option<Self> {
        if value <= Self::MAX {
            Some(Address(value))
        } else {
            None
        }
    }
}
impl From<u32> for Address {
    fn from(from: u32) -> Self {
        Address(u64::from(from))
    }
}
impl Add for Address {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        let value = self.0 + rhs.0;
        Address::from_u64(value).expect("address overflow")
    }
}
impl Sub for Address {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        let value = self.0.checked_sub(rhs.0).expect("address underflow");
        Address(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(Address::from_u64(0).map(|a| a.as_u64()), Some(0));
        assert_eq!(
            Address::from_u64(Address::MAX).map(|a| a.as_u64()),
            Some(Address::MAX)
        );
        assert_eq!(Address::from_u64(Address::MAX + 1), None);

        assert_eq!(Address::from(10) + Address::from(2), Address::from(12));
        assert_eq!(Address::from(10) - Address::from(2), Address::from(8));
    }

    #[test]
    #[should_panic]
    fn overflow() {
        let _ = Address::from_u64(Address::MAX).map(|a| a + Address::from(1));
    }

    #[test]
    #[should_panic]
    fn underflow() {
        let _ = Address::from(0) - Address::from(1);
    }
}
