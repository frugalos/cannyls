//! Data Portion Allocator.

use std::cmp;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::free_portion::{FreePortion, StartBasedFreePortion};
use super::U24;
use metrics::DataAllocatorMetrics;
use storage::index::LumpIndex;
use storage::portion::DataPortion;
use storage::Address;
use {ErrorKind, Result};

/// データ領域用のアロケータ.
///
/// 指定された容量を有するデータ領域から、個々のlumpに必要な部分領域の割当を担当する.
///
/// 割当の単位は"バイト"ではなく、"ブロック"となる.
/// (ただし、これをアロケータのレイヤーで意識する必要はない)
///
/// この実装自体は、完全にメモリ上のデータ構造であり、状態は永続化されない.
///
/// ただし、部分領域群の割当状況自体はジャーナル領域に
/// 情報が残っているので、アロケータインスタンスの構築時には、そこから前回の状態を復元することになる.
///
/// # 割当戦略
///
/// このアロケータは"BestFit"戦略を採用している.
///
/// "BestFit"戦略では、空き領域のリストを管理している.
///
/// 新規割当要求が発行された際には、空き領域のリストを探索し、
/// 要求サイズを満たす空き領域の中で、一番サイズが小さいものが選択される.
///
/// 選択された空き領域は、その中から要求サイズ分だけの割当を行い、
/// もしまだ余剰分がある場合には、再び空き領域リストに戻される.
#[derive(Debug)]
pub struct DataPortionAllocator {
    metrics: DataAllocatorMetrics,
    free_portions: BTreeSet<StartBasedFreePortion>, // portionの全体を管理
    next_pos: u64,                                  // 次に書き込むべき位置
}
impl DataPortionAllocator {
    /// `size`分の部分領域の割当を行う.
    ///
    /// 十分な領域が存在しない場合には`None`が返される.
    pub fn allocate(&mut self, size: u16) -> Option<DataPortion> {
        /*
        現在の居場所から右方向に探索を行いヒットしたportionからデータを取る。

        1. next_posから右方向に検索し、sizeだけのデータを含むPortionがある場合
        2. ないので、先頭に戻ってきてしまう場合
         */

        let border = self.next_pos;
        // search_next(next_pos, size, capacity, free_portions)
        // まず現在位置より右側を探す
        // iterはascending orderなイテレータを作ることが保証されていることを利用する。
        // 参考: https://doc.rust-lang.org/std/collections/struct.BTreeSet.html#method.iter
        for portion in self
            .free_portions
            .iter()
            // filterはiterの並びを変えないという仮定をおいているが、定かではない
            .filter(|portion| portion.0.start().as_u64() >= border)
        {
            let mut portion = portion.0;
            if (size as u32) < portion.len() {
                let new_data_portion = portion.allocate(size);
                self.next_pos = new_data_portion.start.as_u64();
                self.metrics.count_allocation(new_data_portion.len);
                return Some(new_data_portion);
            }
        }

        // 現在位置より右側に size を超える容量を持つPortionがなかったため
        // 今度は先頭からnext_pos未満を調べる
        for portion in self
            .free_portions
            .iter()
            .filter(|portion| portion.0.start().as_u64() < border)
        {
            let mut portion = portion.0;
            if (size as u32) < portion.len() {
                let new_data_portion = portion.allocate(size);
                self.next_pos = new_data_portion.start.as_u64();
                self.metrics.count_allocation(new_data_portion.len);
                return Some(new_data_portion);
            }
        }

        // どちらでもPortionを見つけられなかった場合は、allocate失敗とする
        self.metrics.nospace_failures.increment();
        None
    }

    fn merge_free_portions_if_possible(&mut self, mut portion: FreePortion) -> FreePortion {
        // 「`portion`の始端」に一致する終端を持つportion `prev`を探す。
        // もし存在するなら、 prev portion の並びでmerge可能である。
        // 注意: BTreeSetのgetでは、EqではなくOrd traitが用いられる。
        // 従ってendが一致する場合に限りOrdering::Equalとなる。
        let key = FreePortion::new(portion.start(), 0);
        if let Some(prev) = self
            .free_portions
            // free_portionsから、開始位置がportion.start未満のものを探し
            .range((Unbounded, Excluded(&StartBasedFreePortion(key))))
            // そのうちで最後の要素を取り出す
            .next_back()
            .map(|p| p.0)
        {
            // 最後の要素を取り出しただけでは、それが隣接しているかどうか分からないため検査する
            if prev.end() == portion.start() && portion.checked_extend(prev.len()) {
                portion = FreePortion::new(prev.start(), portion.len());
                self.free_portions.remove(&StartBasedFreePortion(prev));
            }
        }

        // 「`portion`の終端」に一致する始端を持つportion `next` を探す。
        // もし存在するなら、 portion next の並びでmerge可能である。
        let key = FreePortion::new(portion.end(), 0);
        if let Some(next) = self
            .free_portions
            .get(&StartBasedFreePortion(key))
            .map(|n| n.0)
        {
            if portion.checked_extend(next.len()) {
                self.free_portions.remove(&StartBasedFreePortion(next));
            }
        }

        portion
    }

    /// 割当済みの部分領域の解放を行う.
    ///
    /// # 事前条件
    ///
    /// - `portion`は「以前に割当済み」かつ「未解放」の部分領域である
    pub fn release(&mut self, portion: DataPortion) {
        self.metrics.count_releasion(portion.len);
        let portion = self.merge_free_portions_if_possible(FreePortion::from(portion));
        self.free_portions.insert(StartBasedFreePortion(portion));
    }

    fn to_node_and_version(lumpid: &u128) -> Option<(u64, u64)> {
        // frugalosによって挿入されたlumpidの構成は次のようになっているはず
        // データ:      lumpid[0] = 1, lumpid[1..6] = node id, lumpid[8..15] = version
        // Raftデータ:  lumpid[0] = 0, lumpid[1..6] = node id, lumpid[7] = type, lumpid[8..15] = lumpid

        if (lumpid >> 120) == 1u128 {
            let node_id = (lumpid >> 64u128) & 0x0000ffff_ffffffff;
            let version = lumpid & 0xffffffff_ffffffff;
            Some((node_id as u64, version as u64))
        } else {
            None
        }
    }

    /*
    現在の実装は　[ [] ] のようにすっぽり収まっている場合を考えられていない。
     */
    fn guess_who_is_the_latest(
        summary: &HashMap<u64, ((u64, u64), (u64, u64))>,
    ) -> Vec<((u64, u64), (u64, u64))> {
        fn is_cycle(x: u64, y: u64, z: u64) -> bool {
            (x < y && y < z) || (z < x && x < y) || (y < z && z < x)
        }

        fn is_dominated(left: (u64, u64), right: (u64, u64)) -> bool {
            is_cycle(right.0, left.1, right.1)
        }

        /*
        各nodeに次の関係 <: を入れる
        (A=Version, B=Version) <: (C=Version, D=Version) <->
        何れかが成立する:
        1. C.pos < B.pos < D.pos
        2. D.pos < C.pos < B.pos
        3. B.pos < D.pos < C.pos

        この関係 <: の左側を、右側によって支配されていると呼ぶ。
        誰にも支配されていない元を求める。
         */

        let mut full: BTreeSet<u64> = BTreeSet::new();
        for key in summary.keys() {
            full.insert(*key);
        }

        for (key1, value1) in summary.iter() {
            let left_pos1 = (value1.0).1;
            let left_pos2 = (value1.1).1;
            for (key2, value2) in summary.iter() {
                let right_pos1 = (value2.0).1;
                let right_pos2 = (value2.1).1;
                if key1 != key2 && is_dominated((left_pos1, left_pos2), (right_pos1, right_pos2)) {
                    full.remove(key1);
                }
            }
        }

        let mut result: Vec<((u64, u64), (u64, u64))> = Vec::new();
        for item in full.iter() {
            result.push(*summary.get(item).unwrap());
        }
        result
    }

    pub fn build2(metrics: DataAllocatorMetrics, lump_index: &LumpIndex) -> Result<Self> {
        type Node = u64;
        type Version = u64;
        type Index = u64;
        let mut summary: HashMap<Node, ((Version, Index), (Version, Index))> = HashMap::new();

        for (lumpid, portion) in lump_index.entries() {
            let index = portion.start.as_u64();
            if let Some((node, version)) = Self::to_node_and_version(&lumpid.as_u128()) {
                if summary.contains_key(&node) {
                    let (vp1, vp2) = summary.get_mut(&node).unwrap();
                    if version > vp2.0 {
                        *vp2 = (version, index);
                    }
                } else {
                    summary.insert(node, ((version, index), (version, index)));
                }
            }
        }

        let candidates = Self::guess_who_is_the_latest(&summary);

        match candidates.len() {
            0 => {
                // lump_indexのサイズがそもそも0だった
            }
            1 => {
                // 次に書き込むべき場所が決まった
            }
            _ => {
                // 次に書き込むべき場所が決まっていない
                // このときって、candidatesの結果は有用なのか？
            }
        }

        let allocator = DataPortionAllocator {
            free_portions: BTreeSet::new(),
            next_pos: 0,
            metrics,
        };
        Ok(allocator)
    }

    /// アロケータを構築する.
    ///
    /// `portions`には、既に割当済みの部分領域群が列挙されている.
    ///
    /// アロケータが利用可能な領域のサイズ（キャパシティ）の情報は、`metrics`から取得される.
    pub fn build<I>(metrics: DataAllocatorMetrics, portions: I) -> Result<Self>
    where
        I: Iterator<Item = DataPortion>,
    {
        let allocator = DataPortionAllocator {
            free_portions: BTreeSet::new(),
            next_pos: 0,
            metrics,
        };
        Ok(allocator)
    }

    /// アロケータ用のメトリクスを返す.
    pub fn metrics(&self) -> &DataAllocatorMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use prometrics::metrics::MetricBuilder;
    use std::iter;
    use trackable::result::TestResult;

    use block::BlockSize;
    use lump::LumpId;
    use metrics::DataAllocatorMetrics;
    use storage::allocator::DataPortionAllocator;
    use storage::index::LumpIndex;
    use storage::portion::{DataPortion, Portion};
    use storage::Address;

    #[test]
    fn it_works() -> TestResult {
        let capacity = Address::from(24);
        let mut allocator = track!(DataPortionAllocator::build(
            metrics(capacity),
            iter::empty()
        ))?;
        assert_eq!(allocator.allocate(10), Some(portion(0, 10)));
        assert_eq!(allocator.allocate(10), Some(portion(10, 10)));
        assert_eq!(allocator.allocate(10), None);
        assert_eq!(allocator.allocate(4), Some(portion(20, 4)));

        allocator.release(portion(10, 10));
        assert_eq!(allocator.allocate(5), Some(portion(10, 5)));
        assert_eq!(allocator.allocate(2), Some(portion(15, 2)));
        assert_eq!(allocator.allocate(4), None);

        let m = allocator.metrics();
        assert_eq!(m.free_list_len(), 1);
        assert_eq!(m.allocated_portions(), 5);
        assert_eq!(m.released_portions(), 1);
        assert_eq!(m.nospace_failures(), 2);
        assert_eq!(m.usage_bytes(), 21 * u64::from(BlockSize::MIN));
        assert_eq!(m.capacity_bytes, 24 * u64::from(BlockSize::MIN));
        Ok(())
    }

    #[test]
    #[should_panic]
    fn it_panics() {
        let capacity = Address::from(24);
        let mut allocator = DataPortionAllocator::build(metrics(capacity), iter::empty())
            .expect("Unexpected panic");

        // Try releasing an unallocated portion
        allocator.release(portion(10, 10));
    }

    #[test]
    fn rebuild() -> TestResult {
        let mut index = LumpIndex::new();
        index.insert(lump_id("000"), Portion::Data(portion(5, 10)));
        index.insert(lump_id("111"), Portion::Data(portion(2, 3)));
        index.insert(lump_id("222"), Portion::Data(portion(15, 5)));
        index.remove(&lump_id("000"));

        let capacity = Address::from(20);
        let mut allocator = track!(DataPortionAllocator::build(
            metrics(capacity),
            index.data_portions()
        ))?;
        assert_eq!(allocator.metrics().free_list_len(), 2);
        assert_eq!(allocator.metrics().allocated_portions(), 2);

        assert_eq!(allocator.allocate(11), None);
        assert_eq!(allocator.allocate(10), Some(portion(5, 10)));
        assert_eq!(allocator.allocate(3), None);
        assert_eq!(allocator.allocate(1), Some(portion(0, 1)));
        assert_eq!(allocator.allocate(1), Some(portion(1, 1)));
        assert_eq!(allocator.allocate(1), None);
        assert_eq!(allocator.metrics().free_list_len(), 0);
        assert_eq!(
            allocator.metrics().usage_bytes(),
            allocator.metrics().capacity_bytes
        );
        Ok(())
    }

    #[test]
    fn rebuild2() -> TestResult {
        let mut index = LumpIndex::new();
        index.insert(lump_id("000"), Portion::Data(portion(1, 10)));
        index.insert(lump_id("222"), Portion::Data(portion(15, 5)));

        let capacity = Address::from(20);
        let mut allocator = track!(DataPortionAllocator::build(
            metrics(capacity),
            index.data_portions()
        ))?;

        assert_eq!(allocator.allocate(2), Some(portion(11, 2)));
        assert_eq!(allocator.allocate(1), Some(portion(0, 1)));
        Ok(())
    }

    #[test]
    fn allocate_and_release() -> TestResult {
        let capacity = Address::from(419431);
        let mut allocator = track!(DataPortionAllocator::build(
            metrics(capacity),
            iter::empty()
        ))?;

        let p0 = allocator.allocate(65).unwrap();
        let p1 = allocator.allocate(65).unwrap();
        let p2 = allocator.allocate(65).unwrap();
        allocator.release(p0);
        allocator.release(p1);

        let p3 = allocator.allocate(65).unwrap();
        let p4 = allocator.allocate(65).unwrap();
        allocator.release(p2);
        allocator.release(p3);

        let p5 = allocator.allocate(65).unwrap();
        let p6 = allocator.allocate(65).unwrap();
        allocator.release(p4);
        allocator.release(p5);
        allocator.release(p6);
        Ok(())
    }

    fn lump_id(id: &str) -> LumpId {
        id.parse().unwrap()
    }

    fn portion(offset: u32, length: u16) -> DataPortion {
        DataPortion {
            start: Address::from(offset),
            len: length,
        }
    }

    fn metrics(capacity: Address) -> DataAllocatorMetrics {
        let capacity_bytes = capacity.as_u64() * u64::from(BlockSize::MIN);
        DataAllocatorMetrics::new(&MetricBuilder::new(), capacity_bytes, BlockSize::min())
    }
}
