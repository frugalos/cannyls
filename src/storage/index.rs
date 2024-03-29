//! デバイスに格納されているlump群の情報を管理するためのインデックス.
use std::collections::{btree_map, BTreeMap};
use std::ops;

use crate::block::BlockSize;
use crate::lump::LumpId;
use crate::storage::portion::{DataPortion, Portion, PortionU64};
use crate::storage::StorageUsage;

/// Lump群の位置情報を保持するインデックス.
///
/// デバイスに格納されているlumpのID群と、それぞれのデータの格納先の情報、を保持している.
///
/// このインデックス自体は永続化されることはないメモリ上のデータ構造であり、
/// デバイスの起動時に、ジャーナルの情報を用いて毎回再構築される.
#[derive(Debug, Clone, Default)]
pub struct LumpIndex {
    // `BTreeMap`の方が`HashMap`よりもメモリ効率が良いので、こちらを採用
    map: BTreeMap<LumpId, PortionU64>,
}
impl LumpIndex {
    /// 新しい`LumpIndex`インスタンスを生成する.
    pub fn new() -> Self {
        LumpIndex {
            map: BTreeMap::new(),
        }
    }

    /// 渡された範囲オブジェクトrangeを用いて、
    /// 登録されているlumpのうちrangeに含まれるもののストレージ使用量を返す。
    pub fn usage_range(&self, range: ops::Range<LumpId>, block_size: BlockSize) -> StorageUsage {
        StorageUsage::approximate(self.map.range(range).fold(0, |acc, (_, p)| {
            acc + Portion::from(*p).len(block_size) as u64
        }))
    }

    /// 指定されたlumpを検索する.
    pub fn get(&self, lump_id: &LumpId) -> Option<Portion> {
        self.map.get(lump_id).map(|p| (*p).into())
    }

    /// 新規lumpを登録する.
    pub fn insert(&mut self, lump_id: LumpId, portion: Portion) {
        self.map.insert(lump_id, portion.into());
    }

    /// インデックスのサイズ(i.e., 登録lump数)を返す.
    ///
    /// 結果は昇順にソートされている.
    pub fn remove(&mut self, lump_id: &LumpId) -> Option<Portion> {
        self.map.remove(lump_id).map(std::convert::Into::into)
    }

    /// 登録されているlumpのID一覧を返す.
    pub fn list(&self) -> Vec<LumpId> {
        self.map.keys().cloned().collect()
    }

    /// インデックスのサイズ(i.e., 登録lump数)を返す.
    pub fn len(&self) -> u64 {
        self.map.len() as u64
    }

    /// 割当済みのデータ部分領域を操作するためのイテレータを返す.
    pub fn data_portions(&self) -> DataPortions {
        DataPortions(self.map.values())
    }

    /// 渡された範囲オブジェクトrangeを用いて、
    /// 登録されているlumpのうちrangeに含まれるものの一覧を返す。
    pub fn list_range(&self, range: ops::Range<LumpId>) -> Vec<LumpId> {
        let btree_range = self.map.range(range);
        btree_range.map(|(k, _)| *k).collect()
    }
}

#[derive(Debug)]
pub struct DataPortions<'a>(btree_map::Values<'a, LumpId, PortionU64>);
impl<'a> Iterator for DataPortions<'a> {
    type Item = DataPortion;
    fn next(&mut self) -> Option<Self::Item> {
        for &portion in &mut self.0 {
            if let Portion::Data(portion) = portion.into() {
                return Some(portion);
            }
        }
        None
    }
}
