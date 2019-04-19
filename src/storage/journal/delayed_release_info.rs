use std::convert::Into;
use std::vec::Vec;
use storage::portion::{DataPortion, DataPortionU64};

/// 削除操作によって消されたDataPortionを表すためのVec<DataPortion64>に対するnew type。
///
/// DataPortionではなくDataPortionU64を用いるのは、
/// Portionに対してPortionU64に対するのと同様の理由で、
/// 可能な限りメモリ上の表現を圧縮したいため。
#[derive(Debug, Clone)]
struct DataPortion64s(Vec<DataPortionU64>);

impl DataPortion64s {
    /// 対応するDataPortionのリストを復元する関数。
    pub fn to_data_portions(&self) -> Vec<DataPortion> {
        self.0.iter().cloned().map(Into::into).collect()
    }
}

/// 遅延解放のために必要になる情報と操作を集約する構造体。
///
/// 主要なメソッドの役割は以下の通り:
/// * set_initial_delete_recordsメソッド
///     * 幾つの削除レコードが起動時に見つかったか（＝open時に永続化が確定しているか）を保存する。
///     * このメソッドは高々一度しか呼び出せない。
/// * detect_new_synced_delete_recordsメソッド
///     * GCキューに削除レコードが幾つ追加されたか（＝幾つの削除レコードが永続化したか）を保存する。
///     * このメソッドは何度呼び出されても良い。
/// * insert_data_portionsメソッド
///     * open以降に、「単発の」Delete及びDeleteRangeメソッドで解放対象となったデータポーション群を順序付け、バッファに保存する。
/// * (take_)releasable_data_portionsメソッド
///     1. set_initial_delete_recordsメソッドと, detect_new_synced_delete_recordsメソッドにより
///       この構造体は幾つの削除レコードが「open以降に」永続化されたか分かる。この数をNとする。
///     2. insert_data_portionsメソッドでバッファに保存したデータポーション群のうち、前からN個は安全に解放可能であるので、
///       これを返す。
#[derive(Debug)]
pub struct DelayedReleaseInfo {
    // ジャーナル領域をopenした以降に追加したDelete及びDeleteRangeレコードで、永続化が確定済み（解放可能）のものの数。
    num_of_releasable_delete_records: usize,
    // ジャーナル領域のrestore時に処理済みのDelete及びDeleteRangeレコードで、未だGCキューに投入されていないものの数。
    num_of_unqueued_initial_delete_records: usize,
    // 永続化済みのDelete及びDeleteRangeレコードに紐づく、解放可能なPortionを表現するための集合。
    entries: Vec<DataPortion64s>,
    already_set_initial_delete_records_called: bool,
}
impl DelayedReleaseInfo {
    pub fn new() -> Self {
        DelayedReleaseInfo {
            num_of_releasable_delete_records: 0,
            num_of_unqueued_initial_delete_records: 0,
            entries: Vec::new(),
            already_set_initial_delete_records_called: false,
        }
    }

    /// 安全に解放可能なデータポーションの一覧を返す。
    /// このメソッドは非破壊的である。すなわち次が成立する:
    /// ```#[ignore]
    /// use storage::portion::{DataPortion, DataPortionU64};
    /// ...
    /// let portions1 = info.releasable_data_portions();
    /// let portions2 = info.releasable_data_portions();
    /// assert_eq!(portions1, portions2);
    /// ```
    pub fn releasable_data_portions(&self) -> Vec<DataPortion> {
        let n = self.num_of_releasable_delete_records;
        (&self.entries)[..n]
            .iter()
            .flat_map(DataPortion64s::to_data_portions)
            .collect()
    }

    /// 安全に解放可能なデータポーションの一覧を返し、かつ内部バッファを消去する。
    /// このメソッドは破壊的であり、すなわち次が成立する:
    /// ```#[ignore]
    /// let info = DelayedReleaseInfo::new();
    /// ...
    /// let portions = info.take_releasable_data_portions();
    /// assert_eq!(info.releasable_data_portions(), vec![]);
    /// ```
    pub fn take_releasable_data_portions(&mut self) -> Vec<DataPortion> {
        let n = self.num_of_releasable_delete_records;
        self.num_of_releasable_delete_records = 0;
        self.entries
            .drain(..n)
            .flat_map(|e| DataPortion64s::to_data_portions(&e))
            .collect()
    }

    /// ジャーナル領域のrestore処理中に見つけたDeleteレコードとDeleteRangeレコードの総数を記録するためのメソッド。
    pub fn set_initial_delete_records(&mut self, u: usize) {
        if !self.already_set_initial_delete_records_called {
            self.num_of_unqueued_initial_delete_records = u;
            self.already_set_initial_delete_records_called = true;
        } else {
            panic!("set_initial_delete_records has been called in multiple times");
        }
    }

    /// 永続化が確定したDeleteレコードとDeleteRangeレコードを
    /// 「新たに」`u`個見つけた場合に呼び出して登録するためのメソッド。
    ///
    /// 既に発見したDelete(Range)レコードを登録しないように、呼び出し側が注意する必要がある。
    pub fn detect_new_synced_delete_records(&mut self, u: usize) {
        if self.num_of_unqueued_initial_delete_records == 0 {
            self.num_of_releasable_delete_records += u;
        } else if self.num_of_unqueued_initial_delete_records >= u {
            self.num_of_unqueued_initial_delete_records -= u;
        } else {
            self.num_of_releasable_delete_records +=
                u - self.num_of_unqueued_initial_delete_records;
            self.num_of_unqueued_initial_delete_records = 0;
        }
    }

    /// 削除操作が行われて、LumpIndexから外れたデータポーション群を内部バッファに登録するためのメソッド。
    ///
    /// 注意:
    /// * このメソッドは、個別のDelete及びDeleteRange毎に呼び出す必要がある。
    /// * 複数のDelete及びDeleteRangeの結果外れたデータポーション群をまとめて登録してはならない。
    pub fn insert_data_portions(&mut self, portions: Vec<DataPortion>) {
        self.entries.push(DataPortion64s(
            portions.into_iter().map(Into::into).collect(),
        ));
    }

    // 以下はユニットテスト用のメソッド
    #[allow(dead_code)]
    pub(crate) fn num_of_releasable_delete_records(&self) -> usize {
        self.num_of_releasable_delete_records
    }

    #[allow(dead_code)]
    pub(crate) fn num_of_unqueued_initial_delete_records(&self) -> usize {
        self.num_of_unqueued_initial_delete_records
    }
}

#[cfg(test)]
mod tests {
    use self::DataPortion64s;
    use super::*;
    use trackable::result::TestResult;

    fn make_dataportion(start: u16, len: u16) -> DataPortion {
        use storage::Address;
        DataPortion {
            start: Address::from_u64(u64::from(start)).unwrap(),
            len,
        }
    }

    fn make_dataportion64(start: u16, len: u16) -> DataPortionU64 {
        make_dataportion(start, len).into()
    }

    #[test]
    fn to_data_portions_works() -> TestResult {
        let portion = make_dataportion64(0, 0);
        let single_delete = DataPortion64s(vec![portion.clone()]);
        assert_eq!(single_delete.to_data_portions(), vec![portion.into()]);

        let portion_range: Vec<DataPortionU64> =
            (0..10).map(|i| make_dataportion64(i, i)).collect();
        let deletes = DataPortion64s(portion_range);
        assert_eq!(
            deletes.to_data_portions(),
            (0..10)
                .map(|i| make_dataportion(i, i))
                .collect::<Vec<DataPortion>>()
        );

        Ok(())
    }

    #[test]
    fn num_of_releasable_delete_records_reflects_correct_number() -> TestResult {
        let mut info = DelayedReleaseInfo::new();

        // Journal領域のopen中に、併せて5つのDeleteまたはDeleteRangeレコードを発見したとする。
        info.set_initial_delete_records(5);

        // fill_gc_queue中に、3つの永続化されたDeleteまたはDeleteRangeレコードを見つけたとする。
        info.detect_new_synced_delete_records(3);

        // open時にみつけたDeleteまたはDeleteRangeレコードのうち
        // まだ2つはGCキューに入っていない。
        assert_eq!(info.num_of_unqueued_initial_delete_records(), 2);
        // 解放して意味のあるDeleteまたはDeleteRangeレコードは存在しない。
        assert_eq!(info.num_of_releasable_delete_records(), 0);

        // fill_gc_queue中に、新たに4つのDeleteまたはDeleteRangeレコードを見つけたとする。
        info.detect_new_synced_delete_records(4);

        // open時に見つけたDeleteまたはDeleteRangeレコードは全てGCキューに入った
        assert_eq!(info.num_of_unqueued_initial_delete_records(), 0);
        // open以降に作成されたDeleteまたはDeleteRangeレコードで、解放して問題ないものは2つある。
        assert_eq!(info.num_of_releasable_delete_records(), 2);

        info.detect_new_synced_delete_records(1);
        assert_eq!(info.num_of_unqueued_initial_delete_records(), 0);
        assert_eq!(info.num_of_releasable_delete_records(), 3);

        Ok(())
    }

    #[test]
    fn releasable_data_portions_works() -> TestResult {
        let mut info = DelayedReleaseInfo::new();

        // Journal領域のopen中に、併せて2つのDeleteまたはDeleteRangeレコードを発見したとする。
        info.set_initial_delete_records(2);

        // 解放可能か不明なデータポーションが3つ登録されたとする。
        info.insert_data_portions(vec![make_dataportion(0, 0)]);
        info.insert_data_portions(vec![make_dataportion(1, 1)]);
        info.insert_data_portions(vec![make_dataportion(2, 2)]);

        // （fill_gc_queueなどで）1つの永続化済みDeleteまたはDeleteRangeレコードを発見した。
        // これはopen中に見つけたDelete(Range)レコードの１つ目に対応する。
        info.detect_new_synced_delete_records(1);
        assert_eq!(info.releasable_data_portions(), vec![]);

        // 新たに2つの永続化済みDeleteまたはDeleteRangeレコードを発見した。
        // open中に見つけた２つ目のDelete(Range)レコードに加えて、
        // open以降に新たに削除されたレコードも見つけられたことを意味する。
        info.detect_new_synced_delete_records(2);
        assert_eq!(
            info.releasable_data_portions(),
            vec![make_dataportion(0, 0)]
        );

        // 解放可能化不明なデータポーション群が1つ登録されたとする。
        // 実際にはDeleteRangeにより一括で複数削除されることに対応する。
        let portions: Vec<DataPortion> = (20..30).map(|i| make_dataportion(i, i)).collect();
        info.insert_data_portions(portions.clone());

        // 新たに3つの永続化済みDeleteまたはDeleteRangeレコードを発見した。
        // 全ての内部バッファに登録済みの領域が安全に解放可能な状態に到達したことに等しい。
        info.detect_new_synced_delete_records(3);

        let mut tmp = vec![
            make_dataportion(0, 0),
            make_dataportion(1, 1),
            make_dataportion(2, 2),
        ];
        tmp.extend(portions.clone());
        assert_eq!(info.releasable_data_portions(), tmp);

        Ok(())
    }

    #[test]
    /*
     * releasable_data_portions_worksと類似したテストの
     * take_releasable_data_portionsバージョン。
     */
    fn take_releasable_data_portions_works() -> TestResult {
        let mut info = DelayedReleaseInfo::new();

        info.set_initial_delete_records(2);

        info.insert_data_portions(vec![make_dataportion(0, 0)]);
        info.insert_data_portions(vec![make_dataportion(1, 1)]);
        info.insert_data_portions(vec![make_dataportion(2, 2)]);

        info.detect_new_synced_delete_records(1);
        assert_eq!(info.releasable_data_portions(), vec![]);

        info.detect_new_synced_delete_records(2);
        assert_eq!(
            info.take_releasable_data_portions(),
            vec![make_dataportion(0, 0)]
        );
        assert_eq!(info.releasable_data_portions(), vec![]);

        let portions: Vec<DataPortion> = (20..30).map(|i| make_dataportion(i, i)).collect();

        info.insert_data_portions(portions.clone());
        info.detect_new_synced_delete_records(3);

        let mut tmp = vec![make_dataportion(1, 1), make_dataportion(2, 2)];
        tmp.extend(portions.clone());
        assert_eq!(info.take_releasable_data_portions(), tmp);
        assert_eq!(info.releasable_data_portions(), vec![]);

        Ok(())
    }
}
