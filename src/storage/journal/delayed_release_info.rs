use std::convert::Into;
use std::vec::Vec;
use storage::portion::{DataPortion, DataPortionU64};

/// 削除操作によって消されたDataPortionを表すためのVec<DataPortion64>に対するnew type。
#[derive(Debug, Clone)]
struct DataPortion64s(Vec<DataPortionU64>);

impl DataPortion64s {
    /// 対応するDataPortionのリストを復元する関数。
    pub fn to_data_portions(&self) -> Vec<DataPortion> {
        self.0.iter().cloned().map(Into::into).collect()
    }
}

/// 遅延解放のために必要になる情報と操作を集約する構造体。
#[derive(Debug)]
pub struct DelayedReleaseInfo {
    // ジャーナル領域をopenした以降に追加したDelete及びDeleteRangeレコードで、永続化が確定済みのものの数
    num_of_releasable_delete_records: usize,
    // ジャーナル領域のrestore時に処理済みのDelete及びDeleteRangeレコードで、未だGCキューに投入されていないものの数
    num_of_unqueued_initial_delete_records: usize,
    // 永続化済みのDelete及びDeleteRangeレコードに紐づく、解放可能なPortionを表現するための集合
    entries: Vec<DataPortion64s>,
}
impl DelayedReleaseInfo {
    pub fn new() -> Self {
        DelayedReleaseInfo {
            num_of_releasable_delete_records: 0,
            num_of_unqueued_initial_delete_records: 0,
            entries: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub(in storage::journal) fn num_of_releasable_delete_records(&self) -> usize {
        self.num_of_releasable_delete_records
    }

    #[allow(dead_code)]
    pub(in storage::journal) fn num_of_unqueued_initial_delete_records(&self) -> usize {
        self.num_of_unqueued_initial_delete_records
    }

    pub fn releasable_data_portions(&self) -> Vec<DataPortion> {
        let n = self.num_of_releasable_delete_records;
        (&self.entries)[..n]
            .iter()
            .flat_map(DataPortion64s::to_data_portions)
            .collect()
    }

    pub fn take_releasable_data_portions(&mut self) -> Vec<DataPortion> {
        let n = self.num_of_releasable_delete_records;
        self.num_of_releasable_delete_records = 0;
        self.entries
            .drain(..n)
            .flat_map(|e| DataPortion64s::to_data_portions(&e))
            .collect()
    }

    pub fn set_initial_delete_records(&mut self, u: usize) {
        self.num_of_unqueued_initial_delete_records = u;
    }

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

    pub fn insert_data_portions(&mut self, portions: Vec<DataPortion>) {
        self.entries.push(DataPortion64s(
            portions.into_iter().map(Into::into).collect(),
        ));
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

        // Journal領域のopen中に、併せて5つのDeleteまたはDeleteRangeレコードとする。
        info.set_initial_delete_records(5);

        // fill_gc_queue中に、3つのDeleteまたはDeleteRangeレコードを見つけたとする。
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

        info.set_initial_delete_records(2);

        info.insert_data_portions(vec![make_dataportion(0, 0)]);
        info.insert_data_portions(vec![make_dataportion(1, 1)]);
        info.insert_data_portions(vec![make_dataportion(2, 2)]);

        info.detect_new_synced_delete_records(1);
        assert_eq!(info.releasable_data_portions(), vec![]);

        info.detect_new_synced_delete_records(2);
        assert_eq!(
            info.releasable_data_portions(),
            vec![make_dataportion(0, 0)]
        );

        let portions: Vec<DataPortion> = (20..30).map(|i| make_dataportion(i, i)).collect();

        info.insert_data_portions(portions.clone());
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
