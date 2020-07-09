/// デバイスのキューが長い場合にどうするか
/// default は RefuseNewRequests
#[derive(Debug, Clone)]
pub enum LongQueuePolicy {
    /// 新しいリクエストを拒否する
    RefuseNewRequests,

    /// デバイスを止める
    Stop,

    /// 一定の割合でリクエストをドロップする。
    ///
    /// ratio としてドロップ率 (0 以上 1 以下) を決める。
    /// 本当はもっと柔軟にやったほうがいいかもしれないが、当面固定値で問題ないだろうと思われる。
    ///
    /// TODO: ドロップ率を真面目に計算する
    Drop {
        /// ドロップ率
        ratio: f64,
    },
}

impl Default for LongQueuePolicy {
    fn default() -> Self {
        LongQueuePolicy::RefuseNewRequests
    }
}
