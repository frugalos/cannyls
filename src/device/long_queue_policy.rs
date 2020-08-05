/// デバイスのキューが長い場合にどうするか
/// default は RefuseNewRequests
#[derive(Debug, Clone, PartialEq)]
pub enum LongQueuePolicy {
    /// 一定の割合で新しいリクエストを拒否する
    ///
    /// ratio として拒否率 (0 以上 1 以下) を決める。
    /// 本当はもっと柔軟にやったほうがいいかもしれないが、当面固定値で問題ないだろうと思われる。
    ///
    /// TODO: 拒否率を真面目に計算する
    RefuseNewRequests {
        /// 拒否率
        ratio: f64,
    },

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
        LongQueuePolicy::Stop
    }
}

impl LongQueuePolicy {
    /// 過負荷時にリクエストが実行されない確率を返す。
    /// 「実行されない」は、「拒否される」あるいは「ドロップされる」のいずれかを意味する。
    pub fn ratio(&self) -> f64 {
        match *self {
            LongQueuePolicy::RefuseNewRequests { ratio } => ratio,
            LongQueuePolicy::Stop => 0.0,
            LongQueuePolicy::Drop { ratio } => ratio,
        }
    }
}
