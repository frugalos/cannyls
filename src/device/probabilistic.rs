use std::fmt::Debug;

/// リクエストを落としたり落とさなかったりする決定を下すオブジェクト。
pub(crate) trait Dropper: Debug {
    /// 次のリクエストを落とすなら true、落とさないなら false。
    /// 内部状態の変更も許されることに注意。
    fn will_drop(&mut self) -> bool;
}

/// あらかじめ指定した確率で落とす判定をする Dropper。
#[derive(Debug)]
pub(crate) struct ProbabilisticDropper {
    ratio: f64,
    counter: f64,
}

impl ProbabilisticDropper {
    pub fn new(ratio: f64) -> Self {
        Self {
            ratio,
            counter: 0.0,
        }
    }
}

impl Dropper for ProbabilisticDropper {
    fn will_drop(&mut self) -> bool {
        self.counter += self.ratio;
        // counter >= 1 であれば、counter -= 1 を行って落とす。
        // 毎回 counter が ratio だけ増えて、たまに 1 減るので、counter の大きさがそこまで変わらないため
        // 1 減る回数の割合は ratio に収束する。
        if self.counter >= 1.0 {
            self.counter -= 1.0;
            return true;
        }
        false
    }
}
