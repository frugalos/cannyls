use slog::Logger;
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
    logger: Logger,
    ratio: f64,
    counter: f64,
}

impl ProbabilisticDropper {
    pub fn new(logger: Logger, ratio: f64) -> Self {
        Self {
            logger,
            ratio,
            counter: 0.0,
        }
    }
}

impl Dropper for ProbabilisticDropper {
    fn will_drop(&mut self) -> bool {
        debug!(self.logger, "old counter = {}",self.counter; "ratio" => self.ratio);
        self.counter += self.ratio;
        // counter >= 1 であれば、counter -= 1 を行って落とす。
        // 毎回 counter が ratio だけ増えて、たまに 1 減るので、counter の大きさがそこまで変わらないため
        // 1 減る回数の割合は ratio に収束する。
        debug!(self.logger, "new counter = {}", self.counter; "ratio" => self.ratio);
        if self.counter >= 1.0 {
            self.counter -= 1.0;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probabilistic_dropper_works() {
        let ratio = 0.3;
        let mut dropper = ProbabilisticDropper::new(ratio);
        let n = 10000;
        // n 回実行しておよそ 3 割のリクエストが drop されることを確かめる。
        let mut dropped = 0;
        for _ in 0..n {
            if dropper.will_drop() {
                dropped += 1;
            }
        }
        assert!(2900 < dropped);
        assert!(dropped < 3100);
    }
}
