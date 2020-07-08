use super::failure::{IOErrorThreshold, IOLatencyThreshold};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(crate) struct ExecutionObserver {
    // 時間の集計。直近 n 回の IO にかかった時間を持つ。
    io_latency_threshold: IOLatencyThreshold,
    last_io_duration: VecDeque<Duration>,
    last_io_duration_sum: Duration,

    // エラー回数の集計。直近 n 回のエラーが起きた時刻を管理する。
    io_error_threshold: IOErrorThreshold,
    last_io_errors: VecDeque<Instant>,
}

impl ExecutionObserver {
    pub fn new(
        io_latency_threshold: IOLatencyThreshold,
        io_error_threshold: IOErrorThreshold,
    ) -> Self {
        Self {
            io_latency_threshold,
            last_io_duration: VecDeque::new(),
            last_io_duration_sum: Duration::from_secs(0),
            io_error_threshold,
            last_io_errors: VecDeque::new(),
        }
    }
    pub fn observe(&mut self, duration: Duration, time: Instant, has_error: bool) {
        self.last_io_duration.push_back(duration);
        self.last_io_duration_sum += duration;
        while self.last_io_duration.len() > self.io_latency_threshold.count as usize {
            let first = self.last_io_duration.pop_front().unwrap();
            self.last_io_duration_sum -= first;
        }
        if has_error {
            self.last_io_errors.push_back(time);
        }
        while self.last_io_errors.len() > self.io_error_threshold.count_limit as usize {
            let _ = self.last_io_errors.pop_front().unwrap();
        }
    }

    pub fn is_failing(&self) -> bool {
        // 時間は ok?
        if !self.last_io_duration.is_empty() {
            let count = self.io_latency_threshold.count;
            let average = self.io_latency_threshold.time_limit;
            // 直近 count 回の平均が average 以上であれば、true を返す。
            // IO の回数が count に満たない場合は、確実に今後 count 回で average 以上になる、
            // つまりすでに count * average だけ時間を消費している場合にエラーを返す。
            if self.last_io_duration_sum >= count * average {
                return true;
            }
        }
        // エラー回数は ok?
        if !self.last_io_errors.is_empty() {
            let &first_error = self.last_io_errors.front().unwrap();
            let &last_error = self.last_io_errors.back().unwrap();
            let count = self.io_error_threshold.count_limit;
            // もしエラーが count 個以上あって、最初と最後のエラーの間が duration 以下であったならば、エラーが設定値以上の頻度で起きている。
            if self.last_io_errors.len() >= count as usize
                && last_error - first_error >= self.io_error_threshold.duration
            {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_works() {
        // 10 回の平均が 2 秒以上になったらダメ
        let io_latency_threshold = IOLatencyThreshold {
            count: 10,
            time_limit: Duration::from_secs(2),
        };
        // 2 秒間に 10 回以上エラーが起きていたらダメ
        // 任意の 2 秒間なので、間隔 20/9 秒以下で 10 回連続でエラーが発生したら 2 秒間に 10 回起きた判定になることに注意。
        let io_error_threshold = IOErrorThreshold {
            duration: Duration::from_secs(2),
            count_limit: 10,
        };

        let mut execution_observer =
            ExecutionObserver::new(io_latency_threshold.clone(), io_error_threshold.clone());
        // 2.1 秒かかる I/O を 10 回実行する
        for _ in 0..10 {
            execution_observer.observe(Duration::from_millis(2100), Instant::now(), false);
        }
        assert!(execution_observer.is_failing());

        let mut execution_observer =
            ExecutionObserver::new(io_latency_threshold.clone(), io_error_threshold.clone());
        //  1.9 秒かかる I/O を 10 回実行する
        for _ in 0..10 {
            execution_observer.observe(Duration::from_millis(1900), Instant::now(), false);
        }
        assert!(!execution_observer.is_failing());

        let mut execution_observer =
            ExecutionObserver::new(io_latency_threshold, io_error_threshold);
        // 間隔 2.1 秒 (< 20/9) で 10 回エラーを起こす
        let now = Instant::now();
        for i in 0..10 {
            execution_observer.observe(
                Duration::from_secs(0),
                now + Duration::from_millis(2100) * i,
                true,
            );
        }
        assert!(execution_observer.is_failing());
    }
}
