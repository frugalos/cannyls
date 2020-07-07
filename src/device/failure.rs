use std::time::Duration;

/// How does CannyLS act on failure?
#[derive(Debug, Clone)]
pub struct FailurePolicy {
    /// Configuration for IO latencies
    pub io_latency_threshold: IOLatencyThreshold,

    /// Configuration for IO errors
    pub io_error_threshold: IOErrorThreshold,

    /// How device errors are handled
    pub takedown_policy: TakedownPolicy,
}

/// Configuration for IO latencies
#[derive(Debug, Clone)]
pub struct IOLatencyThreshold {
    /// How many operations do we use for averaging?
    pub count: u32,
    /// Limit of time consumed.
    pub time_limit: Duration,
}

impl Default for IOLatencyThreshold {
    fn default() -> Self {
        Self {
            count: 100,
            time_limit: Duration::from_millis(2000),
        }
    }
}

/// Configuration for IO errors
#[derive(Debug, Clone)]
pub struct IOErrorThreshold {
    /// Limit of the number of failures.
    pub count_limit: u32,
    /// Duration during which failures are counted.
    pub duration: Duration,
}

impl Default for IOErrorThreshold {
    fn default() -> Self {
        Self {
            count_limit: 10,
            duration: Duration::from_millis(1000),
        }
    }
}
/// How device errors are handled
#[derive(Debug, Clone)]
pub enum TakedownPolicy {
    /// Stop on error at once
    Stop,
    /// hold termination until x failures happen
    Tolerance(u64),
    /// Keep running on error
    Keep,
}
