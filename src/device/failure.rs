use std::time::Duration;

/// How does CannyLS act on failure?
pub struct FailurePolicy {
    /// Configuration for IO latencies
    pub io_latency_threshold: IOLatencyThreshold,

    /// Configuration for IO errors
    pub io_error_threshold: IOErrorThreshold,

    /// How device errors are handled
    pub takedown_policy: TakedownPolicy,
}

/// Configuration for IO latencies
pub struct IOLatencyThreshold {
    /// How many operations do we use for averaging?
    pub count: u64,
    /// Limit of time consumed. Probably better be Duration.
    pub time_milli_limit: u64,
}

/// Configuration for IO errors
pub struct IOErrorThreshold {
    /// Limit of the number of failures.
    pub count_limit: u64,
    /// Duration during which failures are counted.
    pub duration: Duration,
}

/// How device errors are handled
pub enum TakedownPolicy {
    /// Stop on error at once
    Stop,
    /// hold termination until x failures happen
    Tolerance(u64),
    /// Keep running on error
    Keep,
}
