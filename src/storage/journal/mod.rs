pub use self::delayed_release_info::DelayedReleaseInfo;
pub use self::header::{JournalHeader, JournalHeaderRegion};
pub use self::nvm_buffer::JournalNvmBuffer;
pub use self::options::JournalRegionOptions;
pub use self::record::{JournalEntry, JournalRecord};
pub use self::region::JournalRegion;

mod delayed_release_info;
mod header;
mod nvm_buffer;
mod options;
mod record;
mod region;
mod ring_buffer;

/// ジャーナル領域のスナップショット。
pub struct JournalSnapshot {
    /// ジャーナル領域の未開放開始位置。
    pub unreleased_head: u64,

    /// ジャーナル領域の開始位置。
    ///
    /// 古いジャーナルエントリを指す。
    pub head: u64,

    /// ジャーナル領域の末尾位置。
    ///
    /// 最新のジャーナルエントリの一つ後ろを指している。
    /// エントリはここ追記される。
    pub tail: u64,

    /// 開始位置から末尾位置まで順に読んで得られたジャーナルエントリ群。
    pub entries: Vec<JournalEntry>,
}
