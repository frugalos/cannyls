use crate::block::BlockSize;

/// ジャーナル領域の挙動を調整するためのパラメータ群.
///
/// 各オプションの説明は`StorageBuilder'のドキュメントを参照のこと.
#[derive(Debug, Clone)]
pub struct JournalRegionOptions {
    pub gc_queue_size: usize,
    pub sync_interval: usize,
    pub block_size: BlockSize,
}
impl Default for JournalRegionOptions {
    fn default() -> Self {
        JournalRegionOptions {
            gc_queue_size: 0x1000,
            sync_interval: 0x1000,
            block_size: BlockSize::min(),
        }
    }
}
