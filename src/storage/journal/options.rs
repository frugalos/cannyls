use block::BlockSize;

/// ジャーナル領域の挙動を調整するためのパラメータ群.
///
/// 各オプションの説明は`StorageBuilder'のドキュメントを参照のこと.
#[derive(Debug, Clone, Copy)]
pub struct JournalRegionOptions {
    pub gc_queue_size: usize,
    pub sync_interval: usize,
    pub block_size: BlockSize,
    pub buffer_options: JournalBufferOptions,
}
impl Default for JournalRegionOptions {
    fn default() -> Self {
        JournalRegionOptions {
            gc_queue_size: 0x1000,
            sync_interval: 0x1000,
            block_size: BlockSize::min(),
            buffer_options: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct JournalBufferOptions {
    pub safe_flush: bool,
    pub safe_enqueue: bool,
}
impl Default for JournalBufferOptions {
    fn default() -> Self {
        JournalBufferOptions {
            safe_flush: false,
            safe_enqueue: false,
        }
    }
}
