//! データ領域用のアロケータ.
//!
//! アロケータは、データ格納用に利用可能な連続した領域を（仮想的に）受け取り、
//! 個々のlumpに対して、その中から必要なサイズの部分領域（Portion）を割り当てる責務を負っている。
//!
//! アロケータが担当するのは、領域の計算処理のみで、実際のデータの読み書き等を、この中で行うことは無い.
pub use self::data_portion_allocator::DataPortionAllocator;

mod data_portion_allocator;
mod free_portion;

/// 24bit幅の整数.
type U24 = u32;
