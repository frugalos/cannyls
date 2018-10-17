use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use uuid::Uuid;

use block::BlockSize;
use nvm::NonVolatileMemory;
use storage::{
    MAGIC_NUMBER, MAJOR_VERSION, MAX_DATA_REGION_SIZE, MAX_JOURNAL_REGION_SIZE, MINOR_VERSION,
};
use {ErrorKind, Result};

/// ヘッダを表現するのに必要なバイト数.
const HEADER_SIZE: u16 =
    2 /* major_version */ +
    2 /* minor_version */ +
    2 /* block_size */ +
    16 /* UUID */ +
    8 /* journal_region_size */ +
    8 /* data_region_size */;

/// **マジックナンバー** と **ヘッダサイズ** も含めたサイズ.
pub(crate) const FULL_HEADER_SIZE: u16 = 4 + 2 + HEADER_SIZE;

/// ストレージのヘッダ情報.
///
/// # 参考
///
/// - [ストレージフォーマット(v1.0)][format]
///
/// [format]: https://github.com/frugalos/cannyls/wiki/Storage-Format
#[derive(Debug, Clone)]
pub struct StorageHeader {
    /// メジャーバージョン.
    ///
    /// メジャーバージョンが異なるストレージ同士のデータ形式には互換性が無い.
    ///
    /// 現在の最新バージョンは[`MAJOR_VERSION`](./constant.MAJOR_VERSION.html).
    pub major_version: u16,

    /// マイナーバージョン.
    ///
    /// マイナーバージョンには、後方互換性がある
    /// (i.e., 古い形式で作成されたストレージを、新しいプログラムで扱うことが可能).
    ///
    /// 現在の最新バージョンは[`MINOR_VERSION`](./constant.MINOR_VERSION.html).
    pub minor_version: u16,

    /// ストレージのブロックサイズ.
    pub block_size: BlockSize,

    /// ストレージの特定のインスタンスを識別するためのUUID.
    pub instance_uuid: Uuid,

    /// ジャーナル領域のサイズ(バイト単位).
    pub journal_region_size: u64,

    /// データ領域のサイズ(バイト単位).
    pub data_region_size: u64,
}
impl StorageHeader {
    /// ストレージが使用する領域全体のサイズを返す.
    ///
    /// 内訳としては **ヘッダ領域** と **ジャーナル領域** 、 **データ領域** のサイズの合計となる.
    pub fn storage_size(&self) -> u64 {
        self.region_size() + self.journal_region_size + self.data_region_size
    }

    /// ヘッダ領域のサイズを返す.
    ///
    /// **ヘッダ領域** には、以下が含まれる:
    ///
    /// - マジックナンバー
    /// - ヘッダ長
    /// - `StorageHeader`
    /// - 領域のサイズをブロック境界に揃えるためのパディング
    pub fn region_size(&self) -> u64 {
        Self::calc_region_size(self.block_size)
    }

    /// 存在するLump Storageから
    /// 保存済みのストレージヘッダを取り出す。
    pub fn read_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = track_io!(File::open(path))?;
        track!(Self::read_from(file))
    }

    /// ヘッダ情報を`reader`から読み込む.
    pub fn read_from<R: Read>(mut reader: R) -> Result<Self> {
        // magic number
        let mut magic_number = [0; 4];
        track_io!(reader.read_exact(&mut magic_number))?;
        track_assert_eq!(magic_number, MAGIC_NUMBER, ErrorKind::InvalidInput);

        // header size
        let header_size = track_io!(reader.read_u16::<BigEndian>())?;
        let mut reader = reader.take(u64::from(header_size));

        // versions
        let major_version = track_io!(reader.read_u16::<BigEndian>())?;
        let minor_version = track_io!(reader.read_u16::<BigEndian>())?;
        track_assert_eq!(
            major_version,
            MAJOR_VERSION,
            ErrorKind::InvalidInput,
            "Unsupported major version",
        );
        track_assert!(
            minor_version <= MINOR_VERSION,
            ErrorKind::InvalidInput,
            "Unsupported minor version: actual={}, supported={}",
            minor_version,
            MINOR_VERSION
        );

        // block_size
        let block_size = track_io!(reader.read_u16::<BigEndian>())?;
        let block_size = track!(BlockSize::new(block_size), "block_size:{}", block_size)?;

        // UUID
        let mut instance_uuid = [0; 16];
        track_io!(reader.read_exact(&mut instance_uuid))?;
        let instance_uuid = Uuid::from_bytes(instance_uuid);

        // region sizes
        let journal_region_size = track_io!(reader.read_u64::<BigEndian>())?;
        let data_region_size = track_io!(reader.read_u64::<BigEndian>())?;
        track_assert!(
            journal_region_size <= MAX_JOURNAL_REGION_SIZE,
            ErrorKind::InvalidInput,
            "journal_region_size:{}",
            journal_region_size
        );
        track_assert!(
            data_region_size <= MAX_DATA_REGION_SIZE,
            ErrorKind::InvalidInput,
            "data_region_size:{}",
            data_region_size
        );

        track_assert_eq!(reader.limit(), 0, ErrorKind::InvalidInput);
        Ok(StorageHeader {
            major_version,
            minor_version,
            instance_uuid,
            block_size,
            journal_region_size,
            data_region_size,
        })
    }

    /// ヘッダ情報を`writer`に書き込む.
    pub fn write_to<W: Write>(&self, mut writer: W) -> Result<()> {
        track_io!(writer.write_all(&MAGIC_NUMBER[..]))?;
        track_io!(writer.write_u16::<BigEndian>(HEADER_SIZE))?;
        track_io!(writer.write_u16::<BigEndian>(self.major_version))?;
        track_io!(writer.write_u16::<BigEndian>(self.minor_version))?;
        track_io!(writer.write_u16::<BigEndian>(self.block_size.as_u16()))?;
        track_io!(writer.write_all(self.instance_uuid.as_bytes()))?;
        track_io!(writer.write_u64::<BigEndian>(self.journal_region_size))?;
        track_io!(writer.write_u64::<BigEndian>(self.data_region_size))?;
        Ok(())
    }

    /// ヘッダ領域を`writer`に書き込む.
    ///
    /// ヘッダ領域(サイズは`self.region_size()`)の未使用部分に0-パディングを行う以外は、
    /// `write_to`メソッドと同様.
    pub(crate) fn write_header_region_to<W: Write>(&self, mut writer: W) -> Result<()> {
        track!(self.write_to(&mut writer))?;

        let padding = vec![0; self.region_size() as usize - FULL_HEADER_SIZE as usize];
        track_io!(writer.write_all(&padding))?;
        Ok(())
    }

    /// 指定されたブロックサイズを有するストレージのために必要な、ヘッダ領域のサイズを計算する.
    pub(crate) fn calc_region_size(block_size: BlockSize) -> u64 {
        block_size.ceil_align(u64::from(FULL_HEADER_SIZE))
    }

    /// 不揮発性メモリ全体の領域を分割して、ジャーナル領域およびデータ領域用のメモリを返す.
    pub(crate) fn split_regions<N: NonVolatileMemory>(&self, nvm: N) -> Result<(N, N)> {
        let header_tail = self.region_size();
        let (_, body_nvm) = track!(nvm.split(header_tail))?;
        let (journal_nvm, data_nvm) = track!(body_nvm.split(self.journal_region_size))?;
        Ok((journal_nvm, data_nvm))
    }
}

#[cfg(test)]
mod tests {
    use trackable::result::TestResult;
    use uuid::Uuid;

    use super::*;
    use block::BlockSize;

    #[test]
    fn it_works() -> TestResult {
        let header = StorageHeader {
            major_version: MAJOR_VERSION,
            minor_version: MINOR_VERSION,
            block_size: BlockSize::min(),
            instance_uuid: Uuid::new_v4(),
            journal_region_size: 1024,
            data_region_size: 4096,
        };

        // size
        assert_eq!(header.region_size(), u64::from(BlockSize::MIN));
        assert_eq!(
            header.storage_size(),
            u64::from(BlockSize::MIN) + 1024 + 4096
        );

        // read/write
        let mut buf = Vec::new();
        track!(header.write_to(&mut buf))?;

        let h = track!(StorageHeader::read_from(&buf[..]))?;
        assert_eq!(h.major_version, header.major_version);
        assert_eq!(h.minor_version, header.minor_version);
        assert_eq!(h.block_size, header.block_size);
        assert_eq!(h.instance_uuid, header.instance_uuid);
        assert_eq!(h.journal_region_size, header.journal_region_size);
        assert_eq!(h.data_region_size, header.data_region_size);
        Ok(())
    }

    #[test]
    fn compatibility_check_works() -> TestResult {
        // Lower minor version: OK
        let h = header(MAJOR_VERSION, MINOR_VERSION - 1);
        let mut buf = Vec::new();
        track!(h.write_to(&mut buf))?;

        let h = track!(StorageHeader::read_from(&buf[..]))?;
        assert_eq!(h.major_version, MAJOR_VERSION);
        assert_eq!(h.minor_version, MINOR_VERSION - 1);

        // Current version: OK
        let h = header(MAJOR_VERSION, MINOR_VERSION);
        let mut buf = Vec::new();
        track!(h.write_to(&mut buf))?;

        let h = track!(StorageHeader::read_from(&buf[..]))?;
        assert_eq!(h.major_version, MAJOR_VERSION);
        assert_eq!(h.minor_version, MINOR_VERSION);

        // Higher minor version: NG
        let h = header(MAJOR_VERSION, MINOR_VERSION + 1);
        let mut buf = Vec::new();
        track!(h.write_to(&mut buf))?;

        assert!(StorageHeader::read_from(&buf[..]).is_err());

        // Higher major version: NG
        let h = header(MAJOR_VERSION + 1, MINOR_VERSION);
        let mut buf = Vec::new();
        track!(h.write_to(&mut buf))?;

        // Lower major version: NG
        let h = header(MAJOR_VERSION - 1, MINOR_VERSION);
        let mut buf = Vec::new();
        track!(h.write_to(&mut buf))?;

        assert!(StorageHeader::read_from(&buf[..]).is_err());

        Ok(())
    }

    fn header(major_version: u16, minor_version: u16) -> StorageHeader {
        StorageHeader {
            major_version,
            minor_version,
            block_size: BlockSize::min(),
            instance_uuid: Uuid::new_v4(),
            journal_region_size: 1024,
            data_region_size: 4096,
        }
    }
}
