#![feature(test)]
extern crate byteorder;
extern crate cannyls;
extern crate tempdir;
extern crate test;
#[macro_use]
extern crate trackable;

use cannyls::lump::{LumpData, LumpId};
use cannyls::nvm::{FileNvm, MemoryNvm};
use cannyls::storage::StorageBuilder;
use tempdir::TempDir;
use test::Bencher;

fn id(id: usize) -> LumpId {
    LumpId::new(id as u128)
}

#[bench]
fn file_put_small(b: &mut Bencher) {
    let dir = TempDir::new("cannyls_bench").unwrap();
    let nvm = FileNvm::create(dir.path().join("bench.lusf"), 1024 * 1024 * 1024).unwrap();
    let mut storage =
        track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.9).create(nvm));
    let mut i = 0;
    let data = LumpData::new_embedded("foo".into()).unwrap();
    b.iter(|| {
        track_try_unwrap!(storage.put(&id(i), &data));
        i += 1;
    });
}

#[bench]
fn file_put_small_no_embedded(b: &mut Bencher) {
    let dir = TempDir::new("cannyls_bench").unwrap();
    let nvm = FileNvm::create(dir.path().join("bench.lusf"), 1024 * 1024 * 1024).unwrap();
    let mut storage =
        track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.5).create(nvm));
    let mut i = 0;

    let data = storage.allocate_lump_data_with_bytes(b"foo").unwrap();
    b.iter(|| {
        track_try_unwrap!(storage.put(&id(i), &data));
        i += 1;
    });
}

#[bench]
fn memory_put_small(b: &mut Bencher) {
    let nvm = MemoryNvm::new(vec![0; 1024 * 1024 * 1024]);
    let mut storage =
        track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm));
    let mut i = 0;
    let data = LumpData::new_embedded("foo".into()).unwrap();
    b.iter(|| {
        track_try_unwrap!(storage.put(&id(i), &data));
        i += 1;
    });
}

#[bench]
fn memory_put_and_delete_small(b: &mut Bencher) {
    let nvm = MemoryNvm::new(vec![0; 1024 * 1024]);
    let mut storage =
        track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm));
    let mut i = 0;
    let data = LumpData::new_embedded("foo".into()).unwrap();
    b.iter(|| {
        let id = id(i);
        track_try_unwrap!(storage.put(&id, &data));
        track_try_unwrap!(storage.delete(&id));
        i += 1;
    });
}

#[bench]
fn memory_put_and_delete_small_no_embedded(b: &mut Bencher) {
    let nvm = MemoryNvm::new(vec![0; 1024 * 1024]);
    let mut storage =
        track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.5).create(nvm));
    let mut i = 0;
    let data = storage.allocate_lump_data_with_bytes(b"foo").unwrap();
    b.iter(|| {
        let id = id(i);
        track_try_unwrap!(storage.put(&id, &data));
        track_try_unwrap!(storage.delete(&id));
        i += 1;
    });
}

#[bench]
fn memory_put_and_get_and_delete_small(b: &mut Bencher) {
    let nvm = MemoryNvm::new(vec![0; 1024 * 1024]);
    let mut storage =
        track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm));
    let mut i = 0;
    let data = LumpData::new_embedded("foo".into()).unwrap();
    b.iter(|| {
        let id = id(i);
        track_try_unwrap!(storage.put(&id, &data));
        track_try_unwrap!(storage.get(&id));
        track_try_unwrap!(storage.delete(&id));
        i += 1;
    });
}
