#![feature(test)]
extern crate byteorder;
extern crate cannyls;
extern crate test;
#[macro_use]
extern crate trackable;

use cannyls::device::DeviceBuilder;
use cannyls::lump::{LumpData, LumpId};
use cannyls::nvm::MemoryNvm;
use cannyls::storage::StorageBuilder;
use test::Bencher;

fn id(id: usize) -> LumpId {
    LumpId::new(id as u128)
}

#[bench]
fn memory_put_small(b: &mut Bencher) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let nvm = MemoryNvm::new(vec![0; 1024 * 1024 * 1024]);
    let storage = track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm));
    let device = DeviceBuilder::new().spawn(|| Ok(storage));
    let d = device.handle();
    let _ = runtime.block_on(d.request().wait_for_running().list()); // デバイスの起動を待機

    let mut i = 0;
    let data = LumpData::new_embedded("foo".into()).unwrap();
    b.iter(|| {
        track_try_unwrap!(runtime.block_on(d.request().put(id(i), data.clone())));
        i += 1;
    });
}

#[bench]
fn memory_put_and_delete_small(b: &mut Bencher) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let nvm = MemoryNvm::new(vec![0; 1024 * 1024 * 1024]);
    let storage = track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm));
    let device = DeviceBuilder::new().spawn(|| Ok(storage));
    let d = device.handle();
    let _ = runtime.block_on(d.request().wait_for_running().list()); // デバイスの起動を待機

    let mut i = 0;
    let data = LumpData::new_embedded("foo".into()).unwrap();
    b.iter(|| {
        let future0 = d.request().put(id(i), data.clone());
        let future1 = d.request().delete(id(i));
        track_try_unwrap!(runtime.block_on(future0));
        track_try_unwrap!(runtime.block_on(future1));
        i += 1;
    });
}
