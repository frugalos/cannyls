#![feature(test)]
extern crate byteorder;
extern crate cannyls;
extern crate futures;
extern crate test;
#[macro_use]
extern crate trackable;
#[macro_use]
extern crate slog;

use cannyls::device::DeviceBuilder;
use cannyls::lump::{LumpData, LumpId};
use cannyls::nvm::MemoryNvm;
use cannyls::storage::StorageBuilder;
use cannyls::{Error, Result};
use futures::Future;
use slog::{Discard, Logger};
use test::Bencher;

fn id(id: usize) -> LumpId {
    LumpId::new(id as u128)
}

fn wait<F: Future<Error = Error>>(mut f: F) -> Result<()> {
    while !track!(f.poll())?.is_ready() {}
    Ok(())
}

#[bench]
fn memory_put_small(b: &mut Bencher) {
    let nvm = MemoryNvm::new(vec![0; 1024 * 1024 * 1024]);
    let storage = track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm));
    let logger = Logger::root(Discard, o!());
    let device = DeviceBuilder::new().spawn(|| Ok(storage), logger, None);
    let d = device.handle();
    let _ = wait(d.request().wait_for_running().list()); // デバイスの起動を待機

    let mut i = 0;
    let data = LumpData::new_embedded("foo".into()).unwrap();
    b.iter(|| {
        track_try_unwrap!(wait(d.request().put(id(i), data.clone())));
        i += 1;
    });
}

#[bench]
fn memory_put_and_delete_small(b: &mut Bencher) {
    let nvm = MemoryNvm::new(vec![0; 1024 * 1024 * 1024]);
    let storage = track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.99).create(nvm));
    let logger = Logger::root(Discard, o!());
    let device = DeviceBuilder::new().spawn(|| Ok(storage), logger, None);
    let d = device.handle();
    let _ = wait(d.request().wait_for_running().list()); // デバイスの起動を待機

    let mut i = 0;
    let data = LumpData::new_embedded("foo".into()).unwrap();
    b.iter(|| {
        let future0 = d.request().put(id(i), data.clone());
        let future1 = d.request().delete(id(i));
        track_try_unwrap!(wait(future0));
        track_try_unwrap!(wait(future1));
        i += 1;
    });
}
