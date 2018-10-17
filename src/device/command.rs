//! デバイスに発行されるコマンド群の定義.
use fibers::sync::oneshot;
use futures::{Future, Poll};
use std::ops::Range;
use std::sync::mpsc::{Receiver, Sender};
use trackable::error::ErrorKindExt;

use deadline::Deadline;
use lump::{LumpData, LumpHeader, LumpId};
use {Error, ErrorKind, Result};

pub type CommandSender = Sender<Command>;
pub type CommandReceiver = Receiver<Command>;

#[derive(Debug)]
pub enum Command {
    Put(PutLump),
    Get(GetLump),
    Head(HeadLump),
    Delete(DeleteLump),
    DeleteRange(DeleteLumpRange),
    List(ListLump),
    ListRange(ListLumpRange),
    Stop(StopDevice),
}
impl Command {
    pub fn deadline(&self) -> Deadline {
        match *self {
            Command::Put(ref c) => c.deadline,
            Command::Get(ref c) => c.deadline,
            Command::Head(ref c) => c.deadline,
            Command::Delete(ref c) => c.deadline,
            Command::DeleteRange(ref c) => c.deadline,
            Command::List(ref c) => c.deadline,
            Command::ListRange(ref c) => c.deadline,
            Command::Stop(ref c) => c.deadline,
        }
    }
    pub fn failed(self, error: Error) {
        match self {
            Command::Put(c) => c.reply.send(Err(error)),
            Command::Get(c) => c.reply.send(Err(error)),
            Command::Head(c) => c.reply.send(Err(error)),
            Command::Delete(c) => c.reply.send(Err(error)),
            Command::DeleteRange(c) => c.reply.send(Err(error)),
            Command::List(c) => c.reply.send(Err(error)),
            Command::ListRange(c) => c.reply.send(Err(error)),
            Command::Stop(_) => {}
        }
    }
}

/// `Result`の非同期版.
#[derive(Debug)]
pub struct AsyncResult<T>(oneshot::Monitor<T, Error>);
impl<T> AsyncResult<T> {
    fn new() -> (AsyncReply<T>, Self) {
        let (tx, rx) = oneshot::monitor();
        (AsyncReply(tx), AsyncResult(rx))
    }
}
impl<T> Future for AsyncResult<T> {
    type Item = T;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        track!(self.0.poll().map_err(|e| e.unwrap_or_else(|| {
            ErrorKind::DeviceTerminated
                .cause("monitoring channel disconnected")
                .into()
        })))
    }
}

#[derive(Debug)]
struct AsyncReply<T>(oneshot::Monitored<T, Error>);
impl<T> AsyncReply<T> {
    fn send(self, result: Result<T>) {
        self.0.exit(result);
    }
}

#[derive(Debug)]
pub struct PutLump {
    lump_id: LumpId,
    lump_data: LumpData,
    deadline: Deadline,
    journal_sync: bool,
    reply: AsyncReply<bool>,
}
impl PutLump {
    pub fn new(
        lump_id: LumpId,
        lump_data: LumpData,
        deadline: Deadline,
        journal_sync: bool,
    ) -> (Self, AsyncResult<bool>) {
        let (reply, result) = AsyncResult::new();
        let command = PutLump {
            lump_id,
            lump_data,
            deadline,
            journal_sync,
            reply,
        };
        (command, result)
    }
    pub fn lump_id(&self) -> &LumpId {
        &self.lump_id
    }
    pub fn lump_data(&self) -> &LumpData {
        &self.lump_data
    }
    pub fn do_sync_journal(&self) -> bool {
        self.journal_sync
    }

    pub fn reply(self, result: Result<bool>) {
        self.reply.send(result)
    }
}

#[derive(Debug)]
pub struct GetLump {
    lump_id: LumpId,
    deadline: Deadline,
    reply: AsyncReply<Option<LumpData>>,
}
impl GetLump {
    pub fn new(lump_id: LumpId, deadline: Deadline) -> (Self, AsyncResult<Option<LumpData>>) {
        let (reply, result) = AsyncResult::new();
        let command = GetLump {
            lump_id,
            deadline,
            reply,
        };
        (command, result)
    }
    pub fn lump_id(&self) -> &LumpId {
        &self.lump_id
    }
    pub fn reply(self, result: Result<Option<LumpData>>) {
        self.reply.send(result);
    }
}

#[derive(Debug)]
pub struct HeadLump {
    lump_id: LumpId,
    deadline: Deadline,
    reply: AsyncReply<Option<LumpHeader>>,
}
impl HeadLump {
    pub fn new(lump_id: LumpId, deadline: Deadline) -> (Self, AsyncResult<Option<LumpHeader>>) {
        let (reply, result) = AsyncResult::new();
        let command = HeadLump {
            lump_id,
            deadline,
            reply,
        };
        (command, result)
    }
    pub fn lump_id(&self) -> &LumpId {
        &self.lump_id
    }
    pub fn reply(self, result: Result<Option<LumpHeader>>) {
        self.reply.send(result);
    }
}

#[derive(Debug)]
pub struct DeleteLump {
    lump_id: LumpId,
    deadline: Deadline,
    journal_sync: bool,
    reply: AsyncReply<bool>,
}
impl DeleteLump {
    pub fn new(
        lump_id: LumpId,
        deadline: Deadline,
        journal_sync: bool,
    ) -> (Self, AsyncResult<bool>) {
        let (reply, result) = AsyncResult::new();
        let command = DeleteLump {
            lump_id,
            deadline,
            journal_sync,
            reply,
        };
        (command, result)
    }
    pub fn lump_id(&self) -> &LumpId {
        &self.lump_id
    }
    pub fn do_sync_journal(&self) -> bool {
        self.journal_sync
    }
    pub fn reply(self, result: Result<bool>) {
        self.reply.send(result);
    }
}

#[derive(Debug)]
pub struct DeleteLumpRange {
    range: Range<LumpId>,
    deadline: Deadline,
    journal_sync: bool,
    reply: AsyncReply<Vec<LumpId>>,
}
impl DeleteLumpRange {
    pub fn new(
        range: Range<LumpId>,
        deadline: Deadline,
        journal_sync: bool,
    ) -> (Self, AsyncResult<Vec<LumpId>>) {
        let (reply, result) = AsyncResult::new();
        let command = DeleteLumpRange {
            range,
            deadline,
            journal_sync,
            reply,
        };
        (command, result)
    }
    pub fn lump_range(&self) -> Range<LumpId> {
        self.range.clone()
    }
    pub fn do_sync_journal(&self) -> bool {
        self.journal_sync
    }
    pub fn reply(self, result: Result<Vec<LumpId>>) {
        self.reply.send(result);
    }
}

#[derive(Debug)]
pub struct ListLump {
    deadline: Deadline,
    reply: AsyncReply<Vec<LumpId>>,
}
impl ListLump {
    pub fn new(deadline: Deadline) -> (Self, AsyncResult<Vec<LumpId>>) {
        let (reply, result) = AsyncResult::new();
        let command = ListLump { deadline, reply };
        (command, result)
    }
    pub fn reply(self, result: Result<Vec<LumpId>>) {
        self.reply.send(result);
    }
}

#[derive(Debug)]
pub struct ListLumpRange {
    range: Range<LumpId>,
    deadline: Deadline,
    reply: AsyncReply<Vec<LumpId>>,
}
impl ListLumpRange {
    pub fn new(range: Range<LumpId>, deadline: Deadline) -> (Self, AsyncResult<Vec<LumpId>>) {
        let (reply, result) = AsyncResult::new();
        let command = ListLumpRange {
            range,
            deadline,
            reply,
        };
        (command, result)
    }
    pub fn lump_range(&self) -> Range<LumpId> {
        self.range.clone()
    }
    pub fn reply(self, result: Result<Vec<LumpId>>) {
        self.reply.send(result);
    }
}

#[derive(Debug)]
pub struct StopDevice {
    deadline: Deadline,
}
impl StopDevice {
    pub fn new(deadline: Deadline) -> Self {
        StopDevice { deadline }
    }
}
