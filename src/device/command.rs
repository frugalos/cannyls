//! デバイスに発行されるコマンド群の定義.

use futures::channel::oneshot;
use futures::{Future, FutureExt};
use std::ops::Range;
use std::pin::Pin;
use std::sync::mpsc::{Receiver, Sender};
use std::task::{Context, Poll};
use trackable::error::ErrorKindExt;

use crate::deadline::Deadline;
use crate::lump::{LumpData, LumpHeader, LumpId};
use crate::storage::StorageUsage;
use crate::{Error, ErrorKind, Result};

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
    UsageRange(UsageLumpRange),
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
            Command::UsageRange(ref c) => c.deadline,
            Command::Stop(ref c) => c.deadline,
        }
    }
    pub fn prioritized(&self) -> bool {
        match *self {
            Command::Put(ref c) => c.prioritized,
            Command::Get(ref c) => c.prioritized,
            Command::Head(ref c) => c.prioritized,
            Command::Delete(ref c) => c.prioritized,
            Command::DeleteRange(ref c) => c.prioritized,
            Command::List(ref c) => c.prioritized,
            Command::ListRange(ref c) => c.prioritized,
            Command::UsageRange(ref c) => c.prioritized,
            Command::Stop(ref c) => c.prioritized,
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
            Command::UsageRange(c) => c.reply.send(Err(error)),
            Command::Stop(_) => {}
        }
    }
}

/// `Result`の非同期版.
#[derive(Debug)]
pub struct AsyncResult<T>(oneshot::Receiver<Result<T>>);
impl<T> AsyncResult<T> {
    #[allow(clippy::new_ret_no_self)]
    fn new() -> (AsyncReply<T>, Self) {
        let (tx, rx) = oneshot::channel();
        (AsyncReply(tx), AsyncResult(rx))
    }
}
impl<T> Future for AsyncResult<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        track!(self.0.poll_unpin(cx).map(|result| match result {
            Ok(Ok(x)) => Ok(x),
            Ok(Err(e)) => track!(Err(e)),
            Err(_) => track!(Err(ErrorKind::DeviceTerminated
                .cause("monitoring channel disconnected")
                .into())),
        }))
    }
}

#[derive(Debug)]
struct AsyncReply<T>(oneshot::Sender<Result<T>>);
impl<T> AsyncReply<T> {
    fn send(self, result: Result<T>) {
        let _ = self.0.send(result); // fails if the receiver has been dropped
    }
}

#[derive(Debug)]
pub struct PutLump {
    lump_id: LumpId,
    lump_data: LumpData,
    deadline: Deadline,
    prioritized: bool,
    journal_sync: bool,
    reply: AsyncReply<bool>,
}
impl PutLump {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        lump_id: LumpId,
        lump_data: LumpData,
        deadline: Deadline,
        prioritized: bool,
        journal_sync: bool,
    ) -> (Self, AsyncResult<bool>) {
        let (reply, result) = AsyncResult::new();
        let command = PutLump {
            lump_id,
            lump_data,
            deadline,
            prioritized,
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
    prioritized: bool,
    reply: AsyncReply<Option<LumpData>>,
}
impl GetLump {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        lump_id: LumpId,
        deadline: Deadline,
        prioritized: bool,
    ) -> (Self, AsyncResult<Option<LumpData>>) {
        let (reply, result) = AsyncResult::new();
        let command = GetLump {
            lump_id,
            deadline,
            prioritized,
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
    prioritized: bool,
    reply: AsyncReply<Option<LumpHeader>>,
}
impl HeadLump {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        lump_id: LumpId,
        deadline: Deadline,
        prioritized: bool,
    ) -> (Self, AsyncResult<Option<LumpHeader>>) {
        let (reply, result) = AsyncResult::new();
        let command = HeadLump {
            lump_id,
            deadline,
            prioritized,
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
    prioritized: bool,
    journal_sync: bool,
    reply: AsyncReply<bool>,
}
impl DeleteLump {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        lump_id: LumpId,
        deadline: Deadline,
        prioritized: bool,
        journal_sync: bool,
    ) -> (Self, AsyncResult<bool>) {
        let (reply, result) = AsyncResult::new();
        let command = DeleteLump {
            lump_id,
            deadline,
            prioritized,
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
    prioritized: bool,
    journal_sync: bool,
    reply: AsyncReply<Vec<LumpId>>,
}
impl DeleteLumpRange {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        range: Range<LumpId>,
        deadline: Deadline,
        prioritized: bool,
        journal_sync: bool,
    ) -> (Self, AsyncResult<Vec<LumpId>>) {
        let (reply, result) = AsyncResult::new();
        let command = DeleteLumpRange {
            range,
            deadline,
            prioritized,
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
    prioritized: bool,
    reply: AsyncReply<Vec<LumpId>>,
}
impl ListLump {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(deadline: Deadline, prioritized: bool) -> (Self, AsyncResult<Vec<LumpId>>) {
        let (reply, result) = AsyncResult::new();
        let command = ListLump {
            deadline,
            prioritized,
            reply,
        };
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
    prioritized: bool,
    reply: AsyncReply<Vec<LumpId>>,
}
impl ListLumpRange {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        range: Range<LumpId>,
        deadline: Deadline,
        prioritized: bool,
    ) -> (Self, AsyncResult<Vec<LumpId>>) {
        let (reply, result) = AsyncResult::new();
        let command = ListLumpRange {
            range,
            deadline,
            prioritized,
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
pub struct UsageLumpRange {
    range: Range<LumpId>,
    deadline: Deadline,
    prioritized: bool,
    reply: AsyncReply<StorageUsage>,
}
impl UsageLumpRange {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        range: Range<LumpId>,
        deadline: Deadline,
        prioritized: bool,
    ) -> (Self, AsyncResult<StorageUsage>) {
        let (reply, result) = AsyncResult::new();
        let command = UsageLumpRange {
            range,
            deadline,
            prioritized,
            reply,
        };
        (command, result)
    }
    pub fn lump_range(&self) -> Range<LumpId> {
        self.range.clone()
    }
    pub fn reply(self, result: Result<StorageUsage>) {
        self.reply.send(result);
    }
}

#[derive(Debug)]
pub struct StopDevice {
    deadline: Deadline,
    prioritized: bool,
}
impl StopDevice {
    pub fn new(deadline: Deadline, prioritized: bool) -> Self {
        StopDevice {
            deadline,
            prioritized,
        }
    }
}
