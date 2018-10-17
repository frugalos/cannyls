use std::cmp;
use std::collections::BinaryHeap;
use std::time::Instant;

use deadline::Deadline;
use device::command::Command;

/// デバイスに発行されたコマンド群の管理キュー.
///
/// `LumpDevice`の実装インスタンスは、 各コマンドを同期的に処理するため、
/// 並行的に発行されたコマンド群は、 順番が来るまでは、このキューによって保持されることになる.
///
/// スケジューリングはデッドラインベースで行われ、
/// デバイスに対して並行的に発行されたコマンド群は、
/// そのデッドラインが近い順に実行される.
///
/// なお、これが行うのはあくまでも並び替えのみで、
/// デッドラインを過ぎたコマンドの破棄は行わない.
#[derive(Debug)]
pub struct DeadlineQueue {
    seqno: u64,
    heap: BinaryHeap<Item>,
}
impl DeadlineQueue {
    /// 新しい`DeadlineQueue`インスタンスを生成する.
    pub fn new() -> Self {
        DeadlineQueue {
            seqno: 0,
            heap: BinaryHeap::new(),
        }
    }

    /// 新しいコマンドをキューに追加する.
    pub fn push(&mut self, command: Command) {
        let deadline = AbsoluteDeadline::new(command.deadline());
        let item = Item {
            seqno: self.seqno,
            command,
            deadline,
        };
        self.heap.push(item);
        self.seqno += 1;
    }

    /// 次に処理するコマンドを取り出す.
    pub fn pop(&mut self) -> Option<Command> {
        self.heap.pop().map(|t| t.command)
    }

    /// キューに格納されている要素数を返す.
    pub fn len(&self) -> usize {
        self.heap.len()
    }
}

/// ヒープに格納する要素.
#[derive(Debug)]
struct Item {
    seqno: u64, // デッドラインが同じ要素をFIFO順で扱うためのシーケンス番号
    command: Command,
    deadline: AbsoluteDeadline,
}
impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.seqno == other.seqno
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Item {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.seqno.cmp(&self.seqno))
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum AbsoluteDeadline {
    Immediate,
    Until(Instant),
    Infinity,
}
impl AbsoluteDeadline {
    fn new(relative: Deadline) -> Self {
        match relative {
            Deadline::Immediate => AbsoluteDeadline::Immediate,
            Deadline::Within(d) => AbsoluteDeadline::Until(Instant::now() + d),
            Deadline::Infinity => AbsoluteDeadline::Infinity,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;
    use deadline::Deadline;
    use device::command::{Command, GetLump};
    use lump::LumpId;

    #[test]
    fn deadline_works() {
        let mut queue = DeadlineQueue::new();

        queue.push(command(0, Deadline::Infinity));
        queue.push(command(1, Deadline::Immediate));
        queue.push(command(2, Deadline::Within(Duration::from_millis(1))));
        thread::sleep(Duration::from_millis(5));
        queue.push(command(3, Deadline::Within(Duration::from_millis(0))));
        queue.push(command(4, Deadline::Immediate));

        assert_eq!(queue.len(), 5);
        assert_eq!(lump_id(queue.pop()), Some(1));
        assert_eq!(lump_id(queue.pop()), Some(4)); // デッドラインが同じならFIFO順
        assert_eq!(lump_id(queue.pop()), Some(2));
        assert_eq!(lump_id(queue.pop()), Some(3));
        assert_eq!(lump_id(queue.pop()), Some(0));
        assert_eq!(lump_id(queue.pop()), None);
        assert_eq!(queue.len(), 0);
    }

    fn command(lump_id: u128, deadline: Deadline) -> Command {
        Command::Get(GetLump::new(LumpId::new(lump_id), deadline).0)
    }

    fn lump_id(command: Option<Command>) -> Option<u128> {
        command.map(|c| {
            if let Command::Get(c) = c {
                c.lump_id().as_u128()
            } else {
                unreachable!()
            }
        })
    }
}
