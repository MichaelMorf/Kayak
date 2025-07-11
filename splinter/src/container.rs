/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use std::cell::Cell;
use std::panic::*;
use std::rc::Rc;
use std::sync::Arc;

use super::proxy::ProxyDB;

use db::cycles;
use db::rpc::*;
use db::task::TaskState::*;
use db::task::{Task, TaskPriority, TaskState};

use db::e2d2::common::EmptyMetadata;
use db::e2d2::headers::UdpHeader;
use db::e2d2::interface::Packet;

use db::wireformat::OpType;

use sandstorm::db::DB;
use sandstorm::ext::Extension;

/// A container for untrusted code that can be scheduled by the database.
pub struct Container {
    // The current state of the task. Required to determine if the task
    // has completed execution.
    state: TaskState,

    // The priority of the task. Required to determine when the task should
    // be run next, if it has not completed already.
    priority: TaskPriority,

    // The total amount of time in cycles the task has run for. Required to
    // determine when the task should be run next, and for accounting purposes.
    time: u64,

    // The total amount of time in cycles the task has spend inside the database.
    // Required to determine the credit for each run of an extension.
    db_time: u64,

    // An execution context for the task that implements the DB trait. Required
    // for the task to interact with the database.
    db: Cell<Option<Rc<ProxyDB>>>,

    // A handle to the dynamically loaded extension. Required to initialize the
    // task, and ensure that the extension stays loaded for as long as the task
    // is executing in the system.
    ext: Arc<Extension>,

    // The identifier to uniquely identify a task.
    id: u64,
}

// Implementation of methods on Container.
impl Container {
    /// Creates a new container holding an untrusted extension that can be
    /// scheduled by the database.
    ///
    /// # Arguments
    ///
    /// * `prio`:    The priority of the container/task. Required by the
    ///              scheduler.
    /// * `context`: The execution context for the extension. Allows the
    ///              extension to interact with the database.
    /// * `ext`:     A handle to the extension that will be run inside this
    ///              container.
    ///
    /// # Return
    ///
    /// A container that when scheduled, runs the extension.
    pub fn new(
        prio: TaskPriority,
        context: Rc<ProxyDB>,
        ext: Arc<Extension>,
        id: u64,
    ) -> Container {
        Container {
            state: INITIALIZED,
            priority: prio,
            time: 0,
            db_time: 0,
            db: Cell::new(Some(context)),
            ext: ext,
            id: id,
        }
    }
}

// Implementation of the Task trait for Container.
impl Task for Container {
    /// Refer to the Task trait for Documentation.
    fn run(&mut self) -> (TaskState, u64) {
        // Mark as completed immediately (stub implementation)
        self.state = COMPLETED;
        (self.state, 0)
    }

    /// Refer to the Task trait for Documentation.
    fn state(&self) -> TaskState {
        self.state.clone()
    }

    /// Refer to the Task trait for Documentation.
    fn time(&self) -> u64 {
        self.time.clone()
    }

    /// Refer to the Task trait for Documentation.
    fn db_time(&self) -> u64 {
        self.db_time.clone()
    }

    /// Refer to the Task trait for Documentation.
    fn priority(&self) -> TaskPriority {
        self.priority.clone()
    }

    /// Refer to the Task trait for Documentation.
    unsafe fn tear(
        &mut self,
    ) -> Option<(
        Packet<UdpHeader, EmptyMetadata>,
        Packet<UdpHeader, EmptyMetadata>,
    )> {
        None
    }

    /// Refer to the `Task` trait for Documentation.
    fn set_state(&mut self, state: TaskState) {
        self.state = state;
    }

    /// Refer to the `Task` trait for Documentation.
    fn update_cache(&mut self, record: &[u8], keylen: usize) {
        if let Some(proxydb) = self.db.get_mut() {
            match parse_record_optype(record) {
                OpType::SandstormRead => proxydb.set_read_record(record.split_at(1).1, keylen),

                OpType::SandstormWrite => proxydb.set_write_record(record.split_at(1).1, keylen),

                _ => {}
            }
        }
    }

    /// Refer to the `Task` trait for Documentation.
    fn get_id(&self) -> u64 {
        self.id.clone()
    }
}
