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

extern crate sandstorm;

use sandstorm::buf::{MultiReadBuf};
use sandstorm::db::DB;
use sandstorm::pack::pack;
use sandstorm::rc::Rc;
use sandstorm::size_of;
use sandstorm::vec::*;

/// Status codes for the response to the tenant.
const SUCCESSFUL: u8 = 0x01;
const INVALIDARG: u8 = 0x02;
const INVALIDKEY: u8 = 0x03;

const KEYLENGTH: u16 = 30;

macro_rules! GET1 {
    ($db:ident, $table:ident, $key:ident, $obj:ident) => {
        let (server, _, val) = $db.search_get_in_cache($table, &$key);
        if server == false {
            $obj = val;
        } else {
            $obj = $db.get($table, &$key);
        }
    };
}

macro_rules! MULTIGET1 {
    ($db:ident, $table:ident, $keylen:ident, $keys:ident, $buf:ident) => {
        let (server, _, val) = $db.search_multiget_in_cache($table, $keylen, $keys);
        if server == true {
            $buf = $db.multiget($table, $keylen, &$keys);
        } else {
            $buf = val;
        }
    };
}

/// This function serves as the entry to the aggregate extension.
///
/// # Arguments
///
/// * `db`: An argument whose type implements the `DB` trait which can be used
///         to interact with the database.
///
/// # Return
///
/// Returns 0 on success, 1 on error.
#[no_mangle]
pub fn init(db: Rc<dyn DB>) -> u64 {
    // Error code and response defined upfront so that results are written only
    // at the end of this function.
    let mut _err = INVALIDARG; // Prefix with _ to silence unused assignment warning
    let mut _order: u32 = 0; // Prefix with _ to silence unused assignment warning
    let mut _aggr: u64 = 0; // Prefix with _ to silence unused assignment warning
    let mut _obj = None; // Prefix with _ to silence unused assignment warning
    let mut _buf: Option<MultiReadBuf> = None; // Prefix with _ to silence unused assignment warning
    {
        let arg: &[u8] = db.args();
        let (t, val) = arg.split_at(size_of::<u64>());
        let (n, val) = val.split_at(size_of::<u32>());
        let (o, key) = val.split_at(size_of::<u32>());

        // Get the table id from the unwrapped arguments.
        let mut table: u64 = 0;
        for (idx, e) in t.iter().enumerate() {
            table |= (*e as u64) << (idx << 3);
        }

        // Get the number of keys to aggregate across.
        let mut num_k: u32 = 0;
        for (idx, e) in n.iter().enumerate() {
            num_k |= (*e as u32) << (idx << 3);
        }

        // Get the order.
        for (idx, e) in o.iter().enumerate() {
            _order |= (*e as u32) << (idx << 3);
        }

        // Retrieve the list of keys to aggregate across.
        GET1!(db, table, key, _obj);

        // Try performing the aggregate if the key list was successfully retrieved.
        if let Some(val) = _obj {
            let mut col = Vec::new();
            let value = val
                .read()
                .split_at((KEYLENGTH as usize) * (num_k as usize))
                .0;

            MULTIGET1!(db, table, KEYLENGTH, value, _buf);

            match _buf {
                Some(vals) => {
                    if vals.num() > 0 {
                        col.push(vals.read()[0]);
                    }

                    while vals.next() {
                        col.push(vals.read()[0]);
                    }
                }

                None => {
                    _err = INVALIDKEY;
                    db.resp(pack(&_err));
                    return 0;
                }
            }

            // Aggregate the saved column.
            _aggr = col.iter().fold(0, |a, e| a + (*e as u64));
        }
    }

    // Compute pow(aggr, order).
    for _mul in 1.._order {
        _aggr *= _aggr;
    }
    _err = SUCCESSFUL;
    // First write in the response code.
    db.resp(pack(&_err));
    // Second write the result.
    db.resp(pack(&_aggr));

    0
}
