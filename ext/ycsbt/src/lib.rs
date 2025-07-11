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

#![crate_type = "dylib"]
// Disable this because rustc complains about no_mangle being unsafe
//#![forbid(unsafe_code)]

extern crate sandstorm;

use core::arch::asm;
use std::rc::Rc;

use sandstorm::db::DB;

/// Return a 64-bit timestamp using the rdtsc instruction.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub fn rdtsc() -> u64 {
    unsafe {
        let mut _lo: u32;
        let mut _hi: u32;
        asm!(
            "rdtsc",
            out("eax") _lo,
            out("edx") _hi,
            options(nomem, nostack, preserves_flags)
        );
        ((_hi as u64) << 32) | _lo as u64
    }
}

/// This function implements the get() extension using the sandstorm interface.
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
    let key_len = 30;
    let mut _table: u64 = 0;
    let mut _optype = 0;
    let mut _order: u32 = 0;
    let mut _keys = Vec::with_capacity(2 * key_len);
    let mut _obj = None;
    let mut _multiobj = None;
    {
        // First off, retrieve the arguments to the extension.
        let args = db.args();

        // Check that the arguments received is long enough to contain an 1 bytes
        // operation type, 8 byte table id and a key to be looked up. If not, then
        // write an error message to the response and return to the database.
        if args.len() <= 38 {
            let error = "Invalid args";
            db.resp(error.as_bytes());
            return 1;
        }

        _optype = args[args.len() - 1];
        if _optype == 3 {
            db.debug_log("");
            return 0;
        }

        // Next, split the arguments into a view over the table identifier
        // (first eight bytes), and a view over the key to be looked up.
        // De-serialize the table identifier into a u64.
        let (s_table, rem) = args.split_at(8);
        let rem = rem.split_at(rem.len() - 1).0;
        let (key, ord) = rem.split_at(rem.len() - 4);
        _keys.extend_from_slice(key);

        // Get the table id from the unwrapped arguments.
        for (idx, e) in s_table.iter().enumerate() {
            _table |= (*e as u64) << (idx << 3);
        }

        for (idx, e) in ord.iter().enumerate() {
            _order |= (*e as u32) << (idx << 3);
        }
    }

    if _optype == 1 {
        // Read operation
        _obj = db.get(_table, &_keys);
        match _obj {
            Some(val) => {
                db.resp(val.read());
                return 0;
            }

            None => {
                let error = "Object does not exist";
                db.resp(error.as_bytes());
                return 1;
            }
        }
    } else {
        _multiobj = db.multiget(_table, key_len as u16, &_keys);

        // Compute part for this extension
        if _order > 0 {
            if _order >= 600 {
                let start = rdtsc();
                while rdtsc() - start < 600 as u64 {}
                _order -= 600;
            }

            loop {
                if _order <= 2000 {
                    let start = rdtsc();
                    while rdtsc() - start < _order as u64 {}
                    break;
                } else {
                    let start = rdtsc();
                    while rdtsc() - start < 2000 as u64 {}
                    _order -= 2000;
                }
            }
        }

        match _multiobj {
            Some(vals) => {
                if vals.num() == 2 {
                    let mut _value1 = Vec::with_capacity(100);
                    let mut _value2 = Vec::with_capacity(100);
                    let (key1, key2) = _keys.split_at(key_len as usize);
                    _value1.extend_from_slice(vals.read());
                    let _ = vals.next();
                    _value2.extend_from_slice(vals.read());
                    if _value1[0] > 0 && _value2[0] < 255 {
                        _value1[0] -= 1;
                        _value2[0] += 1;
                    } else if _value2[0] > 0 && _value1[0] < 255 {
                        _value2[0] -= 1;
                        _value1[0] += 1;
                    }

                    if let Some(mut buf1) = db.alloc(_table, key1, _value1.len() as u64) {
                        if let Some(mut buf2) = db.alloc(_table, key2, _value2.len() as u64) {
                            buf1.write_slice(&_value1);
                            buf2.write_slice(&_value2);
                            db.put(buf1);
                            db.put(buf2);
                            return 0;
                        }
                    }
                }

                let error = "Error";
                db.resp(error.as_bytes());
                return 1;
            }

            None => {
                let error = "Object does not exist";
                db.resp(error.as_bytes());
                return 1;
            }
        }
    }

    0
}
