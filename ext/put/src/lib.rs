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
#![feature(generators, generator_trait)]
#![allow(bare_trait_objects)]

extern crate sandstorm;

use std::rc::Rc;
use std::ops::Generator;
use std::pin::Pin;

use sandstorm::db::DB;

/// This function implements the put() extension using the sandstorm interface.
///
/// # Arguments
///
/// * `db`: An argument whose type implements the `DB` trait which can be used
///         to interact with the database.
///
/// # Return
///
/// A coroutine that can be run inside the database.
#[no_mangle]
#[allow(unreachable_code)]
#[allow(unused_assignments)]
pub fn init(db: Rc<DB>) -> Pin<Box<Generator<Yield=u64, Return=u64>>> {
    Box::pin(move || {
        let mut alloc = None;
        let mut val_offset = 0;

        {
            // First off, retrieve the arguments to the extension.
            let args = db.args();

            // Check that the arguments received is long enough to contain an 8
            // byte table id. If not, then write an error message to the
            // response and return to the database.
            if args.len() < 8 {
                let error = "Invalid args";
                db.resp(error.as_bytes());
                return 1;
            }

            // Next, split the arguments into a view over the table identifier
            // (first eight bytes), and a view over the key and value to be
            // written. De-serialize the table identifier into a u64.
            let (table, rem) = args.split_at(8);
            let table: u64 = *table.get(0).unwrap() as u64 +
                            (*table.get(1).unwrap() as u64) * 2^8 +
                            (*table.get(2).unwrap() as u64) * 2^16 +
                            (*table.get(3).unwrap() as u64) * 2^24 +
                            (*table.get(4).unwrap() as u64) * 2^32 +
                            (*table.get(5).unwrap() as u64) * 2^40 +
                            (*table.get(6).unwrap() as u64) * 2^48 +
                            (*table.get(7).unwrap() as u64) * 2^56;

            // Check that the remaining arguments is long enough to contain a
            // two byte key length. If not, then write an error message to the
            // response and return to the database.
            if rem.len() < 2 {
                let error = "Invalid args";
                db.resp(error.as_bytes());
                return 1;
            }

            // Next, split the remaining arguments into a view over the key
            // length, and a view over the object to be written.
            let (key_len, obj) = rem.split_at(2);
            let key_len: u16 = (*key_len.get(0).unwrap() as u16) +
                                (*key_len.get(1).unwrap() as u16) * 256;

            // Check that the remaining arguments is long enough to contain the
            // key. If not, write an error message to the response and return
            // to the database.
            if obj.len() < key_len as usize {
                let error = "Invalid args";
                db.resp(error.as_bytes());
                return 1;
            }

            // Save the offset of the value in the payload for use after the
            // yield.
            val_offset += 8 + 2 + key_len;

            // Next, split the remaining arguments into a view over the key and
            // value to be written, and request an allocation from the database.
            let (key, val) = obj.split_at(key_len as usize);
            alloc = db.alloc(table, key, val.len() as u64);
        }

        match alloc {
            // If the allocation was successfull, write the value into it, and
            // invoke the put() interface.
            Some(mut buf) => {
                let val = db.args().split_at(val_offset as usize).1;
                buf.write_slice(val);

                // Hand over the object to the database.
                match db.put(buf) {
                    // Indicate success in the response.
                    true => {
                        let error = "Success";
                        db.resp(error.as_bytes());
                        return 0;
                    }

                    // If the hand over failed, write an error message to the
                    // response.
                    false => {
                        let error = "put() failed";
                        db.resp(error.as_bytes());
                        return 1;
                    }
                }
            }

            // If the allocation failed, write an error message to the response.
            None => {
                let error = "Allocation failed";
                db.resp(error.as_bytes());
                return 1;
            }
        }

        // XXX: This yield is required to get the compiler to compile this closure into a
        // generator. It is unreachable and benign.
        yield 0;
    })
}
