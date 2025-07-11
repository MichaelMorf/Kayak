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
#![allow(bare_trait_objects)]

extern crate sandstorm;

use std::rc::Rc;
use sandstorm::db::DB;

/// This function implements an uncooperative extension using the sandstorm interface.
///
/// # Arguments
///
/// * `db`: An argument whose type implements the `DB` trait which can be used
///         to interact with the database.
///
/// # Return
///
/// A stub function for compatibility with Rust 2021+ (no coroutine).
#[no_mangle]
#[allow(unreachable_code)]
#[allow(unused_assignments)]
pub fn init(_db: Rc<dyn DB>) -> u64 {
    // This extension does nothing and immediately returns 0.
    0
}
