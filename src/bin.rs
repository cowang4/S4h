
extern crate s4h_lib;

use s4h_lib::*;

pub fn main() {
    let hello = "Hello, World!".as_bytes();
    let _hello_hash = hash(hello);
}
