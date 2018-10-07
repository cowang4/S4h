#![feature(plugin, never_type)]
#![plugin(tarpc_plugins)]

extern crate blake2b_simd;
extern crate bytes;
extern crate chashmap;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tarpc;
extern crate uuid;

pub mod key;
pub mod peer_info;
pub mod rpc;

use bytes::Bytes;

use key::{KEY_SIZE_BYTES, Key};

pub const ALPHA: usize = 3; // concurrency parameter

/// Hashes the input data to a KEY_SIZE_BYTES blake2b hash.
pub fn hash(data: &[u8]) -> Key {
    let mut state = blake2b_simd::Params::new().hash_length(KEY_SIZE_BYTES).to_state();
    state.update(data);
    let hash = state.finalize();
    Bytes::from(hash.as_bytes())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        let input = "the quick brown fox jumps over the lazy dog".as_bytes();
        let out = hash(input);
        assert_eq!(out, &[91, 251, 115, 114, 189, 178, 241, 105, 237, 140, 128, 14, 94, 228, 65, 232][..]);
    }
}
