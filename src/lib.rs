
extern crate blake2b_simd;
extern crate uuid;

mod key;
mod peer_info;

use blake2b_simd::{Hash};

use key::{KEY_SIZE_BYTES};

const ALPHA: usize = 3; // concurrency parameter

/// Hashes the input data to a KEY_SIZE_BYTES blake2b hash.
pub fn hash(data: &[u8]) -> Hash {
    let mut state = blake2b_simd::Params::new().hash_length(KEY_SIZE_BYTES).to_state();
    state.update(data);
    let hash = state.finalize();
    hash
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        let input = "the quick brown fox jumps over the lazy dog".as_bytes();
        let out = hash(input);
        let out_bytes = out.as_bytes();
        assert_eq!(out_bytes, [91, 251, 115, 114, 189, 178, 241, 105, 237, 140, 128, 14, 94, 228, 65, 232]);
    }
}
