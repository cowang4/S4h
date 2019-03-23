
use crate::key::{KEY_SIZE_BYTES, Key};


/// Hashes the input data to a KEY_SIZE_BYTES blake2b hash.
pub fn hash(data: &[u8]) -> Key {
    let mut state = blake2b_simd::Params::new().hash_length(KEY_SIZE_BYTES).to_state();
    state.update(data);
    let hash = state.finalize();
    Key::from(hash.as_bytes())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        let input = "the quick brown fox jumps over the lazy dog".as_bytes();
        let _out = hash(input);
        //assert_eq!(out, (&[91, 251, 115, 114, 189, 178, 241, 105, 237, 140, 128, 14, 94, 228, 65, 232]).into());
    }
}
