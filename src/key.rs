
use std::cmp::Ordering;

use bytes::{Bytes, BytesMut, BufMut};
use hex;


pub const KEY_SIZE_BITS: usize = 128;
pub const KEY_SIZE_BYTES: usize = KEY_SIZE_BITS / 8;

// A big endian stored key
pub type Key = Bytes;


/// Compares two keys, returns an std::cmp::Ordering
pub fn key_cmp(k1: &Key, k2: &Key) -> Ordering {
    for (b1, b2) in k1.iter().zip(k2.iter()) {
        if b1 > b2 {
            return Ordering::Greater;
        }
        else if b1 < b2 {
            return Ordering::Less;
        }
        // else continue
    }
    Ordering::Equal
}

pub fn key_dist(k1: &Key, k2: &Key) -> Key {
    let mut dist = BytesMut::with_capacity(KEY_SIZE_BYTES);
    for (b1, b2) in k1.iter().zip(k2.iter()) {
        dist.put(b1 ^ b2);
    }
    dist.freeze()
}

pub fn key_inverse(k: &Key) -> Key {
    let mut inv = BytesMut::with_capacity(KEY_SIZE_BYTES);
    for b in k.iter() {
        inv.put(!b);
    }
    inv.freeze()
}

pub fn key_fmt(k: &Key) -> String {
    format!("{}", hex::encode(k))
}

pub fn option_key_fmt(k: &Option<Key>) -> String {
    match k {
        Some(k) => format!("Some({})", hex::encode(k)),
        None    => "None".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_key_cmp() {
        let x1 = Bytes::from_static(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let x2 = Bytes::from_static(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let x3 = Bytes::from_static(&[2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(key_cmp(&x1, &x2), Ordering::Greater);
        assert_eq!(key_cmp(&x2, &x1), Ordering::Less);
        assert_eq!(key_cmp(&x1, &x1), Ordering::Equal);
        assert_eq!(key_cmp(&x2, &x2), Ordering::Equal);
        assert_eq!(key_cmp(&x3, &x3), Ordering::Equal);
        assert_eq!(key_cmp(&x1, &x3), Ordering::Less);
        assert_eq!(key_cmp(&x2, &x3), Ordering::Less);
    }

    #[test]
    fn test_key_dist() {
        let x1 = Bytes::from_static(&[255; 16]);
        let x2 = Bytes::from_static(&[0; 16]);
        let x3 = Bytes::from_static(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(key_dist(&x1, &x2), Bytes::from_static(&[255; 16]));
        assert_eq!(key_dist(&x1, &x1), Bytes::from_static(&[0; 16]));
        assert_eq!(key_dist(&x2, &x2), Bytes::from_static(&[0; 16]));
        assert_eq!(key_dist(&x2, &x3), Bytes::from_static(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]));
        assert_eq!(key_dist(&x1, &x3), Bytes::from_static(&[254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]));
        assert_eq!(key_dist(&x3, &x1), Bytes::from_static(&[254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]));
    }
}
