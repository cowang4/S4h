
use std::cmp::{PartialEq, Ordering, Ord, PartialOrd};
use std::fmt;
use std::slice::Iter;

use bytes::{Bytes, BytesMut, BufMut};
use hex;
use serde_derive::{Deserialize, Serialize};


pub const KEY_SIZE_BITS: usize = 128;
pub const KEY_SIZE_BYTES: usize = KEY_SIZE_BITS / 8;


// A big endian stored key
#[derive(Clone, Eq, Hash, Deserialize, Serialize)]
pub struct Key(Bytes);


impl Key {

    pub fn from_str(string: &str) -> Result<Key, hex::FromHexError> {
        let bytes = hex::decode(string)?;
        Ok(Key { 0: bytes.into() })
    }


    pub fn iter(&self) -> Iter<u8> {
        self.0.iter()
    }


    pub fn dist(&self, other: &Key) -> Key {
        let mut dist = BytesMut::with_capacity(KEY_SIZE_BYTES);
        for (b1, b2) in self.iter().zip(other.iter()) {
            dist.put(b1 ^ b2);
        }
        dist.freeze().into()
    }

    pub fn inverse(&self) -> Key {
        let mut inv = BytesMut::with_capacity(KEY_SIZE_BYTES);
        for b in self.iter() {
            inv.put(!b);
        }
        inv.freeze().into()
    }
}


impl std::convert::AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}


impl From<&[u8]> for Key {
    fn from(bytes: &[u8]) -> Key {
        Key { 0: Bytes::from(bytes) }
    }
}


impl From<Bytes> for Key {
    fn from(bytes: Bytes) -> Key {
        Key { 0: bytes }
    }
}


impl From<&'static [u8; KEY_SIZE_BYTES]> for Key {
    fn from(array: &'static [u8; KEY_SIZE_BYTES]) -> Key {
        Key { 0: Bytes::from_static(array) }
    }
}


impl Ord for Key {
    fn cmp(&self, other: &Key) -> Ordering {
        for (b1, b2) in self.0.iter().zip(other.iter()) {
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
}


impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Key) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


impl PartialEq for Key {
    fn eq(&self, other: &Key) -> bool {
        self.0 == other.0
    }
}


impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}


impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_cmp() {
        let x1 = Key::from(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let x2 = Key::from(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let x3 = Key::from(&[2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(x1.cmp(&x2), Ordering::Greater);
        assert_eq!(x2.cmp(&x1), Ordering::Less);
        assert_eq!(x1.cmp(&x1), Ordering::Equal);
        assert_eq!(x2.cmp(&x2), Ordering::Equal);
        assert_eq!(x3.cmp(&x3), Ordering::Equal);
        assert_eq!(x1.cmp(&x3), Ordering::Less);
        assert_eq!(x2.cmp(&x3), Ordering::Less);
    }

    #[test]
    fn test_key_dist() {
        let x1 = Key::from(&[255; 16]);
        let x2 = Key::from(&[0; 16]);
        let x3 = Key::from(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(x1.dist(&x2), Key::from(&[255; 16]));
        assert_eq!(x1.dist(&x1), Key::from(&[0; 16]));
        assert_eq!(x2.dist(&x2), Key::from(&[0; 16]));
        assert_eq!(x2.dist(&x3), Key::from(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]));
        assert_eq!(x1.dist(&x3), Key::from(&[254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]));
        assert_eq!(x3.dist(&x1), Key::from(&[254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]));
    }
}
