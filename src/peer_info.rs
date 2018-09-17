
use std::collections::LinkedList;
use std::net::SocketAddr;

use uuid::Uuid;

use key::{Key, key_dist, KEY_SIZE_BITS, KEY_SIZE_BYTES};


pub const K: usize = 20;


pub struct KBucket(LinkedList<(SocketAddr, Key)>);

impl KBucket {

    pub fn new() -> KBucket {
        KBucket {
            0: LinkedList::<(SocketAddr, Key)>::new(),
        }
    }

    pub fn push_back(&mut self, elem: (SocketAddr, Key)) {
        if self.0.len() < K {
            self.0.push_back(elem);
        }
    }

    /// Move the element at i to the back of the list
    pub fn move_to_back(&mut self, i: usize) {
        let len = self.0.len();
        if i >= len {
            return;
        }
        let mut tail = self.0.split_off(i);
        let elem = tail.pop_front().expect("tail not empty");
        tail.push_back(elem);
        self.0.append(&mut tail);
    }

    pub fn contains(&self, key: &Key) -> bool {
        for (_addr, k) in self.0.iter() {
            if k == key {
                return true;
            }
        }
        return false;
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn pop_front(&mut self) -> Option<(SocketAddr, Key)> {
        self.0.pop_front()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn full(&self) -> bool {
        self.0.len() >= K
    }

    
    /// Returns the 0-index of the key in the list,
    /// if it exists, else None
    pub fn index(&self, key: &Key) -> Option<usize> {
        for (i, (_addr, k)) in self.0.iter().enumerate() {
            if k == key {
                return Some(i);
            }
        }
        None
    }
    
    /// Removes the key from the list if it exists.
    /// Returns whether or not it was removed.
    pub fn remove(&mut self, key: &Key) -> bool {
        if let Some(i) = self.index(key) {
            if i == 0 {
                self.0.pop_front().expect("front exists");
                return true;
            }
            if i == self.0.len() - 1 {
                self.0.pop_back().expect("back exists");
                return true;
            }
            let mut tail = self.0.split_off(i);
            tail.pop_front().expect("tail not empty");
            self.0.append(&mut tail);
            return true;
        }
        false
    }
}


pub struct PeerInfo {
    pub buckets: Vec<KBucket>,
    pub id: Key,
}

impl PeerInfo {
    
    pub fn new() -> PeerInfo {
        let vec = Vec::<KBucket>::with_capacity(KEY_SIZE_BITS);
        let id = *(Uuid::new_v4().as_bytes());
        PeerInfo {
            buckets: vec,
            id: id,
        }
    }

    pub fn with_id(k: &Key) -> PeerInfo {
        let vec = Vec::<KBucket>::with_capacity(KEY_SIZE_BITS);
        PeerInfo {
            buckets: vec,
            id: k.clone(),
        }
    }

    /// Returns the bucket number of the key.
    /// Bucket is determined by distance from self.
    /// 2^1 <= dist(self, k) < 2^i+1
    pub fn bucket_of(&self, k: &Key) -> usize {
        let dist = key_dist(&self.id, k);
        for (group, b) in dist.iter().enumerate() {
            let mut b: u8 = *b;
            if b > 0 {
                let mut i: usize = 0;
                while b > 0 {
                    i += 1;
                    b >>= 1;
                }
                return (i + (KEY_SIZE_BYTES - group - 1) * 8) - 1;
            }
        }
        return 0;
    }

    pub fn contains(&self, k: &Key) -> bool {
        let i = self.bucket_of(k);
        self.buckets[i].contains(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kbucket_move_to_back() {
        let mut l1 = KBucket::new();
        let k1 = [255; 16];
        l1.push_back(("127.0.0.1:8080".parse().unwrap(), k1));
        l1.push_back(("127.0.0.2:8080".parse().unwrap(), k1));
        l1.push_back(("127.0.0.3:8080".parse().unwrap(), k1));
        l1.push_back(("127.0.0.4:8080".parse().unwrap(), k1));
        l1.move_to_back(1); // move 127.0.0.2 to back
        assert_eq!(("127.0.0.1:8080".parse().unwrap(), k1), l1.pop_front().unwrap());
        assert_eq!(("127.0.0.3:8080".parse().unwrap(), k1), l1.pop_front().unwrap());
        assert_eq!(("127.0.0.4:8080".parse().unwrap(), k1), l1.pop_front().unwrap());
        assert_eq!(("127.0.0.2:8080".parse().unwrap(), k1), l1.pop_front().unwrap());
        assert_eq!(l1.is_empty(), true);

        // test empty
        l1.move_to_back(0);
        assert_eq!(l1.is_empty(), true);

        // test one elem
        l1.push_back(("127.0.0.1:8080".parse().unwrap(), k1));
        l1.move_to_back(0);
        assert_eq!(("127.0.0.1:8080".parse().unwrap(), k1), l1.pop_front().unwrap());
        assert_eq!(l1.is_empty(), true);
    }

    #[test]
    fn test_bucket_of() {
        let k1 = [0; 16];
        let k2 = [255; 16];
        let k3 = [1; 16]; 
        let k4 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0];
        let k5 = [128; 16]; 
        let p1 = PeerInfo::with_id(&k1);
        assert_eq!(0, p1.bucket_of(&k1));
        assert_eq!(120, p1.bucket_of(&k3));
        assert_eq!(127, p1.bucket_of(&k2));
        assert_eq!(8, p1.bucket_of(&k4));
        let p2 = PeerInfo::with_id(&k2);
        assert_eq!(0, p2.bucket_of(&k2));
        assert_eq!(127, p2.bucket_of(&k3));
        assert_eq!(127, p2.bucket_of(&k4));
        assert_eq!(126, p2.bucket_of(&k5));
    }

    #[test]
    fn test_remove() {
        let mut bucket1 = KBucket::new();
        let k1 = [255; 16];
        let k2 = [254; 16];
        let k3 = [253; 16];
        let k4 = [252; 16];
        let k5 = [251; 16];
        bucket1.push_back(("127.0.0.1:8080".parse().unwrap(), k1));
        bucket1.push_back(("127.0.0.2:8080".parse().unwrap(), k2));
        bucket1.push_back(("127.0.0.3:8080".parse().unwrap(), k3));
        bucket1.push_back(("127.0.0.4:8080".parse().unwrap(), k4));
        // remove front
        assert_eq!(bucket1.remove(&k1), true);
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.2:8080".parse().unwrap(), k2));
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.3:8080".parse().unwrap(), k3));
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.4:8080".parse().unwrap(), k4));
        assert_eq!(bucket1.is_empty(), true);
        // remove back
        bucket1.push_back(("127.0.0.1:8080".parse().unwrap(), k1));
        bucket1.push_back(("127.0.0.2:8080".parse().unwrap(), k2));
        bucket1.push_back(("127.0.0.3:8080".parse().unwrap(), k3));
        bucket1.push_back(("127.0.0.4:8080".parse().unwrap(), k4));
        assert_eq!(bucket1.remove(&k4), true);
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.1:8080".parse().unwrap(), k1));
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.2:8080".parse().unwrap(), k2));
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.3:8080".parse().unwrap(), k3));
        assert_eq!(bucket1.is_empty(), true);
        // remove middle
        bucket1.push_back(("127.0.0.1:8080".parse().unwrap(), k1));
        bucket1.push_back(("127.0.0.2:8080".parse().unwrap(), k2));
        bucket1.push_back(("127.0.0.3:8080".parse().unwrap(), k3));
        bucket1.push_back(("127.0.0.4:8080".parse().unwrap(), k4));
        assert_eq!(bucket1.remove(&k3), true);
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.1:8080".parse().unwrap(), k1));
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.2:8080".parse().unwrap(), k2));
        assert_eq!(bucket1.pop_front().unwrap(), ("127.0.0.4:8080".parse().unwrap(), k4));
        assert_eq!(bucket1.is_empty(), true);
        //remove empty
        assert_eq!(bucket1.remove(&k5), false);
        // remove one
        bucket1.push_back(("127.0.0.1:8080".parse().unwrap(), k1));
        assert_eq!(bucket1.remove(&k1), true);
        assert_eq!(bucket1.is_empty(), true);
    }

}
