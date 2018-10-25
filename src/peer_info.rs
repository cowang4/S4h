
use std::collections::LinkedList;
use std::net::SocketAddr;
use std::sync::{RwLock, RwLockReadGuard};
use std::ops::{Deref, DerefMut};

use bytes::Bytes;
use uuid::Uuid;
use serde;

use crate::key::{Key, key_dist, KEY_SIZE_BITS, KEY_SIZE_BYTES};
use crate::rpc::{Client};


pub const K: usize = 20;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Peer {
    pub id: Key,
    pub addr: SocketAddr,
    #[serde(skip)]
    pub client: Option<Box<Client>>,
}

impl PartialEq for Peer {
    fn eq(&self, other: &Peer) -> bool {
        self.id == other.id && self.addr == other.addr
    }
}

impl Peer {

    pub fn new() -> Peer {
        Peer {
            id: Key::new(),
            addr: "127.0.0.1:0".parse().unwrap(),
            client: None,
        }
    }

    pub fn with_id(k: Key) -> Peer {
        Peer {
            id: k,
            addr: "127.0.0.1:0".parse().unwrap(),
            client: None,
        }
    }
}

impl From<(SocketAddr, Key)> for Peer {
    fn from(tup: (SocketAddr, Key)) -> Peer {
        Peer {
            id: tup.1,
            addr: tup.0,
            client: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KBucket(LinkedList<Peer>);

impl KBucket {

    pub fn new() -> KBucket {
        KBucket {
            0: LinkedList::<Peer>::new()
        }
    }

    pub fn push_back(&mut self, elem: Peer) {
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

    /// O(n) lookup
    pub fn contains(&self, key: &Key) -> bool {
        for peer in self.0.iter() {
            if peer.id == key {
                return true;
            }
        }
        return false;
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn pop_front(&mut self) -> Option<Peer> {
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
        for (i, peer) in self.0.iter().enumerate() {
            if peer.id == key {
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

    /// O(n) lookup
    pub fn get<'a>(&'a self, key: &Key) -> Option<&'a Peer> {
        for peer in self.0.iter() {
            if peer.id == key {
                return Some(peer);
            }
        }
        None
    }
}


#[derive(Debug)]
pub struct PeerInfo {
    pub buckets: Vec<RwLock<KBucket>>,
    pub id: Key,
}

impl PeerInfo {
    
    pub fn new() -> PeerInfo {
        let mut vec = Vec::<RwLock<KBucket>>::with_capacity(KEY_SIZE_BITS);
        for _ in 0..KEY_SIZE_BITS {
            vec.push(RwLock::new(KBucket::new()));
        }
        let id = Bytes::from(&Uuid::new_v4().as_bytes()[..]);
        PeerInfo {
            buckets: vec,
            id: id,
        }
    }

    pub fn with_id(k: &Key) -> PeerInfo {
        let mut vec = Vec::<RwLock<KBucket>>::with_capacity(KEY_SIZE_BITS);
        for _ in 0..KEY_SIZE_BITS {
            vec.push(RwLock::new(KBucket::new()));
        }
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

    pub fn get<'a>(&'a self, k: &Key) -> RwLockReadGuard<'a, KBucket> {
        let i: usize = self.bucket_of(k);
        let bucket: std::sync::RwLockReadGuard<'a, KBucket> = self.buckets[i].read().expect("obtain kbucket read lock");
        bucket
    }

    pub fn contains(&self, k: &Key) -> bool {
        let i = self.bucket_of(k);
        self.buckets[i].read().expect("obtain kbucket read lock").deref().contains(k)
    }

    /// Updates a node_id by moving it to the end of the list
    pub fn update(&self, k: &Key) {
        let bucket_num = self.bucket_of(k);
        let mut bucket_read = self.buckets[bucket_num].write()
                                                 .expect("obtain kbucket write lock");
        let bucket = bucket_read.deref_mut();
        let index = bucket.index(k);
        if let Some(index) = index {
            bucket.move_to_back(index);
        }
    }

    pub fn insert(&self, k: &Key, addr: SocketAddr, client: Box<Client>) {
        let bucket_num = self.bucket_of(k);
        let mut bucket_read = self.buckets[bucket_num].write()
                                                 .expect("obtain kbucket write lock");
        let bucket = bucket_read.deref_mut();
        let index = bucket.index(k);
        match index {
            Some(index) => bucket.move_to_back(index),
            None        => {
                let mut peer = Peer::with_id(k.clone());
                peer.addr = addr;
                peer.client = Some(client);
                bucket.push_back(peer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kbucket_move_to_back() {
        let mut l1 = KBucket::new();
        let k1 = Bytes::from_static(&[255; 16]);
        l1.push_back(("127.0.0.1:8080".parse().unwrap(), k1.clone()).into());
        l1.push_back(("127.0.0.2:8080".parse().unwrap(), k1.clone()).into());
        l1.push_back(("127.0.0.3:8080".parse().unwrap(), k1.clone()).into());
        l1.push_back(("127.0.0.4:8080".parse().unwrap(), k1.clone()).into());
        l1.move_to_back(1); // move 127.0.0.2 to back
        assert_eq!(Peer::from(("127.0.0.1:8080".parse::<SocketAddr>().unwrap(), k1.clone())), l1.pop_front().unwrap());
        assert_eq!(Peer::from(("127.0.0.3:8080".parse::<SocketAddr>().unwrap(), k1.clone())), l1.pop_front().unwrap());
        assert_eq!(Peer::from(("127.0.0.4:8080".parse::<SocketAddr>().unwrap(), k1.clone())), l1.pop_front().unwrap());
        assert_eq!(Peer::from(("127.0.0.2:8080".parse::<SocketAddr>().unwrap(), k1.clone())), l1.pop_front().unwrap());
        assert_eq!(l1.is_empty(), true);

        // test empty
        l1.move_to_back(0);
        assert_eq!(l1.is_empty(), true);

        // test one elem
        l1.push_back(("127.0.0.1:8080".parse().unwrap(), k1.clone()).into());
        l1.move_to_back(0);
        assert_eq!(Peer::from(("127.0.0.1:8080".parse::<SocketAddr>().unwrap(), k1.clone())), l1.pop_front().unwrap());
        assert_eq!(l1.is_empty(), true);
    }

    #[test]
    fn test_bucket_of() {
        let k1 = Bytes::from_static(&[0; 16]);
        let k2 = Bytes::from_static(&[255; 16]);
        let k3 = Bytes::from_static(&[1; 16]); 
        let k4 = Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);
        let k5 = Bytes::from_static(&[128; 16]); 
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
        let k1 = Bytes::from_static(&[255; 16]);
        let k2 = Bytes::from_static(&[254; 16]);
        let k3 = Bytes::from_static(&[253; 16]);
        let k4 = Bytes::from_static(&[252; 16]);
        let k5 = Bytes::from_static(&[251; 16]);
        bucket1.push_back(("127.0.0.1:8080".parse().unwrap(), k1.clone()).into());
        bucket1.push_back(("127.0.0.2:8080".parse().unwrap(), k2.clone()).into());
        bucket1.push_back(("127.0.0.3:8080".parse().unwrap(), k3.clone()).into());
        bucket1.push_back(("127.0.0.4:8080".parse().unwrap(), k4.clone()).into());
        // remove front
        assert_eq!(bucket1.remove(&k1), true);
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.2:8080".parse().unwrap(), k2.clone())));
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.3:8080".parse().unwrap(), k3.clone())));
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.4:8080".parse().unwrap(), k4.clone())));
        assert_eq!(bucket1.is_empty(), true);
        // remove back
        bucket1.push_back(("127.0.0.1:8080".parse().unwrap(), k1.clone()).into());
        bucket1.push_back(("127.0.0.2:8080".parse().unwrap(), k2.clone()).into());
        bucket1.push_back(("127.0.0.3:8080".parse().unwrap(), k3.clone()).into());
        bucket1.push_back(("127.0.0.4:8080".parse().unwrap(), k4.clone()).into());
        assert_eq!(bucket1.remove(&k4), true);
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.1:8080".parse().unwrap(), k1.clone())));
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.2:8080".parse().unwrap(), k2.clone())));
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.3:8080".parse().unwrap(), k3.clone())));
        assert_eq!(bucket1.is_empty(), true);
        // remove middle
        bucket1.push_back(("127.0.0.1:8080".parse().unwrap(), k1.clone()).into());
        bucket1.push_back(("127.0.0.2:8080".parse().unwrap(), k2.clone()).into());
        bucket1.push_back(("127.0.0.3:8080".parse().unwrap(), k3.clone()).into());
        bucket1.push_back(("127.0.0.4:8080".parse().unwrap(), k4.clone()).into());
        assert_eq!(bucket1.remove(&k3), true);
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.1:8080".parse().unwrap(), k1.clone())));
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.2:8080".parse().unwrap(), k2.clone())));
        assert_eq!(bucket1.pop_front().unwrap(), Peer::from(("127.0.0.4:8080".parse().unwrap(), k4.clone())));
        assert_eq!(bucket1.is_empty(), true);
        //remove empty
        assert_eq!(bucket1.remove(&k5), false);
        // remove one
        bucket1.push_back(("127.0.0.1:8080".parse().unwrap(), k1.clone()).into());
        assert_eq!(bucket1.remove(&k1), true);
        assert_eq!(bucket1.is_empty(), true);
    }

    #[test]
    fn test_PeerInfo_length() {
        let pi = PeerInfo::new();
        println!("PeerInfo.buckets {:?}", pi.buckets);
        assert_eq!(pi.buckets.len(), KEY_SIZE_BITS);
    }

}
