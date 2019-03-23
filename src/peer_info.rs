
use std::cmp::Ordering;
use std::collections::{LinkedList, linked_list};
use std::fmt::{self, Debug, Display, Formatter};
use std::net::SocketAddr;
use std::sync::{RwLock};
use std::ops::{Deref, DerefMut};

use serde_derive::{Deserialize, Serialize};

use crate::client;
use crate::key::{Key, KEY_SIZE_BITS, KEY_SIZE_BYTES};
use crate::state::{validate_resp};


pub const K: usize = 6;    /// KBucket size parameter
pub const ALPHA: usize = 3; /// concurrency parameter


#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Peer {
    pub id: Key,
    pub addr: SocketAddr,
}

impl Peer {

    pub fn with_id(k: Key) -> Peer {
        Peer {
            id: k,
            addr: "127.0.0.1:0".parse().expect("parse default addr"),
        }
    }
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Peer: id: {}  addr: {:?}", &self.id, self.addr)
    }
}

impl Debug for Peer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Peer: id: {}  addr: {:?}", &self.id, self.addr)
    }
}

impl From<(SocketAddr, Key)> for Peer {
    fn from(tup: (SocketAddr, Key)) -> Peer {
        Peer {
            id: tup.1,
            addr: tup.0,
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

    pub fn full(&self) -> bool {
        self.0.len() >= K
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
            if &peer.id == key {
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

    
    /// Returns the 0-index of the key in the list,
    /// if it exists, else None
    pub fn index(&self, key: &Key) -> Option<usize> {
        for (i, peer) in self.0.iter().enumerate() {
            if &peer.id == key {
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
            if i == self.len() - 1 {
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

    /// O(n) lookup of Peer by key
    pub fn get(&self, key: &Key) -> Option<Peer> {
        for peer in self.0.iter() {
            if &peer.id == key {
                return Some(peer.clone());
            }
        }
        None
    }

    pub fn get_peer_by_addr(&self, addr: &SocketAddr) -> Option<Peer> {
        for peer in self.iter() {
            if peer.addr == *addr {
                return Some(peer.clone());
            }
        }
        None
    }

    pub fn iter(&self) -> linked_list::Iter<Peer> {
        self.0.iter()
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl Display for KBucket {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut res = String::from("KBucket: [");
        for p in self.0.iter() {
            res.push_str(&format!("{}, ", p));
        }
        res.push_str("]\n");
        write!(f, "{}", res)
    }
}

#[derive(Debug)]
pub struct PeerInfo {
    pub buckets: Vec<RwLock<KBucket>>,
    pub id: Key,
}


impl PeerInfo {
    

    #[allow(dead_code)]
    pub fn new(k: Key) -> PeerInfo {
        let mut vec = Vec::<RwLock<KBucket>>::with_capacity(KEY_SIZE_BITS);
        for _ in 0..KEY_SIZE_BITS {
            vec.push(RwLock::new(KBucket::new()));
        }
        let id = k;
        PeerInfo {
            buckets: vec,
            id: id,
        }
    }

    /// Returns the bucket number of the key.
    /// Bucket is determined by distance from self.
    /// 2^1 <= dist(self, k) < 2^i+1
    pub fn bucket_of(&self, k: &Key) -> usize {
        let dist = self.id.dist(k);
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

    pub fn bucket_of_addr(&self, addr: &SocketAddr) -> usize {
        for (i, bucket_locked) in self.buckets.iter().enumerate() {
            let bucket_read = bucket_locked.read().expect("obtain kbucket read lock");
            let bucket = bucket_read.deref();
            for peer in bucket.iter() {
                if *addr == peer.addr {
                    return i;
                }
            }
        }
        return 0;
    }

    pub fn get(&self, k: &Key) -> Option<Peer> {
        let i: usize = self.bucket_of(k);
        let bucket: std::sync::RwLockReadGuard<KBucket> = self.buckets[i].read().expect("obtain kbucket read lock");
        bucket.get(k)
    }

    pub fn get_peer_by_addr(&self, addr: &SocketAddr) -> Option<Peer> {
        let i: usize = self.bucket_of_addr(addr);
        let bucket = self.buckets[i].read().expect("obtain kbucket read lock");
        bucket.get_peer_by_addr(addr)
    }

    #[allow(dead_code)]
    pub fn contains(&self, k: &Key) -> bool {
        let i = self.bucket_of(k);
        self.buckets[i].read().expect("obtain kbucket read lock").deref().contains(k)
    }

    pub fn contains_addr(&self, addr: &SocketAddr) -> bool {
        for bucket_locked in self.buckets.iter() {
            let bucket_read = bucket_locked.read().expect("obtain kbucket read lock");
            let bucket = bucket_read.deref();
            for peer in bucket.iter() {
                if *addr == peer.addr {
                    return true;
                }
            }
        }
        false
    }

    /// Updates a node_id by moving it to the end of the list
    pub fn update(&self, k: &Key) {
        let bucket_num = self.bucket_of(k);
        let mut bucket_write = self.buckets[bucket_num].write()
                                                 .expect("obtain kbucket write lock");
        let bucket = bucket_write.deref_mut();
        let index = bucket.index(k);
        if let Some(index) = index {
            bucket.move_to_back(index);
        }
    }

    /// Insert the peer if there's room in the right bucket.
    /// Otherwise, ping the most stale peer in that bucket,
    /// if that peer doesn't respond, drop it and the new one gets in.
    /// else, it responded, so the new one gets dropped.
    pub fn insert<'a>(&'a self, my_peer: Peer, k: Key, addr: SocketAddr) {
        
        let mut oldest = None;

        let mut peer = Peer::with_id(k.clone());
        peer.addr = addr;
        
        // The use of the RwLockWriteGuard needs to be in a lower scope than the function
        // because it's !Send. This is a limitation of the async generator.
        {
            let bucket_num = self.bucket_of(&k);
            let mut bucket_write = self.buckets[bucket_num].write()
                                                     .expect("obtain kbucket write lock");
            let bucket = bucket_write.deref_mut();
            let index = bucket.index(&k);
            match index {
                Some(index) => bucket.move_to_back(index),
                None        => {
                    if bucket.full() {
                        oldest = Some(bucket.pop_front().expect("full bucket has a value"));
                    }
                    else {
                        bucket.push_back(peer.clone());
                    }
                }
            }
        }

        if let Some(oldest) = oldest {
            // ping oldest peer to see if it's still alive
            let ping_resp = client::ping(oldest.addr, my_peer);

            // Re-obtain a mut ref to the bucket, because of limitation of async generator
            // See https://users.rust-lang.org/t/mutexguard-cannot-be-sent-inside-future-generator/21584
            let bucket_num = self.bucket_of(&k);
            let mut bucket_write = self.buckets[bucket_num].write()
                                     .expect("obtain kbucket write lock");
            let bucket = bucket_write.deref_mut();
            
            match ping_resp {
                Ok(resp) => {
                    if validate_resp(&resp) {
                        bucket.push_back(oldest);
                    }
                },
                Err(_) => bucket.push_back(peer),
            }
        }

    }

    /// Returns the K closer to key known peers.
    /// Filters closest_k_peers by being closer than self to key.
    pub fn closer_k_peers(&self, key: Key) -> Option<Vec<Peer>> {
        let dist_from_self = self.id.dist(&key);
        let all_peers = self.closest_k_peers(key.clone());
        if let Some(all_peers) = all_peers {
            let filtered_peers: Vec<Peer> = all_peers.into_iter().filter(|k| k.id.dist(&key).cmp(&dist_from_self) == Ordering::Less).collect();
            if filtered_peers.len() == 0 {
                None
            }
            else {
                Some(filtered_peers)
            }
        }
        else {
            None
        }
    }

    /// Returns the x closest known peers to key.
    fn closest_x_peers(&self, key: Key, x: usize) -> Option<Vec<Peer>> {
       let mut all_peers = Vec::new();

        for bucket_locked in self.buckets.iter() {
            let bucket_read = bucket_locked.read().expect("obtain kbucket read lock");
            let bucket = bucket_read.deref();
            for peer in bucket.iter() {
                all_peers.push(peer.clone());
            }
        }

        all_peers.sort_unstable_by(|a, b| key.dist(&a.id).cmp(&key.dist(&b.id)));
        all_peers.truncate(x);

        if all_peers.len() == 0 {
            None
        }
        else {
            Some(all_peers)
        }
    }

    /// Returns the K closest known peers to key.
    pub fn closest_k_peers(&self, key: Key) -> Option<Vec<Peer>> {
        self.closest_x_peers(key, K)
    }

    /// Returns the ALPHA closest known peers to key.
    pub fn closest_alpha_peers(&self, key: Key) -> Option<Vec<Peer>> {
        self.closest_x_peers(key, ALPHA)
    }

    pub fn remove(&self, key: &Key) -> bool {
        let i = self.bucket_of(key);
        let mut bucket_write = self.buckets[i].write().expect("obtain kbucket write lock");
        let bucket = bucket_write.deref_mut();
        if bucket.contains(key) {
            return bucket.remove(key);
        }
        return false;
    }

    #[allow(dead_code)]
    pub fn all_peers(&self) -> Vec<Peer> {
        let mut all_peers = Vec::new();

        for bucket_locked in self.buckets.iter() {
            let bucket_read = bucket_locked.read().expect("obtain kbucket read lock");
            let bucket = bucket_read.deref();
            for peer in bucket.iter() {
                all_peers.push(peer.clone());
            }
        }
        all_peers
    }

    // clears the kbuckets, so that clients are dropped and the tests can finish
    #[allow(dead_code)]
    pub fn clear(&self) {
        for bucket in self.buckets.iter() {
            let mut bucket_write = bucket.write().expect("obtain kbucket write lock");
            bucket_write.clear();
        }
    }

}

impl Display for PeerInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut res = format!("Peer Info:\n\tid: {}\n\tbuckets:\n", &self.id);
        let mut start_empty: Option<usize> = None;
        for (i, bucket) in self.buckets.iter().enumerate() {
            let bucket_read = bucket.read().expect("obtain kbucket read lock");
            // consolidate empty rows
            // cur is empty
            let cur_empty = bucket_read.is_empty();
            // prev was empty
            let prev_empty = start_empty.is_some();
            // last one of loop
            let last_one = i == self.buckets.len()-1;

            match (cur_empty, prev_empty, last_one) {
                (true, true, true) => {
                    res.push_str(&format!("\t\t2^{}-{}:\tNone\n", start_empty.unwrap()+1, i+1));
                },
                (false, true, true) => {
                    if start_empty.unwrap()+1 != i {
                        res.push_str(&format!("\t\t2^{}-{}:\tNone\n", start_empty.unwrap()+1, i));
                    } else {
                        res.push_str(&format!("\t\t2^{}:\tNone\n", i));
                    }
                    res.push_str(&format!("\t\t2^{}:\t{}\n", i+1, bucket_read));
                    start_empty = None;
                },
                (true, true, false) => {
                    // pass
                },
                (false, true, false) => {
                    if start_empty.unwrap()+1 != i {
                        res.push_str(&format!("\t\t2^{}-{}:\tNone\n", start_empty.unwrap()+1, i));
                    } else {
                        res.push_str(&format!("\t\t2^{}:\tNone\n", i));
                    }
                    res.push_str(&format!("\t\t2^{}:\t{}\n", i+1, bucket_read));
                    start_empty = None;
                },
                (true, false, true) => {
                    res.push_str(&format!("\t\t2^{}:\tNone\n", i+1));
                },
                (false, false, true) => {
                    res.push_str(&format!("\t\t2^{}:\t{}\n", i+1, bucket_read));
                },
                (true, false, false) => {
                    start_empty = Some(i);
                },
                (false, false, false) => {
                    res.push_str(&format!("\t\t2^{}:\t{}\n", i+1, bucket_read));
                },
            };
        }
        write!(f, "{}", res)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kbucket_move_to_back() {
        let mut l1 = KBucket::new();
        let k1 = Key::from(&[255; KEY_SIZE_BYTES]);
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
        let k1 = Key::from(&[0; KEY_SIZE_BYTES]);
        let k2 = Key::from(&[255; KEY_SIZE_BYTES]);
        let k3 = Key::from(&[1; KEY_SIZE_BYTES]); 
        let k4 = Key::from(&[0, 0, 1, 0]);
        let k5 = Key::from(&[128; KEY_SIZE_BYTES]); 
        let k6 = Key::from(&[127; KEY_SIZE_BYTES]); 
        let k7 = Key::from(&[252; KEY_SIZE_BYTES]); 
        let p1 = PeerInfo::new(k1.clone());
        assert_eq!(0,  p1.bucket_of(&k1));
        assert_eq!(31, p1.bucket_of(&k2));
        assert_eq!(24, p1.bucket_of(&k3));
        assert_eq!(8,  p1.bucket_of(&k4));
        assert_eq!(31, p1.bucket_of(&k5));
        assert_eq!(30, p1.bucket_of(&k6));
        assert_eq!(31, p1.bucket_of(&k7));
        let p2 = PeerInfo::new(k2.clone());
        assert_eq!(31, p2.bucket_of(&k1));
        assert_eq!(0,  p2.bucket_of(&k2));
        assert_eq!(31, p2.bucket_of(&k3));
        assert_eq!(31, p2.bucket_of(&k4));
        assert_eq!(30, p2.bucket_of(&k5));
        assert_eq!(31, p2.bucket_of(&k6));
        assert_eq!(25, p2.bucket_of(&k7));
    }

    #[test]
    fn test_remove() {
        let mut bucket1 = KBucket::new();
        let k1 = Key::from(&[255; KEY_SIZE_BYTES]);
        let k2 = Key::from(&[254; KEY_SIZE_BYTES]);
        let k3 = Key::from(&[253; KEY_SIZE_BYTES]);
        let k4 = Key::from(&[252; KEY_SIZE_BYTES]);
        let k5 = Key::from(&[251; KEY_SIZE_BYTES]);
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

}
