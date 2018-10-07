
use std::ops::{Deref, DerefMut};

use chashmap::CHashMap;
use tarpc::util::{Never};

use key::Key;
use peer_info::{Peer, PeerInfo};


#[derive(Debug, Deserialize, Serialize)]
pub enum FindValueReturned {
    Values(Vec<String>),
    Peers(Vec<Peer>),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageReturned {
    from_id: Key,               // from peer with id
    key: Option<Key>,           // key/id to store or lookup
    vals: Option<Vec<String>>,  // Strings to store or lookup
    peers: Option<Vec<Peer>>,   // lookup: peers that are closer to key
    sig: (),                    // TODO peer's signature of above data
}

service! {
    rpc ping();
    rpc store(pair: (Key, String));
    rpc find_node(node_id: Key) -> Vec<Peer>;
    rpc find_value(key: Key) -> FindValueReturned;
}

#[derive(Clone)]
pub struct S4hServer {
    map: CHashMap<Key, Vec<String>>,
    peer_info: PeerInfo, // TODO add mutability
}

impl S4hServer {
    
    pub fn new() -> S4hServer {
        S4hServer {
            map: CHashMap::<Key, Vec<String>>::new(),
            peer_info: PeerInfo::new(),
        }
    }

    pub fn validate_and_update_peer(&self, msg: MessageReturned) -> bool {
        // TODO check that S/Kad id generation requirement is met
        // TODO check that signature authenticates messsage
        // TODO update kbuckets with this node
        unimplemented!();
    }
}

impl FutureService for S4hServer {
    type PingFut = Result<(), Never>;
    type StoreFut = Result<(), Never>;
    type FindNodeFut = Result<Vec<Peer>, Never>;
    type FindValueFut = Result<FindValueReturned, Never>;

    /// Ask this peer if it's alive
    fn ping(&self) -> Self::PingFut {
        info!("Received a ping request");
        Ok(())
    }

    /// Ask this peer to store this pair in the HashTable
    /// pair: (Key, URL)
    fn store(&self, pair: (Key, String)) -> Self::StoreFut {
        info!("Received a store request");
        if self.map.contains_key(&pair.0) {
            self.map.get_mut(&pair.0).unwrap().deref_mut().push(pair.1);
        }
        else {
            self.map.insert_new(pair.0, vec![pair.1]);
        }
        Ok(())
    }

    /// Find a node by it's ID
    fn find_node(&self, node_id: Key) -> Self::FindNodeFut {
        unimplemented!();
    }

    /// Find a value by it's key
    fn find_value(&self, key: Key) -> Self::FindValueFut {
        info!("Received a find value request");
        match self.map.get(&key) {
            Some(value) => return Ok(FindValueReturned::Values(value.deref().clone())),
            None => {
                unimplemented!();
            }
        }
    }
}

