
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc};
use std::ops::{Deref, DerefMut};

use bincode_transport;
use chashmap::CHashMap;
use futures::{
    executor::{ThreadPool},
    future::{self, Ready},
};
use log::{info, warn};
use tarpc::{
    client,
    context,
};
use serde::{Deserialize, Serialize};

use crate::key::{Key, key_fmt, option_key_fmt};
use crate::peer_info::{Peer, PeerInfo};


#[derive(Debug, Deserialize, Serialize)]
pub struct MessageReturned {
    from_id: Key,               // from peer with id
    key: Option<Key>,           // key/id to store or lookup
    vals: Option<Vec<String>>,  // Strings to store or lookup
    peers: Option<Vec<Peer>>,   // lookup: peers that are closer to key
    sig: (),                    // TODO peer's signature of above data
}

impl Display for MessageReturned {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "MessageReturned:\n\tfrom: {:?}\n\tkey: {:?}\n\tvals: {:?}\n\tpeers: {:?}\n\tsig: {:?}", key_fmt(&self.from_id), option_key_fmt(&self.key), self.vals, self.peers, self.sig)
    }
}

impl MessageReturned {
    
    pub fn from_id(k: Key) -> MessageReturned {
        MessageReturned {
            from_id: k,
            key: None,
            vals: None,
            peers: None,
            sig: (),
        }
    }
}

#[derive(Debug, Clone)]
pub struct S4hServer {
    map: Arc<CHashMap<Key, Vec<String>>>,
    peer_info: Arc<PeerInfo>,
    my_addr: SocketAddr,
    spawner: ThreadPool,
}

fn validate_peer(_peer: &Peer, _sig: ()) -> bool {
    // TODO check that S/Kad ID generation requirement is met
    // TODO check that signature authenticates messsage
    // TODO update kbuckets with this node
    true 
}

impl S4hServer {
    
    pub fn new(my_addr: &SocketAddr, spawner: ThreadPool) -> S4hServer {
        S4hServer {
            map: Arc::new(CHashMap::<Key, Vec<String>>::new()),
            peer_info: Arc::new(PeerInfo::new()),
            my_addr: my_addr.clone(),
            spawner: spawner,
        }
    }
    
    /// Should get a peer with each request,
    /// validate that it's either in the kbuckets, so update
    /// or add it
    pub async fn validate_and_update_or_add_peer_with_sig(&self, peer: Peer, sig: ()) -> bool {
        if validate_peer(&peer, sig) == false {
            return false;
        }
        let contains = self.peer_info.contains(&peer.id);
        if contains {
            self.peer_info.update(&peer.id);
        }
        else {
            info!("Adding peer: {:?}!", &peer);
            await!(self.add_peer(&peer));
        }
        return true;
    }

    pub async fn add_peer<'a>(&'a self, peer: &'a Peer) -> Option<Box<Client>> {
        let transport = await!(bincode_transport::connect(&peer.addr));
        if let Ok(transport) = transport {
            let options = client::Config::default();
            let client = await!(new_stub(options, transport));
            if let Ok(client) = client {
                self.peer_info.insert(&peer.id, peer.addr.clone(), Box::new(client));
                return self.peer_info.get(&peer.id).deref().get(&peer.id).expect("peer that I just added").client.clone();
            }
        }
        None
    }

    pub fn get_my_peer(&self) -> Peer {
        Peer::from((self.my_addr, self.peer_info.id.clone()))
    }
}

tarpc::service! {
    rpc ping(from: Peer, sig: ()) -> MessageReturned;
    rpc store(key: Key, value: String) -> MessageReturned;
    rpc find_node(node_id: Key) -> MessageReturned;
    rpc find_value(key: Key) -> MessageReturned;
}

impl Service for S4hServer {
    type PingFut = Ready<MessageReturned>;
    type StoreFut = Ready<MessageReturned>;
    type FindNodeFut = Ready<MessageReturned>;
    type FindValueFut = Ready<MessageReturned>;

    /// Respond if this peer is alive
    fn ping(&self, _context: context::Context, from: Peer, sig: ()) -> Self::PingFut {
        info!("Received a ping request");
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid ping request from peer: {:?}", &from);
        }
        info!("Updated Server: {:?}", self);
        let response = MessageReturned::from_id(self.peer_info.id.clone());
        future::ready(response)
    }

    /// Store this pair in the HashTable
    /// pair: (Key, URL)
    fn store(&self, _context: context::Context, key: Key, value: String) -> Self::StoreFut {
        info!("Received a store request");
        if self.map.contains_key(&key) {
            info!("Pushing back value");
            self.map.get_mut(&key).unwrap().deref_mut().push(value);
        }
        else {
            info!("Insert_new key with value");
            self.map.insert_new(key.clone(), vec![value]);
        }
        // TODO forward to closer nodes
        let mut response = MessageReturned::from_id(self.peer_info.id.clone());
        response.key = Some(key.clone());
        match self.map.get(&key) {
            Some(values) => {
                response.vals = Some(values.deref().clone());
            },
            None => {
                response.vals = None;
            }
        }
        future::ready(response)
    }

    /// Find a node by it's ID
    fn find_node(&self, _context: context::Context, node_id: Key) -> Self::FindNodeFut {
        unimplemented!();
    }

    /// Find a value by it's key
    fn find_value(&self, _context: context::Context, key: Key) -> Self::FindValueFut {
        info!("Received a find value request");
        match self.map.get(&key) {
            Some(values) => {
                info!("Found key in store");
                let mut response = MessageReturned::from_id(self.peer_info.id.clone());
                response.key = Some(key);
                response.vals = Some(values.deref().clone());
                return future::ready(response);
            },
            None => {
                info!("Didn't found key in store");
                unimplemented!();
            }
        }
    }
}

