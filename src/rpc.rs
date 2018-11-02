
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc};
use std::ops::{Deref, DerefMut};

use bincode_transport;
use chashmap::CHashMap;
use futures::{
    executor::{ThreadPool},
    future::{self, Ready},
    prelude::*,
};
use log::{info, warn, error};
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
        write!(f, "MessageReturned:\n\tfrom: {}\n\tkey: {}\n\tvals: {:?}\n\tpeers: {:?}\n\tsig: {:?}", key_fmt(&self.from_id), option_key_fmt(&self.key), self.vals, self.peers, self.sig)
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

impl Display for S4hServer {
    
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "S4hSever:\n\tmap: {:?}\n\tmy_addr: {:?}\n\tpeer_info: {}", self.map, self.my_addr, self.peer_info)
    }
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
            info!("Adding peer: {}!", &peer);
            await!(self.add_peer(&peer));
        }
        return true;
    }

    /// Adds a peer to the KBuckets using Kad algo:
    /// If the node is not already in the appropriate k-bucket
    /// and the bucket has fewer than k entries, then the re-
    /// cipient just inserts the new sender at the tail of the
    /// list. If the appropriate k-bucket is full, however, then
    /// the recipient pings the k-bucket’s least-recently seen
    /// node to decide what to do. If the least-recently seen
    /// node fails to respond, it is evicted from the k-bucket
    /// and the new sender inserted at the tail. Otherwise,
    /// if the least-recently seen node responds, it is moved
    /// to the tail of the list, and the new sender’s contact is
    /// discarded.
    pub async fn add_peer<'a>(&'a self, peer: &'a Peer) -> Option<Box<Client>> {
        let transport = await!(bincode_transport::connect(&peer.addr));
        if let Ok(transport) = transport {
            let options = client::Config::default();
            let client = await!(new_stub(options, transport));
            if let Ok(client) = client {
                await!(self.peer_info.insert(self.get_my_peer(), peer.id.clone(), peer.addr.clone(), Box::new(client)));
                return self.peer_info.get(&peer.id).deref().get(&peer.id).expect("peer that I just added").client.clone();
            }
        }
        None
    }

    pub fn get_my_peer(&self) -> Peer {
        Peer::from((self.my_addr, self.peer_info.id.clone()))
    }

    pub fn get_client_for_key(&self, key: &Key) -> Option<Box<Client>> {
        let bucket_read = self.peer_info.get(key);
        let bucket = bucket_read.deref();
        match bucket.get(key) {
            Some(peer)  => peer.client.clone(),
            None        => None
        }
    }

    pub fn node_lookup<'a>(&'a self, key: Key) -> Vec<&'a Peer> {
        let queried = HashSet::<Key>::new();
        if let Some(closest_alpha_peers) = self.peer_info.closest_alpha_peers(key.clone()) {

            let closest_alpha_peers = closest_alpha_peers.iter().map(|p| {
                    queried.insert(p.id.clone());
                    p
                })
                .filter_map(|p| p.client);
            let spawner = self.spawner.clone();
            let find_node_resps = closest_alpha_peers.map(|client| spawner.run(client.find_node(context::current(), self.get_my_peer(), (), key.clone()))).collect();
            

        }
        Vec::new()
    }
}

tarpc::service! {
    rpc ping(from: Peer, sig: ()) -> MessageReturned;
    rpc store(from: Peer, sig: (), key: Key, value: String) -> MessageReturned;
    rpc find_node(from: Peer, sig:(), node_id: Key) -> MessageReturned;
    rpc find_value(from: Peer, sig:(), key: Key) -> MessageReturned;
}

async fn store_future(mut client: Box<Client>, my_peer: Peer, sig: (), key: Key, value: String) {
    if let Err(e) = await!(client.store(context::current(), my_peer, sig, key.clone(), value.clone())) {
        error!("ERROR: {:?}", e);
    }
}

impl Service for S4hServer {
    type PingFut = Ready<MessageReturned>;
    type StoreFut = Ready<MessageReturned>;
    type FindNodeFut = Ready<MessageReturned>;
    type FindValueFut = Ready<MessageReturned>;

    /// Respond if this peer is alive
    fn ping(self, _context: context::Context, from: Peer, sig: ()) -> Self::PingFut {
        info!("Received a ping request");
        
        // Validate request
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid ping request from peer: {}", &from);
        }
        info!("Updated Server:\n{}", self);

        let response = MessageReturned::from_id(self.peer_info.id.clone());
        future::ready(response)
    }

    /// Store this pair in the HashTable
    /// pair: (Key, URL)
    fn store(self, _context: context::Context, from: Peer, sig: (), key: Key, value: String) -> Self::StoreFut {
        info!("Received a store request");

        // validate request
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid store request from peer: {:?}", &from);
        }
        info!("Updated Server:\n{}", self);

        // Store in hash table
        if self.map.contains_key(&key) {
            // check for value, don't want duplicates of the same string value.
            if !self.map.get(&key).unwrap().deref().contains(&value) {
                info!("Pushing back value: {}", &value);
                self.map.get_mut(&key).unwrap().deref_mut().push(value.clone());
            }
            else {
                info!("Map already contains value: {}", &value);
            }
        }
        else {
            info!("Insert_new key: {} with value: {}", key_fmt(&key), &value);
            self.map.insert_new(key.clone(), vec![value.clone()]);
        }

        let closer_peers = self.peer_info.closer_k_peers(key.clone());
        if closer_peers.is_some() {
            info!("{} closer peers to key: {}", closer_peers.as_ref().unwrap().len(), &key_fmt(&key));
        }

        // build response
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
        response.peers = closer_peers;
        future::ready(response)
    }

    /// Find a node by it's ID
    /// Iterative, so just returns a list of closer peers.
    fn find_node(self, _context: context::Context, from: Peer, sig: (), node_id: Key) -> Self::FindNodeFut {
        info!("Received a find node request for node_id: {}", &key_fmt(&node_id));

        // Validate request
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid ping request from peer: {}", &from);
        }
        info!("Updated Server:\n{}", self);

        let closest_peers = self.peer_info.closest_k_peers(node_id.clone());
        let mut response = MessageReturned::from_id(self.peer_info.id.clone());
        response.key = Some(node_id);
        response.peers = closest_peers;
        future::ready(response)
    }

    /// Find a value by it's key
    fn find_value(self, _context: context::Context, from: Peer, sig: (), key: Key) -> Self::FindValueFut {
        info!("Received a find value request for key: {}", &key_fmt(&key));

        // Validate request
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid ping request from peer: {}", &from);
        }
        info!("Updated Server:\n{}", self);

        let mut response = MessageReturned::from_id(self.peer_info.id.clone());
        response.key = Some(key.clone());

        // Lookup values
        match self.map.get(&key) {
            Some(values) => {
                info!("Found key: {} in store", &key_fmt(&key));
                response.vals = Some(values.deref().clone());
            },
            None => {
                info!("Didn't found key: {} in store", &key_fmt(&key));
                response.vals = None;
            }
        }

        let closest_peers = self.peer_info.closest_k_peers(key.clone());
        response.peers = closest_peers;
        future::ready(response)
    }
}

