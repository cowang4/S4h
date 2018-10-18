
use std::sync::Arc;
use std::ops::{Deref, DerefMut};

use bincode_transport;
use chashmap::CHashMap;
use futures::{
    future::{self, Ready},
    prelude::*,
};
use log::{info};
use tarpc::{
    client,
};
use tokio;
use tokio_executor;
use serde::{Deserialize, Serialize};

use crate::key::Key;
use crate::peer_info::{Peer, PeerInfo};


#[derive(Debug, Deserialize, Serialize)]
pub struct MessageReturned {
    from_id: Key,               // from peer with id
    key: Option<Key>,           // key/id to store or lookup
    vals: Option<Vec<String>>,  // Strings to store or lookup
    peers: Option<Vec<Peer>>,   // lookup: peers that are closer to key
    sig: (),                    // TODO peer's signature of above data
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

tarpc::service! {
    rpc ping(from: Peer) -> MessageReturned;
    rpc store(pair: (Key, String));
    rpc find_node(node_id: Key) -> MessageReturned;
    rpc find_value(key: Key) -> MessageReturned;
}

#[derive(Clone)]
pub struct S4hServer {
    map: CHashMap<Key, Vec<String>>,
    peer_info: PeerInfo,
}

impl S4hServer {
    
    pub fn new() -> Arc<S4hServer> {
        Arc::new(S4hServer {
            map: CHashMap::<Key, Vec<String>>::new(),
            peer_info: PeerInfo::new(),
        })
    }

    /// Should get a peer with each request,
    /// validate that it's either in the kbuckets, so update
    /// or make a client to it, call ping, verify signature of response, and add it to kbuckets
    pub fn validate_and_update_peer(&mut self, peer: Peer) -> bool {
        // TODO check that S/Kad ID generation requirement is met
        // TODO check that signature authenticates messsage
        // TODO update kbuckets with this node
        let contains = self.peer_info.contains(&peer.id);
        if contains {
            self.peer_info.update(&peer.id);
        }
        else {
            let transport = bincode_transport::connect(&peer.addr)?;
            let options = client::Config::default();
            let client = tokio_executor::spawn(new_stub(options, transport)
                                         .map_err(tarpc::Error::from)
                                         .and_then(|client| {
                                            self.peer_info.insert(&peer.id, peer.addr, Box::new(client));
                                            client.ping(peer)
                                         })
                                         .map_err(|_| ())
                                         .map(|_| ()));
        }
        return true;
    }
}

impl Service for Arc<S4hServer> {
    type PingFut = Ready<MessageReturned>;
    type StoreFut = Ready<()>;
    type FindNodeFut = Ready<MessageReturned>;
    type FindValueFut = Ready<MessageReturned>;

    /// Respond with a () if this peer is alive
    fn ping(&self, from: Peer) -> Self::PingFut {
        info!("Received a ping request");
        self.validate_and_update_peer(from); // TODO handle invalid peers
        let response = MessageReturned::from_id(self.peer_info.id.clone());
        future::ready(response)
    }

    /// Store this pair in the HashTable
    /// pair: (Key, URL)
    fn store(&self, pair: (Key, String)) -> Self::StoreFut {
        info!("Received a store request");
        if self.map.contains_key(&pair.0) {
            self.map.get_mut(&pair.0).unwrap().deref_mut().push(pair.1);
        }
        else {
            self.map.insert_new(pair.0, vec![pair.1]);
        }
        future::ready(())
    }

    /// Find a node by it's ID
    fn find_node(&self, node_id: Key) -> Self::FindNodeFut {
        unimplemented!();
    }

    /// Find a value by it's key
    fn find_value(&self, key: Key) -> Self::FindValueFut {
        info!("Received a find value request");
        match self.map.get(&key) {
            Some(values) => {
                let mut response = MessageReturned::from_id(self.peer_info.id.clone());
                response.key = Some(key);
                response.vals = Some(values.deref().clone());
                return future::ready(response);
            },
            None => {
                unimplemented!();
            }
        }
    }
}

