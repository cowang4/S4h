
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
};
use log::{debug, info, warn, error};
use tarpc::{
    client,
    context,
};
use serde::{Deserialize, Serialize};

use crate::key::{Key, key_fmt, key_dist, key_cmp, option_key_fmt};
use crate::peer_info::{Peer, PeerInfo, ALPHA, K};


#[derive(Debug, Deserialize, Serialize)]
pub struct MessageReturned {
    pub from: Peer,                 // from peer with id
    pub key: Option<Key>,           // key/id to store or lookup
    pub vals: Option<Vec<String>>,  // Strings to store or lookup
    pub peers: Option<Vec<Peer>>,   // lookup: peers that are closer to key
    pub sig: (),                    // TODO peer's signature of above data
}

impl Display for MessageReturned {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "MessageReturned:\n\tfrom: {}\n\tkey: {}\n\tvals: {:?}\n\tpeers: {:?}\n\tsig: {:?}", self.from, option_key_fmt(&self.key), self.vals, self.peers, self.sig)
    }
}

impl MessageReturned {
    
    pub fn from_peer(p: Peer) -> MessageReturned {
        MessageReturned {
            from: p,
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

pub fn validate_peer(_peer: &Peer, _sig: ()) -> bool {
    // TODO check that S/Kad ID generation requirement is met
    // TODO check that signature authenticates peer
    true 
}

pub fn validate_resp(_resp: &MessageReturned) -> bool {
    // TODO check that S/Kad ID generation requirement is met
    // TODO check that signature authenticates resp
    true
}

//TODO make a check for all arguments

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
            self.peer_info.remove(&peer.id);
            error!("Peer: {} failed to validate", &peer);
            return false;
        }

        await!(self.add_peer(&peer));

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
    ///
    /// NOTE peer should be verified (by pinging that IP and checking result) before this get's
    /// called
    async fn add_peer<'a>(&'a self, peer: &'a Peer) -> Option<Box<Client>> {
        if self.peer_info.contains(&peer.id) {
            self.peer_info.update(&peer.id);
            return None;
        }

        info!("Adding peer: {}!", &peer);
        let transport = await!(bincode_transport::connect(&peer.addr)).ok()?;
        let options = client::Config::default();
        let client = await!(new_stub(options, transport)).ok()?;
        await!(self.peer_info.insert(self.get_my_peer(), peer.id.clone(), peer.addr.clone(), Box::new(client)));
        match self.peer_info.get(&peer.id).deref().get(&peer.id) {
            Some(peer)  => peer.client.clone(),
            None        => None,
        }
    }

    pub async fn add_peer_by_addr<'a>(&'a self, addr: SocketAddr) -> Option<Box<Client>> {
        info!("{}: Start add_peer_by_addr: {}!", &self.my_addr, &addr);
    
        //TODO check that that addr isn't already in the kbuckets

        let transport = await!(bincode_transport::connect(&addr));
        if let Ok(transport) = transport {
            let options = client::Config::default();
            let client = await!(new_stub(options, transport));
            if let Ok(mut client) = client {
                debug!("add_peer_by_addr: created a client to {}, now pinging...", &addr);
                let ping_resp = await!(client.ping(context::current(), self.get_my_peer(), ()));
                if let Ok(ping_resp) = ping_resp {
                    if validate_resp(&ping_resp) {
                        let peer = ping_resp.from.clone();
                        await!(self.peer_info.insert(self.get_my_peer(), peer.id.clone(), peer.addr.clone(), Box::new(client)));
                        info!("{}: Finished add_peer_by_addr of {}", &self.my_addr, &peer);
                        return match self.peer_info.get(&peer.id).deref().get(&peer.id) {
                            Some(peer)  => peer.client.clone(),
                            None        => None,
                        };
                    }
                }
            }
        } else {
            warn!("{}: Couldn't add_peer_by_addr for {}. Error: {:?}", &self.my_addr, &addr, transport);
        }
        None
    }

    pub fn get_my_peer(&self) -> Peer {
        Peer::from((self.my_addr, self.peer_info.id.clone()))
    }

    pub async fn node_lookup<'a>(&'a self, key: Key) -> Vec<Peer> {
        info!("{}: Starting node lookup of {}", &self.my_addr, &key_fmt(&key));
        // will be filled with IDs of nodes that have been asked
        let mut queried = HashSet::<Key>::new();
        let mut closest_peers = Vec::<(Key, Peer)>::new(); // dist, Peer
        
        // Setup initial alpha peers
        let closest_initial_peers = self.peer_info.closest_alpha_peers(key.clone());
        if let Some(closest_initial_peers) = closest_initial_peers {
            for peer in closest_initial_peers {
                closest_peers.push((key_dist(&key, &peer.id), peer));
            }
        }
        closest_peers.sort_unstable_by(|a, b| key_cmp(&a.0, &b.0));
        let mut closest_peer: Option<(Key, Peer)> = None;

        loop {
            info!("Starting next iteration of node_lookup. {} queried so far.", queried.len());
            // gets the closest K or ALPHA (depending on the iteration) un-queried peers
            let to_query = match &closest_peer {
                None => {
                    closest_peers.iter_mut().filter(|peer| !queried.contains(&peer.1.id)).take(K).collect::<Vec<&mut (Key, Peer)>>()
                },
                Some(_) => {
                    closest_peers.iter_mut().filter(|peer| !queried.contains(&peer.1.id)).take(ALPHA).collect::<Vec<&mut (Key, Peer)>>()
                },
                
            };

            // will be filled with the peers that get returned
            let mut next_closest_peers = Vec::new();

            // query each peer in to_query
            for peer_tup in to_query {
                // mark this peer as queried so we don't ask it again
                queried.insert(peer_tup.1.id.clone());
                let mut peer_client = peer_tup.1.client.clone().expect("client");
                // TODO do this asyncly
                // call find_node on that peer
                let find_node_resp: Result<MessageReturned, std::io::Error> = await!(peer_client.find_node(context::current(), self.get_my_peer(), (), key.clone()));
                debug!("find_node returned: {:?}", &find_node_resp);
                if let Ok(find_node_resp) = find_node_resp {
                    // TODO do this asyncly
                    // validate the response, and update our kbuckets
                    let valid: bool = await!(self.validate_and_update_or_add_peer_with_sig(find_node_resp.from.clone(), find_node_resp.sig.clone()));
                    if !valid {
                        continue;
                    }
                    if let Some(peers) = find_node_resp.peers {
                        for mut peer in peers {
                            // check that this node isn't us
                            if peer.id != self.peer_info.id {
                                let optional_client: Option<Box<Client>> = await!(self.add_peer(&peer));
                                peer.client = optional_client;
                                next_closest_peers.push((key_dist(&key, &peer.id), peer));
                            }
                        }
                    }
                } else {
                    warn!("node_lookup find_node error: {:?}", &find_node_resp);
                }
            }

            if closest_peers.len() == 0 || next_closest_peers.len() == 0 {
                break;
            }
            for new_peer in next_closest_peers {
                // check that we don't already know about this peer
                if !closest_peers.contains(&new_peer) {
                    closest_peers.push(new_peer);
                }
            }
            closest_peer = Some(closest_peers[0].clone());
            closest_peers.sort_unstable_by(|a, b| key_cmp(&a.0, &b.0));

            // If a round
            // of FIND NODEs fails to return a node any closer
            // than the closest already seen, the initiator resends
            // the FIND NODE to all of the k closest nodes it has
            // not already queried.
            // We will set closest_peer = None to signal to above to to_query K not ALPHA
            if closest_peer.clone().expect("a peer").1.id == closest_peers.get(0).expect("another peer").1.id {
                closest_peer = None;
            }
        }

        info!("{}: Finished node lookup of {}", &self.my_addr, &key_fmt(&key));

        closest_peers.truncate(K);
        closest_peers.iter().map(|p| p.1.clone()).collect()
    }

    /// Looks up the closest peers to key in the DHT and then sends them a store
    pub async fn store(&self, key: Key, value: String) {
        let closest_peers_in_dht: Vec<Peer> = await!(self.node_lookup(key.clone()));
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            if let Some(mut client) = peer.client {
                await!(client.store(context::current(), self.get_my_peer(), (), key.clone(), value.clone()));
            }
        }
        info!("{}: Finished store. Sent to {} peers.", &self.my_addr, closest_peers_in_dht_len);
    }

    pub async fn find_value(&self, key: Key) -> Vec<String> {
        let closest_peers_in_dht: Vec<Peer> = await!(self.node_lookup(key.clone()));
        let mut values = Vec::new();
        for peer in closest_peers_in_dht {
            if let Some(mut client) = peer.client {
                let find_val_resp: Option<MessageReturned> = await!(client.find_value(context::current(), self.get_my_peer(), (), key.clone())).ok();
                if let Some(find_val_resp) = find_val_resp {
                    if let Some(resp_vals) = find_val_resp.vals {
                        for value in resp_vals {
                            if !values.contains(&value) {
                                values.push(value);
                            }
                        }
                    }
                }
            }
        }
        values
    }

    pub fn clear(self) {
        self.peer_info.clear();
    }
}

tarpc::service! {
    rpc ping(from: Peer, sig: ()) -> MessageReturned;
    rpc store(from: Peer, sig: (), key: Key, value: String) -> MessageReturned;
    rpc find_node(from: Peer, sig:(), node_id: Key) -> MessageReturned;
    rpc find_value(from: Peer, sig:(), key: Key) -> MessageReturned;
}


impl Service for S4hServer {
    type PingFut = Ready<MessageReturned>;
    type StoreFut = Ready<MessageReturned>;
    type FindNodeFut = Ready<MessageReturned>;
    type FindValueFut = Ready<MessageReturned>;

    /// Respond if this peer is alive
    fn ping(self, _context: context::Context, from: Peer, sig: ()) -> Self::PingFut {
        info!("{}: Received a ping request from {}", &self.my_addr, &from);
        
        // Validate request
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid ping request from peer: {}", &from);
        }

        info!("Updated Server:\n{}", self);

        info!("{}: Finished a ping request from {}", &self.my_addr, &from);
        let response = MessageReturned::from_peer(self.get_my_peer());
        future::ready(response)
    }

    /// Store this pair in the HashTable
    /// pair: (Key, URL)
    fn store(self, _context: context::Context, from: Peer, sig: (), key: Key, value: String) -> Self::StoreFut {
        info!("{}: Received a store request from {}", &self.my_addr, &from);

        // validate request
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid store request from peer: {:?}", &from);
        }

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

        info!("Updated Server:\n{}", self);
        info!("{}: Finished a store request from {}", &self.my_addr, &from);

        // build response
        let mut response = MessageReturned::from_peer(self.get_my_peer());
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
        info!("{}: Received a find node request from {} for node_id: {}", &self.my_addr, &from, &key_fmt(&node_id));

        // Validate request
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid ping request from peer: {}", &from);
        }

        info!("Updated Server:\n{}", self);

        let closest_peers = self.peer_info.closest_k_peers(node_id.clone());
        info!("{}: Finished a find node request from {} for node_id: {}", &self.my_addr, &from, &key_fmt(&node_id));
        let mut response = MessageReturned::from_peer(self.get_my_peer());
        response.key = Some(node_id);
        response.peers = closest_peers;
        info!("Actually done find_node now...");
        future::ready(response)
    }

    /// Find a value by it's key
    fn find_value(self, _context: context::Context, from: Peer, sig: (), key: Key) -> Self::FindValueFut {
        info!("{}: Received a find value request from {} for key: {}", &self.my_addr, &from, &key_fmt(&key));

        // Validate request
        let mut spawner2 = self.spawner.clone();
        let update = self.validate_and_update_or_add_peer_with_sig(from.clone(), sig);
        let valid: bool = spawner2.run(update);
        if !valid {
            warn!("Invalid ping request from peer: {}", &from);
        }

        let mut response = MessageReturned::from_peer(self.get_my_peer());
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

        info!("Updated Server:\n{}", self);

        let closest_peers = self.peer_info.closest_k_peers(key.clone());
        response.peers = closest_peers;
        future::ready(response)
    }
}

