
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    RwLock
};

use chashmap::CHashMap;
use log::{debug, info, warn, error};
use serde_derive::{Deserialize, Serialize};

use crate::client;
use crate::key::{Key};
use crate::peer_info::{Peer, PeerInfo, ALPHA, K};
use crate::reputation::{WitnessReport};


/// complaints against key, complaints submitted by key
pub type ComplaintData = (HashSet<Key>, HashSet<Key>);


#[derive(Debug, Deserialize, Serialize)]
pub struct MessageReturned {
    /// response from this peer
    pub from: Peer,
    /// key/id to store or lookup
    pub key: Option<Key>,
    /// Strings to store or lookup
    pub vals: Option<Vec<String>>,
    /// lookup: peers that are closer to key
    pub peers: Option<Vec<Peer>>,
    /// complaints against key, complaints submitted by key
    pub complaints: Option<ComplaintData>,
    /// TODO peer's signature of above data
    pub sig: (),
}

impl Display for MessageReturned {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "MessageReturned:\n\tfrom: {}\n\tkey: {:?}\n\tvals: {:?}\n\tpeers: {:?}\n\tcomplaints: {:?}\n\tsig: {:?}", self.from, self.key, self.vals, self.peers, self.complaints, self.sig)
    }
}

impl MessageReturned {
    
    pub fn from_peer(p: Peer) -> MessageReturned {
        MessageReturned {
            from: p,
            key: None,
            vals: None,
            peers: None,
            complaints: None,
            sig: (),
        }
    }
}

#[derive(Debug, Clone)]
pub struct S4hState {
    /// Map from Content-address of search query to urls.
    pub map: Arc<CHashMap<Key, Vec<String>>>,
    /// The Kad DHT's state.
    pub peer_info: Arc<PeerInfo>,
    /// This server's TCP Address and Port.
    pub my_addr: SocketAddr,
    /// Map from NodeID to (set of complaints against key, set of complaints submitted by key)
    pub complaints: Arc<CHashMap<Key, (HashSet<Key>, HashSet<Key>)>>,
    /// the average number of complaints against a node
    pub cr_avg: Arc<RwLock<f32>>,
    /// the average number of complaints by a node
    pub cf_avg: Arc<RwLock<f32>>,
    /// the number of reputation queries
    pub avg_n: Arc<AtomicUsize>,
    /// the addrs that are waiting to get verified
    unverified_addrs: Arc<RwLock<HashSet<SocketAddr>>>,
}

impl Display for S4hState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "S4hState:\n\tmap: {:?}\n\tmy_addr: {:?}\n\tunverified_addrs: {:?}\n\tpeer_info: {}\n\tcomplaints: {:?}", self.map, self.my_addr, self.unverified_addrs, self.peer_info, self.complaints)
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

impl S4hState {
    
    pub fn new(node_id: Key) -> S4hState {
        S4hState {
            map: Arc::new(CHashMap::<Key, Vec<String>>::new()),
            peer_info: Arc::new(PeerInfo::new(node_id)),
            my_addr: "127.0.0.1:1234".parse().unwrap(),
            complaints: Arc::new(CHashMap::<Key, (HashSet<Key>, HashSet<Key>)>::new()),
            cr_avg: Arc::new(RwLock::new(0.0)),
            cf_avg: Arc::new(RwLock::new(0.0)),
            avg_n: Arc::new(AtomicUsize::new(1)),
            unverified_addrs: Arc::new(RwLock::new(HashSet::new())),
        }
    }
    
    /// Should get a peer with each request,
    /// validate that it's either in the kbuckets, so update
    /// or add it
    pub fn validate_and_update_or_add_peer_with_sig(&self, peer: Peer, sig: ()) -> bool {
        if validate_peer(&peer, sig) == false {
            self.peer_info.remove(&peer.id);
            error!("Peer: {} failed to validate", &peer);
            return false;
        }

        self.add_peer_by_addr(peer.addr.clone());

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
    pub fn add_peer_by_addr<'a>(&'a self, addr: SocketAddr) {
        // check that it isn't our addr
        if self.my_addr == addr {
            warn!("{}: add_peer_by_addr: tried to add our own addr",&self.my_addr);
            return;
        }
    
        // check that that addr isn't already in the kbuckets
        if self.peer_info.contains_addr(&addr) {
            debug!("{}: add_peer_by_addr: contains addr {}", &self.my_addr, &addr);
            let peer_id: Key = self.peer_info.get_peer_by_addr(&addr).unwrap().id;
            self.peer_info.update(&peer_id);
            return;
        }
        
        let mut waiting_for_verification = false;
        {
            let unverified_addrs_r = self.unverified_addrs.read().unwrap();
            if unverified_addrs_r.contains(&addr) {
                waiting_for_verification = true;
            }
        }
        {
            if !waiting_for_verification {
                let mut unverified_addrs_w = self.unverified_addrs.write().unwrap();
                unverified_addrs_w.insert(addr.clone());
            }
        }
        
        if !waiting_for_verification {
            let ping_resp = client::ping(addr, self.get_my_peer());
            if let Ok(ping_resp) = ping_resp {
                if validate_resp(&ping_resp) {
                    let peer = ping_resp.from.clone();
                    self.peer_info.insert(self.get_my_peer(), peer.id.clone(), peer.addr.clone());
                    debug!("{}: add_peer_by_addr: added {}", &self.my_addr, &peer);
                }
            } else {
                warn!("{}: ping to {} failed with error: {:?}", &self.my_addr, addr, ping_resp);
            }

            
            let mut unverified_addrs_w = self.unverified_addrs.write().unwrap();
            unverified_addrs_w.remove(&addr);
        }
    }


    pub fn get_my_peer(&self) -> Peer {
        Peer::from((self.my_addr, self.peer_info.id.clone()))
    }

    
    pub fn store_in_hashtable(&self, key: Key, value: String) {
        if self.map.contains_key(&key) {
            // check for value, don't want duplicates of the same string value.
            if !self.map.get(&key).unwrap().deref().contains(&value) {
                debug!("Pushing back value: {}", &value);
                self.map.get_mut(&key).unwrap().deref_mut().push(value.clone());
            }
            else {
                debug!("Map already contains value: {}", &value);
            }
        }
        else {
            debug!("Insert_new key: {} with value: {}", &key, &value);
            self.map.insert_new(key.clone(), vec![value.clone()]);
        }
    }


    pub fn node_lookup<'a>(&'a self, key: Key) -> Vec<Peer> {
        info!("{}: Starting node lookup of {}", &self.my_addr, &key);
        // will be filled with IDs of nodes that have been asked
        let mut queried = HashSet::<Key>::new();
        let mut closest_peers = Vec::<(Key, Peer)>::new(); // dist, Peer
        let mut last_closest_peers = Vec::<(Key, Peer)>::new(); // dist, Peer
        
        // Setup initial alpha peers
        let closest_initial_peers = self.peer_info.closest_alpha_peers(key.clone());
        if let Some(closest_initial_peers) = closest_initial_peers {
            for peer in closest_initial_peers {
                closest_peers.push((key.dist(&peer.id), peer));
            }
        }
        closest_peers.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut closest_peer: Option<(Key, Peer)> = None;

        loop {
            debug!("Starting next iteration of node_lookup. {} queried so far.", queried.len());
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
                let peer_addr = peer_tup.1.addr.clone();
                // TODO do this asyncly
                // call find_node on that peer
                let find_node_resp = client::find_node(peer_addr, self.get_my_peer(), key.clone());
                debug!("find_node returned: {:?}", &find_node_resp);
                if let Ok(find_node_resp) = find_node_resp {
                    // TODO do this asyncly
                    // validate the response, and update our kbuckets
                    let valid: bool = self.validate_and_update_or_add_peer_with_sig(find_node_resp.from.clone(), find_node_resp.sig.clone());
                    if !valid {
                        continue;
                    }
                    // add the peers that we just received to next_closest_peers
                    if let Some(peers) = find_node_resp.peers {
                        for peer in peers {
                            // check that this node isn't us
                            if peer.id != self.peer_info.id {
                                self.add_peer_by_addr(peer.addr.clone());
                                next_closest_peers.push((key.dist(&peer.id), peer));
                            }
                        }
                    }
                } else {
                    error!("{}: node_lookup find_node error: {:?}", &self.my_addr, &find_node_resp);
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
            closest_peers.sort_unstable_by(|a, b| a.0.cmp(&b.0));

            // If a round
            // of FIND NODEs fails to return a node any closer
            // than the closest already seen, the initiator resends
            // the FIND NODE to all of the k closest nodes it has
            // not already queried.
            // We will set closest_peer = None to signal to above to to_query K not ALPHA
            let mut found_closer = false;
            if last_closest_peers.iter().take(K).len() != closest_peers.iter().take(K).len() {
                found_closer = true;
            }
            for (p1, p2) in last_closest_peers.iter().take(K).zip(closest_peers.iter().take(K)) {
                if p1.1.id != p2.1.id {
                    found_closer = true;
                }
            }
            last_closest_peers = closest_peers.clone();
            if !found_closer {
                closest_peer = None;
            }
        }

        closest_peers.truncate(K);
        info!("{}: Finished node lookup of {}, found {:?}", &self.my_addr, &key, &closest_peers);
        closest_peers.iter().map(|p| p.1.clone()).collect()
    }


    fn node_lookup_exact(&self, node_id: Key) -> Option<Peer> {
        let peers = self.node_lookup(node_id.clone());
        for peer in peers {
            if peer.id == node_id {
                return Some(peer.clone());
            }
        }
        return None;
    }

    /// Lookup our own node_id to fill in our kbuckets
    pub fn find_self(&self) {
        info!("{}: Looking up our own node_id", &self.my_addr);
        let _ = self.node_lookup(self.get_my_peer().id);
    }


    /// Looks up the closest peers to key in the DHT and then sends them a store
    pub fn store(&self, key: Key, value: String) {
        info!("{}: Starting store.", &self.my_addr);
        self.store_in_hashtable(key.clone(), value.clone());
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(key.clone());
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            let store_resp = client::store(peer.addr, self.get_my_peer(), key.clone(), value.clone());
            if store_resp.is_err() {
                warn!("store to {} failed with error: {:?}", peer, store_resp);
            }
        }
        info!("{}: Finished store. Sent to {} peers.", &self.my_addr, closest_peers_in_dht_len);
    }


    pub fn find_value(&self, key: Key) -> Vec<String> {
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(key.clone());
        let mut values = Vec::new();
        for peer in closest_peers_in_dht {
            let find_val_resp: Option<MessageReturned> = client::find_value(peer.addr, self.get_my_peer(), key.clone()).ok();
            if let Some(find_val_resp) = find_val_resp {
                if let Some(resp_vals) = find_val_resp.vals {
                    for value in resp_vals {
                        if !values.contains(&value) {
                            values.push(value);
                        }
                    }
                }
            } else {
                warn!("find_value to {} failed with error: {:?}", peer, find_val_resp);
            }
        }
        values
    }


    /// Looks up the closest peers to against in the DHT and then sends them a store_complaint_against
    pub fn store_complaint_against(&self, against: &Key) {
        info!("{}: Starting store_complaint_against against {}.", &self.my_addr, against);
        if against == &self.get_my_peer().id {
            error!("Cannot complain against ourselves!");
            return;
        }
        let mut against_peer = self.peer_info.get(against);
        if against_peer.is_none() {
            let looked_up_peer = self.node_lookup_exact(against.clone());
            if looked_up_peer.is_none() {
                error!("Peer not known with key: {}", against);
                return;
            }
            against_peer = looked_up_peer;
        }
        let against_peer = against_peer.unwrap();
        let inverse_by = against.inverse();
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(inverse_by.clone());
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            let store_complaint_against_resp = client::store_complaint_against(peer.addr, self.get_my_peer(), against_peer.clone());
            if store_complaint_against_resp.is_err() {
                warn!("store_complaint_against to {} failed with error: {:?}", peer, store_complaint_against_resp);
            }
        }
        info!("{}: Finished store_complaint_against. Sent to {} peers.", &self.my_addr, closest_peers_in_dht_len);
    }


    /// Looks up the closest peers to against.addr in the DHT and then sends them a store_complaint_by
    pub fn store_complaint_by(&self, by: Peer, sig_by: (), against: Peer) {
        info!("{}: Starting store_complaint_by.", &self.my_addr);
        let inverse_by = by.id.inverse();
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(inverse_by.clone());
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            let store_complaint_by_resp = client::store_complaint_by(peer.addr, self.get_my_peer(), by.clone(), sig_by, against.clone());
            if store_complaint_by_resp.is_err() {
                warn!("store_complaint_by to {} failed with error: {:?}", peer, store_complaint_by_resp);
            }
        }
        info!("{}: Finished store_complaint_by. Sent to {} peers.", &self.my_addr, closest_peers_in_dht_len);
    }


    /// Names from Aberer paper
    fn get_complaints(&self, q: Key) -> HashSet<WitnessReport> {
        let mut res = HashSet::new();
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(q.inverse());
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            let query_res = client::query_complaints(peer.addr, self.get_my_peer(), q.clone());
            if let Ok(query_res) = query_res {
                if let Some(data) = query_res.complaints {
                    debug!("peer {} returned complaints {:?}", peer.id.clone(), &data);
                    let witness_report = WitnessReport::from_complaint_data(peer.id.clone(), data);
                    res.insert(witness_report);
                }
            } else {
                warn!("query_complaints to {} failed with error: {:?}", peer, query_res);
            }
        }
        info!("{}: Finished get_complaints. Asked {} peers.", &self.my_addr, closest_peers_in_dht_len);
        res
    }


    /// BUG: if the bad actor doesn't complain back, then it's cf will be 0,
    /// and therefore it's reputation will always be 1
    fn decide_reputation(&self, cr: usize, cf: usize) -> isize {
        let cr_avg = self.cr_avg.read().unwrap();
        let cf_avg = self.cf_avg.read().unwrap();
        let sqr_term = (0.5 + (4.0 / (*cr_avg * *cf_avg).sqrt())).powi(2);
        warn!("sqr_term: {}  cr: {}  cf: {}", &sqr_term, cr, cf);
        warn!("score: {}    threshold: {}", cr as f32 * cf as f32, sqr_term * *cr_avg * *cf_avg);
        if cr as f32 * cf as f32 <= sqr_term * *cr_avg * *cf_avg {
            1
        } else {
            -1
        }
    }


    pub fn explore_trust_simple(&self, q: &Key) -> isize {
        let w = self.get_complaints(q.clone());

        // update average statistics with W
        if w.len() > 0 {
            let cr: usize = w.iter().map(|wr| wr.cr).sum::<usize>() / w.len();
            let cf: usize = w.iter().map(|wr| wr.cf).sum::<usize>() / w.len();
            warn!("explore_trust_simple  cr {}  cf {}", cr, cf);
            self.update_statistics(cr, cf);
        }
        
        let s: isize = w.iter().map(|wr| self.decide_reputation(wr.cr, wr.cf)).sum();

        match s {
            x if x > 0  => 1,
            x if x < 0  => -1,
            _           => 0,
        }
    }

    fn update_statistics(&self, cr: usize, cf: usize) {
        let mut cr_avg = self.cr_avg.write().unwrap();
        let mut cf_avg = self.cf_avg.write().unwrap();
        let avg_n = self.avg_n.load(Ordering::SeqCst) as f32;
        *cr_avg = ((avg_n * *cr_avg) + cr as f32) as f32 / (avg_n+1.0);
        *cf_avg = ((avg_n * *cf_avg) + cf as f32) as f32 / (avg_n+1.0);
        let _ = self.avg_n.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    pub fn clear(self) {
        self.peer_info.clear();
    }
}


