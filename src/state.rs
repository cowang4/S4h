
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::ops::{Deref};
use std::sync::{Arc, RwLock};

use chashmap::CHashMap;
use log::{debug, info, warn, error};
use serde_derive::{Deserialize, Serialize};

use crate::client;
use crate::key::{Key, option_key_fmt, key_dist, key_cmp, key_fmt, key_inverse};
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
}

impl Display for S4hState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "S4hState:\n\tmap: {:?}\n\tmy_addr: {:?}\n\tpeer_info: {}", self.map, self.my_addr, self.peer_info)
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
    
    pub fn new() -> S4hState {
        S4hState {
            map: Arc::new(CHashMap::<Key, Vec<String>>::new()),
            peer_info: Arc::new(PeerInfo::new(None)),
            my_addr: "127.0.0.1:1234".parse().unwrap(),
            complaints: Arc::new(CHashMap::<Key, (HashSet<Key>, HashSet<Key>)>::new()),
            cr_avg: Arc::new(RwLock::new(1.0)),
            cf_avg: Arc::new(RwLock::new(1.0)),
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
    /// NOTE peer should be verified (by pinging that IP and checking result) before this get's
    /// called. Or TCP or HTTP or something needs to verify that the addr actually belongs to the
    /// peer
    fn add_peer<'a>(&'a self, peer: &'a Peer) {
        if self.peer_info.contains(&peer.id) {
            self.peer_info.update(&peer.id);
            return;
        }

        info!("Adding peer: {}!", &peer);
        self.peer_info.insert(self.get_my_peer(), peer.id.clone(), peer.addr.clone());
    }

    pub fn add_peer_by_addr<'a>(&'a self, addr: SocketAddr) {
        info!("{}: Start add_peer_by_addr: {}!", &self.my_addr, &addr);
    
        // check that that addr isn't already in the kbuckets
        if self.peer_info.contains_addr(&addr) {
            debug!("{}: add_peer_by_addr: contains addr {}", &self.my_addr, &addr);
            #[allow(unused_assignments)]
            let mut peer_id: Option<Key> = None;
            {
                let bucket = self.peer_info.get_peer_by_addr(&addr);
                debug!("{}: add_peer_by_addr: got bucket", &self.my_addr);
                let peer = bucket.deref().get_peer_by_addr(&addr).expect("peer that I just checked for");
                peer_id = Some(peer.id.clone());
            }
            let peer_id = peer_id.unwrap();
            self.peer_info.update(&peer_id);
            debug!("{}: add_peer_by_addr: already contains {}", &self.my_addr, &addr);
            return;
        }

        let ping_resp = client::ping(addr, self.get_my_peer());
        if let Ok(ping_resp) = ping_resp {
            if validate_resp(&ping_resp) {
                let peer = ping_resp.from.clone();
                self.peer_info.insert(self.get_my_peer(), peer.id.clone(), peer.addr.clone());
                info!("{}: Finished add_peer_by_addr of {}", &self.my_addr, &peer);
            }
        }
    }

    pub fn get_my_peer(&self) -> Peer {
        Peer::from((self.my_addr, self.peer_info.id.clone()))
    }

    pub fn node_lookup<'a>(&'a self, key: Key) -> Vec<Peer> {
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
                    if let Some(peers) = find_node_resp.peers {
                        for peer in peers {
                            // check that this node isn't us
                            if peer.id != self.peer_info.id {
                                self.add_peer(&peer);
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
    pub fn store(&self, key: Key, value: String) {
        info!("{}: Starting store.", &self.my_addr);
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(key.clone());
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            let _ = client::store(peer.addr, self.get_my_peer(), key.clone(), value.clone());
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
            }
        }
        values
    }


    /// Looks up the closest peers to against in the DHT and then sends them a store_complaint_against
    pub fn store_complaint_against(&self, against: Key) {
        info!("{}: Starting store_complaint_against.", &self.my_addr);
        let against_bucket = self.peer_info.get(&against);
        let against_peer = against_bucket.deref().get(&against);
        if against_peer.is_none() {
            error!("Peer not known with key: {}", &key_fmt(&against));
            return;
        }
        let against_peer = against_peer.unwrap();
        let inverse_by = key_inverse(&against);
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(inverse_by.clone());
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            let _ = client::store_complaint_against(peer.addr, self.get_my_peer(), against_peer.clone());
        }
        info!("{}: Finished store_complaint_against. Sent to {} peers.", &self.my_addr, closest_peers_in_dht_len);
    }


    /// Looks up the closest peers to against.addr in the DHT and then sends them a store_complaint_by
    pub fn store_complaint_by(&self, by: Peer, sig_by: (), against: Peer) {
        info!("{}: Starting store_complaint_by.", &self.my_addr);
        let inverse_by = key_inverse(&by.id);
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(inverse_by.clone());
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            let _ = client::store_complaint_by(peer.addr, self.get_my_peer(), by.clone(), sig_by, against.clone());
        }
        info!("{}: Finished store_complaint_by. Sent to {} peers.", &self.my_addr, closest_peers_in_dht_len);
    }


    /// Names from Aberer paper
    fn get_complaints(&self, q: Key) -> HashSet<WitnessReport> {
        let mut res = HashSet::new();
        let closest_peers_in_dht: Vec<Peer> = self.node_lookup(q.clone());
        let closest_peers_in_dht_len = closest_peers_in_dht.len();
        for peer in closest_peers_in_dht {
            let query_res = client::query_complaints(peer.addr, self.get_my_peer(), q.clone()).expect("query_complaint to succeed");
            if let Some(data) = query_res.complaints {
                let witness_report = WitnessReport::from_complaint_data(peer.id.clone(), data);
                res.insert(witness_report);
            }
        }
        info!("{}: Finished get_complaints. Asked {} peers.", &self.my_addr, closest_peers_in_dht_len);
        res
    }


    fn decide_reputation(&self, cr: usize, cf: usize) -> isize {
        let cr_avg = self.cr_avg.read().unwrap();
        let cf_avg = self.cf_avg.read().unwrap();
        let sqr_term = (0.5 + (4.0 / (*cr_avg * *cf_avg).sqrt())).powi(2);
        if cr as f32 * cf as f32 <= sqr_term * *cr_avg * *cf_avg {
            1
        } else {
            -1
        }
    }


    pub fn explore_trust_simple(&self, q: Key) -> isize {
        let w = self.get_complaints(q.clone());

        // TODO update average statistics with W
        
        let s: isize = w.iter().map(|wr| self.decide_reputation(wr.cr, wr.cf)).sum();

        match s {
            x if x > 0  => 1,
            x if x < 0  => -1,
            _           => 0,
        }
    }

    #[allow(dead_code)]
    pub fn clear(self) {
        self.peer_info.clear();
    }
}


