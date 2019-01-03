

use std::ops::{Deref, DerefMut};

use actix_web::{App, http, HttpRequest, Json};
use log::{info, warn};
use serde_derive::{Deserialize, Serialize};

use crate::{
    key::{key_fmt, Key},
    rpc::{S4hState, MessageReturned, validate_peer},
    peer_info::{Peer},
};


pub fn create_app(state: S4hState) -> App<S4hState> {
    App::with_state(state)
        .resource("/ping",          |r| r.method(http::Method::POST).with(ping))
        .resource("/store",         |r| r.method(http::Method::POST).with(store))
        .resource("/find_node",     |r| r.method(http::Method::POST).with(find_node))
        .resource("/find_value",    |r| r.method(http::Method::POST).with(find_value))
}


#[derive(Debug, Deserialize, Serialize)]
pub struct PingArgs {
    pub from: Peer,
    pub sig: (),
}

/// Respond if this peer is alive
pub fn ping((req, args): (HttpRequest<S4hState>, Json<PingArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a ping request from {}", &state.my_addr, &args.from);
    let response = MessageReturned::from_peer(state.get_my_peer());
    
    let valid = validate_peer(&args.from, args.sig);
    if !valid {
        warn!("Invalid ping request from peer: {}", &args.from);
        return Json(response);
    }

    info!("{}: Finished a ping request from {}", &state.my_addr, &args.from);
    Json(response)
}


#[derive(Debug, Deserialize, Serialize)]
pub struct StoreArgs {
    pub from: Peer,
    pub key: Key,
    pub value: String,
    pub sig: (),
}

/// Store this pair in the HashTable
/// pair: (Key, URL)
pub fn store((req, args): (HttpRequest<S4hState>, Json<StoreArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a store request from {}", &state.my_addr, &args.from);
    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid store request from peer: {:?}", &args.from);
        return Json(response);
    }

    // Store in hash table
    if state.map.contains_key(&args.key) {
        // check for value, don't want duplicates of the same string value.
        if !state.map.get(&args.key).unwrap().deref().contains(&args.value) {
            info!("Pushing back value: {}", &args.value);
            state.map.get_mut(&args.key).unwrap().deref_mut().push(args.value.clone());
        }
        else {
            info!("Map already contains value: {}", &args.value);
        }
    }
    else {
        info!("Insert_new key: {} with value: {}", key_fmt(&args.key), &args.value);
        state.map.insert_new(args.key.clone(), vec![args.value.clone()]);
    }

    let closer_peers = state.peer_info.closer_k_peers(args.key.clone());
    if closer_peers.is_some() {
        info!("{} closer peers to key: {}", closer_peers.as_ref().unwrap().len(), &key_fmt(&args.key));
    }

    info!("Updated Server:\n{}", &state);
    info!("{}: Finished a store request from {}", &state.my_addr, &args.from);

    // build response
    response.key = Some(args.key.clone());
    match state.map.get(&args.key) {
        Some(values) => {
            response.vals = Some(values.deref().clone());
        },
        None => {
            response.vals = None;
        }
    }
    response.peers = closer_peers;
    Json(response)
}


#[derive(Debug, Deserialize, Serialize)]
pub struct FindNodeArgs {
    pub from: Peer,
    pub node_id: Key,
    pub sig: (),  
}

/// Find a node by it's ID
/// Iterative, so just returns a list of closer peers.
pub fn find_node((req, args): (HttpRequest<S4hState>, Json<FindNodeArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a find node request from {} for node_id: {}", &state.my_addr, &args.from, &key_fmt(&args.node_id));
    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid find_node request from peer: {}", &args.from);
        return Json(response);
    }

    info!("Updated Server:\n{}", &state);

    let closest_peers = state.peer_info.closest_k_peers(args.node_id.clone());
    info!("{}: Finished a find node request from {} for node_id: {}", &state.my_addr, &args.from, &key_fmt(&args.node_id));
    response.key = Some(args.node_id.clone());
    response.peers = closest_peers;
    info!("Actually done find_node now...");
    Json(response)
}


#[derive(Debug, Deserialize, Serialize)]
pub struct FindValueArgs {
    pub from: Peer,
    pub key: Key,
    pub sig: (),  
}

/// Find a value by it's key
fn find_value((req, args): (HttpRequest<S4hState>, Json<FindValueArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a find value request from {} for key: {}", &state.my_addr, &args.from, &key_fmt(&args.key));
    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid find_value request from peer: {}", &args.from);
        return Json(response);
    }

    response.key = Some(args.key.clone());

    // Lookup values
    match state.map.get(&args.key) {
        Some(values) => {
            info!("Found key: {} in store", &key_fmt(&args.key));
            response.vals = Some(values.deref().clone());
        },
        None => {
            info!("Didn't find key: {} in store", &key_fmt(&args.key));
            response.vals = None;
        }
    }

    info!("Updated Server:\n{}", &state);

    let closest_peers = state.peer_info.closest_k_peers(args.key.clone());
    response.peers = closest_peers;
    Json(response)
}


#[derive(Debug, Deserialize, Serialize)]
pub struct QueryComplaintsArgs {
    pub from: Peer,
    pub key: Key,
    pub sig: (),
}

pub fn query_complaints((req, args): (HttpRequest<S4hState>, Json<QueryComplaintsArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a query_complaints request from {} for node_id: {}", &state.my_addr, &args.from, &key_fmt(&args.key));

    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid query_complaints request from peer: {}", &args.from);
        return Json(response);
    }

    response.key = Some(args.key.clone());
    response.complaints = None;
    
    if let Some(complaints_guard) = self.complaints.get(&args.key) {
        let complaints = complaints_guard.deref().clone();
        response.complaints = Some(complaints);
    }

    Json(response)
}


#[derive(Debug, Deserialize, Serialize)]
pub struct StoreComplaintAgainstArgs {
    pub from: Peer,
    pub against: Peer,
    pub sig: (),
}

/// should be called at a node close to the inverse of against.id
pub fn store_complaint_against((req, args): (HttpRequest<S4hState>, Json<StoreComplaintAgainstArgs)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a store_complaint_against request from {} against node_id: {}", &state.my_addr, &args.from, &key_fmt(&args.against.id));

    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid file_complaint request from peer: {}", &args.from);
        return Json(response);
    }

    let store_by = state.store_complaint_by(args.from.clone(), args.sig, args.against.clone());
    
    if !state.complaints.contains_key(&args.against.id) {
        state.complaints.insert(args.against.id.clone(), (HashSet::<Key>::new(), HashSet::<Key>::new()));
    }
    // store from.id in set of complaints against 'against'
    if let Some(mut complaint_guard) = self.complaints.get_mut(&args.against.id) {
        let complaint = complaint_guard.deref_mut();
        complaint.0.insert(args.from.id.clone());
    }

    if !state.complaints.contains_key(&args.from.id) {
        state.complaints.insert(args.from.id.clone(), (HashSet::<Key>::new(), HashSet::<Key>::new()));
    }
    // store 'against' in set of complaints by from.id
    if let Some(mut complaint_guard) = self.complaints.get_mut(&args.from.id) {
        let complaint = complaint_guard.deref_mut();
        complaint.1.insert(args.against.id.clone());
    }

    response.key = Some(args.against.id.clone());
    response.complaints = None;
    
    if let Some(complaints_guard) = self.complaints.get(&args.against.id) {
        let complaints = complaints_guard.deref().clone();
        response.complaints = Some(complaints);
    }

    Json(response)
}


#[derive(Debug, Deserialize, Serialize)]
pub struct StoreComplaintByArgs {
    pub from: Peer,
    pub by: Peer,
    pub sig_by: (),
    pub against: Peer,
    pub sig: (),
}

pub fn store_complaint_by((req, args): (HttpRequest<S4hState>, Json<StoreComplaintByArgs)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a store_complaint_by request from {}, by: {}, against node_id: {}", &state.my_addr, &args.from, &key_fmt(&args.by.id), &key_fmt(&args.against.id));
    // should be called at a node close to the inverse of by.id

    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid file_complaint request from peer: {}", &args.from);
        return Json(response);
    }

    // TODO verify that by signed this complaint against `against`

    if !state.complaints.contains_key(&args.against.id) {
        state.complaints.insert(args.against.id.clone(), (HashSet::<Key>::new(), HashSet::<Key>::new()));
    }
    // store by.id in set of complaints against 'against'
    if let Some(mut complaint_guard) = state.complaints.get_mut(&args.against.id) {
        let complaint = complaint_guard.deref_mut();
        complaint.0.insert(args.by.id.clone());
    }

    if !state.complaints.contains_key(&by.id) {
        state.complaints.insert(by.id.clone(), (HashSet::<Key>::new(), HashSet::<Key>::new()));
    }
    // store 'against' in set of complaints by by.id
    if let Some(mut complaint_guard) = self.complaints.get_mut(&by.id) {
        let complaint = complaint_guard.deref_mut();
        complaint.1.insert(against.id.clone());
    }


    response.key = Some(by.id.clone());
    response.complaints = None;
    
    if let Some(complaints_guard) = self.complaints.get(&by.id) {
        let complaints = complaints_guard.deref().clone();
        response.complaints = Some(complaints);
    }

    future::ready(response)
}    

