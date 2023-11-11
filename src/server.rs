

use std::collections::{HashSet};
use std::ops::{Deref, DerefMut};

use actix_web::{App, http, HttpRequest, Json};
use log::{info, warn};
use serde_derive::{Deserialize, Serialize};

use crate::{
    key::{Key},
    state::{S4hState, MessageReturned},
    peer_info::{Peer},
};


pub fn create_app(state: S4hState) -> App<S4hState> {
    App::with_state(state)
        .resource("/ping",          |r| r.method(http::Method::POST).with(ping))
        .resource("/store",         |r| r.method(http::Method::POST).with(store))
        .resource("/find_node",     |r| r.method(http::Method::POST).with(find_node))
        .resource("/find_value",    |r| r.method(http::Method::POST).with(find_value))
        .resource("/query_complaints",          |r| r.method(http::Method::POST).with(query_complaints))
        .resource("/store_complaint_against",   |r| r.method(http::Method::POST).with(store_complaint_against))
        .resource("/store_complaint_by",        |r| r.method(http::Method::POST).with(store_complaint_by))
}


#[derive(Debug, Deserialize, Serialize)]
pub struct PingArgs {
    pub from: Peer,
    pub sig: (),
}

/// Respond if this peer is alive
pub fn ping((req, args): (HttpRequest, Json<PingArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a ping request from {}", &state.my_addr, &args.from);
    let response = MessageReturned::from_peer(state.get_my_peer());
    
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
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
pub fn store((req, args): (HttpRequest, Json<StoreArgs>)) -> Json<MessageReturned> {
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
    state.store_in_hashtable(args.key.clone(), args.value.clone());

    let closer_peers = state.peer_info.closer_k_peers(args.key.clone());
    if closer_peers.is_some() {
        info!("{} closer peers to key: {}", closer_peers.as_ref().unwrap().len(), &args.key);
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
pub fn find_node((req, args): (HttpRequest, Json<FindNodeArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a find node request from {} for node_id: {}", &state.my_addr, &args.from, &args.node_id);
    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid find_node request from peer: {}", &args.from);
        return Json(response);
    }

    info!("Updated Server:\n{}", &state);

    let closest_peers = state.peer_info.closest_k_peers(args.node_id.clone());
    info!("{}: Finished a find node request from {} for node_id: {}", &state.my_addr, &args.from, &args.node_id);
    response.key = Some(args.node_id.clone());
    response.peers = closest_peers;
    Json(response)
}


#[derive(Debug, Deserialize, Serialize)]
pub struct FindValueArgs {
    pub from: Peer,
    pub key: Key,
    pub sig: (),  
}

/// Find a value by it's key
fn find_value((req, args): (HttpRequest, Json<FindValueArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a find value request from {} for key: {}", &state.my_addr, &args.from, &args.key);
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
            info!("Found key: {} in store", &args.key);
            response.vals = Some(values.deref().clone());
        },
        None => {
            info!("Didn't find key: {} in store", &args.key);
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

pub fn query_complaints((req, args): (HttpRequest, Json<QueryComplaintsArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a query_complaints request from {} for node_id: {}", &state.my_addr, &args.from, &args.key);

    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid query_complaints request from peer: {}", &args.from);
        return Json(response);
    }

    response.key = Some(args.key.clone());
    response.complaints = None;
    
    if let Some(complaints_guard) = state.complaints.get(&args.key) {
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
pub fn store_complaint_against((req, args): (HttpRequest, Json<StoreComplaintAgainstArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a store_complaint_against request from {} against node_id: {}", &state.my_addr, &args.from, &args.against.id);

    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid file_complaint request from peer: {}", &args.from);
        return Json(response);
    }

    let _store_by = state.store_complaint_by(args.from.clone(), args.sig, args.against.clone());
    
    if !state.complaints.contains_key(&args.against.id) {
        state.complaints.insert(args.against.id.clone(), (HashSet::<Key>::new(), HashSet::<Key>::new()));
    }
    // store from.id in set of complaints against 'against'
    if let Some(mut complaint_guard) = state.complaints.get_mut(&args.against.id) {
        let complaint = complaint_guard.deref_mut();
        complaint.0.insert(args.from.id.clone());
    }

    if !state.complaints.contains_key(&args.from.id) {
        state.complaints.insert(args.from.id.clone(), (HashSet::<Key>::new(), HashSet::<Key>::new()));
    }
    // store 'against' in set of complaints by from.id
    if let Some(mut complaint_guard) = state.complaints.get_mut(&args.from.id) {
        let complaint = complaint_guard.deref_mut();
        complaint.1.insert(args.against.id.clone());
    }

    response.key = Some(args.against.id.clone());
    response.complaints = None;
    
    if let Some(complaints_guard) = state.complaints.get(&args.against.id) {
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

pub fn store_complaint_by((req, args): (HttpRequest, Json<StoreComplaintByArgs>)) -> Json<MessageReturned> {
    let state = req.state();
    info!("{}: Received a store_complaint_by request from {}, by: {}, against node_id: {}", &state.my_addr, &args.from, &args.by.id, &args.against.id);
    // should be called at a node close to the inverse of by.id

    let mut response = MessageReturned::from_peer(state.get_my_peer());

    // Validate request
    let valid = state.validate_and_update_or_add_peer_with_sig(args.from.clone(), args.sig);
    if !valid {
        warn!("Invalid file_complaint request from peer: {}", &args.from);
        return Json(response);
    }

    // TODO verify that 'by' signed this complaint against `against`

    if !state.complaints.contains_key(&args.against.id) {
        state.complaints.insert(args.against.id.clone(), (HashSet::<Key>::new(), HashSet::<Key>::new()));
    }
    // store by.id in set of complaints against 'against'
    if let Some(mut complaint_guard) = state.complaints.get_mut(&args.against.id) {
        let complaint = complaint_guard.deref_mut();
        complaint.0.insert(args.by.id.clone());
    }

    if !state.complaints.contains_key(&args.by.id) {
        state.complaints.insert(args.by.id.clone(), (HashSet::<Key>::new(), HashSet::<Key>::new()));
    }
    // store 'against' in set of complaints by by.id
    if let Some(mut complaint_guard) = state.complaints.get_mut(&args.by.id) {
        let complaint = complaint_guard.deref_mut();
        complaint.1.insert(args.against.id.clone());
    }


    response.key = Some(args.by.id.clone());
    response.complaints = None;
    
    if let Some(complaints_guard) = state.complaints.get(&args.by.id) {
        let complaints = complaints_guard.deref().clone();
        response.complaints = Some(complaints);
    }

    Json(response)
}    

