

use std::net::SocketAddr;
use std::time::Duration;

use lazy_static::lazy_static;
use reqwest;

use crate::{
    key::{Key},
    server::{PingArgs, StoreArgs, FindNodeArgs, FindValueArgs, QueryComplaintsArgs, StoreComplaintByArgs, StoreComplaintAgainstArgs},
    state::{MessageReturned},
    peer_info::{Peer},
};


lazy_static! {
    static ref CLIENT: reqwest::Client = {
        reqwest::Client::builder()
            .gzip(false)
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client")
    };
}


pub fn ping(to_addr: SocketAddr, from: Peer) -> Result<MessageReturned, reqwest::Error> {
    CLIENT.post(&format!("http://{}/ping", to_addr))
        .json(&PingArgs{ from: from, sig: () })
        .send()?
        .json()
}


pub fn store(to_addr: SocketAddr, from: Peer, key: Key, value: String) -> Result<MessageReturned, reqwest::Error> {
    CLIENT.post(&format!("http://{}/store", to_addr))
        .json(&StoreArgs{ from: from, key: key, value: value, sig: () })
        .send()?
        .json()
}


pub fn find_node(to_addr: SocketAddr, from: Peer, node_id: Key) -> Result<MessageReturned, reqwest::Error> {
    CLIENT.post(&format!("http://{}/find_node", to_addr))
        .json(&FindNodeArgs{ from: from, node_id: node_id, sig: () })
        .send()?
        .json()
}


pub fn find_value(to_addr: SocketAddr, from: Peer, key: Key) -> Result<MessageReturned, reqwest::Error> {
    CLIENT.post(&format!("http://{}/find_value", to_addr))
        .json(&FindValueArgs{ from: from, key: key, sig: () })
        .send()?
        .json()
}


pub fn query_complaints(to_addr: SocketAddr, from: Peer, key: Key) -> Result<MessageReturned, reqwest::Error> {
    CLIENT.post(&format!("http://{}/query_complaints", to_addr))
        .json(&QueryComplaintsArgs{ from: from, key: key, sig: () })
        .send()?
        .json()
}


pub fn store_complaint_by(to_addr: SocketAddr, from: Peer, by: Peer, sig_by: (), against: Peer) -> Result<MessageReturned, reqwest::Error> {
    CLIENT.post(&format!("http://{}/store_complaint_by", to_addr))
        .json(&StoreComplaintByArgs{ from: from, by: by, sig_by: sig_by, against: against, sig: () })
        .send()?
        .json()
}


pub fn store_complaint_against(to_addr: SocketAddr, from: Peer, against: Peer) -> Result<MessageReturned, reqwest::Error> {
    CLIENT.post(&format!("http://{}/store_complaint_against", to_addr))
        .json(&StoreComplaintAgainstArgs{ from: from, against: against, sig: () })
        .send()?
        .json()
}
