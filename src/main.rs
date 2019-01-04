
mod client;
mod hash;
mod key;
mod peer_info;
mod reputation;
mod server;
mod state;

use std::env;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::{
    atomic::{self, AtomicBool},
    Arc,
};
use std::thread;
use std::time::{Duration};

use actix::prelude::*;
use actix_web;
use dotenv;
use log::{error, warn, info, debug};
use futures;

use crate::{
    hash::hash,
    key::{key_fmt, key_new},
    server::{create_app},
    state::{S4hState},
};


fn command_line_shell(s4h_state: S4hState, done: Arc<AtomicBool>) {
    loop {
        print!("$ ");
        io::stdout().flush().expect("flush stdout");
        let mut input = String::new();
	io::stdin().read_line(&mut input).expect("read line from stdin");
	input = input.trim().to_string();

        let words: Vec<&str> = input.split(" ").collect();
        if words.len() < 1 {
            continue;
        }
        match words[0] {
            "connect" => {
                if words.len() != 2 {
                    error!("Usage: connect addr");
                    continue;
                }
                let addr: SocketAddr = words[1].parse().expect("valid addr");
                s4h_state.add_peer_by_addr(addr);
            },
            "store" | "insert" => {
                if words.len() != 3 {
                    error!("Usage {} key value", words[0]);
                    continue;
                }
                let key = hash(words[1].as_bytes());
                let val = words[2].to_string();
                s4h_state.store(key, val);
            },
            "find" | "lookup" | "find_val" | "find_value" => {
                if words.len() != 2 {
                    error!("Usage: {} key", words[0]);
                    continue;
                }
                let key = hash(words[1].as_bytes());
                let vals = s4h_state.find_value(key);
                println!("{:?}", vals);
            },
            "complain" => {
                if words.len() != 2 {
                    error!("Usage: {} against_node_id", words[0]);
                    continue;
                }
                let against = key_new(words[1].to_string()).expect("invalid hex key");
                s4h_state.store_complaint_against(against);
            },
            "reputation" => {
                if words.len() != 2 {
                    error!("Usage: {} node_id", words[0]);
                    continue;
                }
                let node_id = key_new(words[1].to_string()).expect("invalid hex node_id");
                match s4h_state.explore_trust_simple(&node_id) {
                    1   => println!("{} is trustworthy", key_fmt(&node_id)),
                    0   => println!("{} is neutral", key_fmt(&node_id)),
                    -1  => println!("{} is untrustworthy", key_fmt(&node_id)),
                    _   => error!("{} has an unknown trust score", key_fmt(&node_id)),
                }
            }
            "print" => {
                println!("{}", &s4h_state);
            },
            "exit" | "quit" | "stop" => {
                done.store(true, atomic::Ordering::SeqCst);
                break;
            },
            w => {
                warn!("Unknown command {}", w);
            },
        }

    }
    info!("Stopping s4h");
}


fn create_peer(addr: SocketAddr, done: Arc<AtomicBool>) -> S4hState {
    // futures could potentially not like this solution because they want 'static, so no futures
    let mut s4h_state = S4hState::new();
    s4h_state.my_addr = addr;
    let s4h_state2 = s4h_state.clone();
    let s4h_state3 = s4h_state.clone();
    let s4h_state4 = s4h_state.clone();

    thread::spawn(move || {
        let http_server = actix_web::server::new(move || {
            create_app(s4h_state.clone()).finish()
        })
        .bind(addr)
        .expect("bind server");

        System::run(move || {
            Arbiter::spawn_fn(move || {
                thread::spawn(move || { command_line_shell(s4h_state2.clone(), done); });

                let my_peer = s4h_state3.get_my_peer();
                info!("Running server on {} with id {} ...", &my_peer.addr, key_fmt(&my_peer.id));

                http_server.start();

                futures::future::ok(())
            })
        });
    });

    debug!("Returning from create_peer.");
    s4h_state4
}


fn main() {

    // TODO overhaul error handling throughout whole project. Any function that can fail shoud
    // return a Result.
    // TODO work on encapsulation. Make struct members private and helper functions
    // TODO use Arc::clone(x) instead of x.clone()
    // TODO fix the kbucket api, it is prone to creating deadlocks and can
    // be dangerous because in between a contains check and some get or modify,
    // the peer could be deleted.
    // TODO make sure that everywhere that adds a peer is properly validating, with a ping
    // TODO the CHashMap accesses have some subtle race conditions too, with contains
    // TODO use new futures::future::join_all to run async queries simultaneouslly

    dotenv::dotenv().expect("dotenv");
    env_logger::init();
    
    let listen_addr: SocketAddr = {
        let addr = env::args().nth(1);
        let addr = match &addr {
            None => "127.0.0.1:10234",
            Some(addr) => addr.as_str(),
        };
        addr.parse().expect("Invalid listen addr")
    };

    let done = Arc::new(AtomicBool::new(false));
    let done2 = Arc::clone(&done);

    create_peer(listen_addr, done2);

    while !done.load(atomic::Ordering::SeqCst) { thread::sleep(Duration::from_secs(1)); }
}


#[cfg(test)]
mod tests {
    use super::*;
    use failure::{Fail};

    fn basic_rpc_test(addr: SocketAddr, addr2: SocketAddr) -> Result<(), reqwest::Error> {
        let done = Arc::new(AtomicBool::new(false));
        let done2 = Arc::clone(&done);

        let s4h_server = create_peer(addr.clone(), done);
        let my_peer = s4h_server.get_my_peer();

        let s4h_server2 = create_peer(addr2.clone(), done2);
        let my_peer2 = s4h_server2.get_my_peer();

        // client test example
        let ping_resp = client::ping(my_peer.addr, my_peer2.clone()).expect("first ping");
        info!("Ping response: {}", &ping_resp);
        assert_eq!(ping_resp.from.id, my_peer.id);
        assert_eq!(ping_resp.from.addr, my_peer.addr);

        let ping_resp2 = client::ping(my_peer.addr, my_peer2.clone())?;
        info!("Ping response: {}", &ping_resp2);
        assert_eq!(ping_resp2.from.id, my_peer.id);
        assert_eq!(ping_resp2.from.addr, my_peer.addr);

        let hello = "Hello, World!".as_bytes();
        let hello_hash = hash(hello);
        let hello_hash2 = hello_hash.clone();
        let hello_hash3 = hello_hash.clone();
        let store_resp = client::store(my_peer.addr, my_peer2.clone(), hello_hash, "ipfs://foobar".into())?;
        info!("Store response: {}", &store_resp);
        assert_eq!(store_resp.from.id, my_peer.id);
        assert_eq!(store_resp.from.addr, my_peer.addr);
        assert_eq!(store_resp.key, Some(hello_hash2.clone()));

        let find_val_resp = client::find_value(my_peer.addr, my_peer2.clone(), hello_hash2)?;
        info!("Find_val response: {}", &find_val_resp);
        assert_eq!(find_val_resp.from.id, my_peer.id);
        assert_eq!(find_val_resp.from.addr, my_peer.addr);
        assert_eq!(find_val_resp.key, Some(hello_hash3));
        assert_eq!(find_val_resp.vals, Some(vec!["ipfs://foobar".to_string()]));

        Ok(())
    }

    // This test isn't deterministic, because it generates node_ids randomly.
    #[test]
    fn basic_rpc_test_runner() {
        dotenv::dotenv().expect("dotenv");
        let _ = env_logger::try_init();

        let addr: SocketAddr = "127.0.0.1:10234".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:10235".parse().unwrap();

        match basic_rpc_test(addr, addr2) {
            Err(e) => error!("{}: {:?}", e, e.backtrace()),
            Ok(_) => {},
        }
    }


    fn basic_api_test(addr: SocketAddr, addr2: SocketAddr) -> Result<(), reqwest::Error> {
        let done = Arc::new(AtomicBool::new(false));
        let done2 = Arc::clone(&done);

        let s4h_server = create_peer(addr.clone(), done);

        let s4h_server2 = create_peer(addr2.clone(), done2);

        debug!("Adding peer2 to peer1's kbuckets");
        s4h_server.add_peer_by_addr(addr2.clone());

        debug!("Adding peer1 to peer2's kbuckets");
        s4h_server2.add_peer_by_addr(addr.clone());
        
        let key = hash("Hello, World!".as_bytes());
        let val = "ipfs://foobar".to_string();
        s4h_server.store(key.clone(), val);

        info!("Finished basic_apt_test");
        s4h_server.clear();
        s4h_server2.clear();
        Ok(())
    }

    // This test isn't deterministic, because it generates node_ids randomly.
    #[test]
    fn basic_api_test_runner() {
        dotenv::dotenv().expect("dotenv");
        let _ = env_logger::try_init();

        let addr: SocketAddr = "127.0.0.1:10236".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:10237".parse().unwrap();

        match basic_api_test(addr, addr2) {
           Err(e) => error!("{}: {:?}", e, e.backtrace()),
            Ok(_) => {},
        }
    }


    fn three_peer_test(addr: SocketAddr, addr2: SocketAddr, addr3: SocketAddr) -> Result<(), reqwest::Error> {
        let done = Arc::new(AtomicBool::new(false));
        let done2 = Arc::clone(&done);
        let done3 = Arc::clone(&done);

        let s4h_server = create_peer(addr.clone(), done);
        let s4h_server2 = create_peer(addr2.clone(), done2);
        let s4h_server3 = create_peer(addr3.clone(), done3);

        s4h_server.add_peer_by_addr(addr2.clone());
        s4h_server2.add_peer_by_addr(addr3.clone());

        let key = hash("Hello, World!".as_bytes());
        let val = "ipfs://foobar".to_string();
        s4h_server.store(key.clone(), val);

        let vals = s4h_server.find_value(key.clone());
        assert_eq!(vals, vec!["ipfs://foobar".to_string()]);

        let key2 = hash("Goodbye!".as_bytes());
        let no_vals = s4h_server.find_value(key2);
        assert_eq!(no_vals, Vec::<String>::new());

        s4h_server.clear();
        s4h_server2.clear();
        s4h_server3.clear();
        
        Ok(())
    }

    // This test isn't deterministic, because it generates node_ids randomly.
    #[test]
    fn three_peer_test_runner() {
        dotenv::dotenv().expect("dotenv");
        let _ = env_logger::try_init();

        let addr: SocketAddr = "127.0.0.1:10238".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:10239".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.1:10240".parse().unwrap();

        match three_peer_test(addr, addr2, addr3) {
           Err(e) => error!("{}: {:?}", e, e.backtrace()),
            Ok(_) => {},
        }
    }

}
