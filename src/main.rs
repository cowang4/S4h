#![feature(futures_api, pin, arbitrary_self_types, await_macro, async_await, proc_macro_hygiene, custom_attribute)]

mod hash;
mod key;
mod peer_info;
mod rpc;

use std::env;
use std::io;
use std::net::SocketAddr;

use bincode_transport;
use dotenv;
use failure::{Error};
use futures::{
    compat::{TokioDefaultSpawner},
    future::{FutureExt, TryFutureExt},
    executor::ThreadPool,
};
use log::{error, warn, info};
use tarpc::{
    client, context,
    server::{self, Handler},
};
use tokio;

use crate::{
    hash::hash,
    key::{key_fmt},
    rpc::{S4hServer, new_stub, serve},
};


fn create_peer(spawner: ThreadPool, addr: SocketAddr) -> Result<S4hServer, Error> {
    let s4h_server = S4hServer::new(&addr, spawner);
    let my_peer = s4h_server.get_my_peer();
    let server_transport = bincode_transport::listen(&addr)?;
    let server = server::new(server::Config::default())
        .incoming(server_transport)
        .respond_with(serve(s4h_server.clone()));

    info!("Running server on {} with id {} ...", &my_peer.addr, key_fmt(&my_peer.id));
    tokio::spawn(server.unit_error().boxed().compat());

    Ok(s4h_server)
}


async fn old_test(spawner: ThreadPool, is_client: bool, server_addr: SocketAddr, client_addr: SocketAddr) -> Result<(), Error> {
    let s4h_server = create_peer(spawner.clone(), server_addr)?;
    let my_peer = s4h_server.get_my_peer();

    if is_client {

        // client init code
        let client_transport = await!(bincode_transport::connect(&client_addr))?;
        let mut client = await!(new_stub(client::Config::default(), client_transport))?;

        // client test example
        let ping_resp = await!(client.ping(context::current(), my_peer.clone(), ()))?;
        info!("Ping response: {}", ping_resp);

        // testing client.clone(). It works!
        let mut client2 = client.clone();
        let ping_resp2 = await!(client2.ping(context::current(), my_peer.clone(), ()))?;
        info!("Ping response: {}", ping_resp2);

        let hello = "Hello, World!".as_bytes();
        let hello_hash = hash(hello);
        let hello_hash2 = hello_hash.clone();
        let store_resp = await!(client.store(context::current(), my_peer.clone(), (), hello_hash, "ipfs://foobar".into()))?;
        info!("Store response: {}", store_resp);
        let find_val_resp = await!(client.find_value(context::current(), my_peer.clone(), (), hello_hash2))?;
        info!("Find_val response: {}", find_val_resp);
    }

    Ok(())
}

async fn command_line_shell(spawner: ThreadPool, addr: SocketAddr) -> Result<(), Error> {
    let s4h_server: S4hServer = create_peer(spawner.clone(), addr)?;
    loop {
        println!("$ ");
        let mut input = String::new();
	io::stdin().read_line(&mut input)?;
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
                let addr: SocketAddr = words[1].parse()?;
                await!(s4h_server.add_peer_by_addr(addr));
            },
            "store" | "insert" => {
                if words.len() != 3 {
                    error!("Usage {} key value", words[0]);
                    continue;
                }
                let key = hash(words[1].as_bytes());
                let val = words[2].to_string();
                await!(s4h_server.store(key, val));
            },
            "find" | "lookup" | "find_val" | "find_value" => {
                if words.len() != 2 {
                    error!("Usage: {} key", words[0]);
                    continue;
                }
                let key = hash(words[1].as_bytes());
                let vals = await!(s4h_server.find_value(key));
                println!("{:?}", vals);
            },
            w => {
                warn!("Unknown command {}", w);
            },
        }

    }
    
}


fn main() {

    // TODO overhaul error handling throughout whole project. Any function that can fail shoud
    // return a Result.
    // TODO work on encapsulation. Make struct members private and helper functions
    // TODO use Arc::clone(x) instead of x.clone()
    // TODO fix when a client drops, but client obj thinks it's connected.
    // TODO tarpc shouldn't use systemclock for timeout, since no clock sync

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

    let thread_pool = ThreadPool::new().expect("Create ThreadPool");
    tarpc::init(TokioDefaultSpawner);
    tokio::run(
        command_line_shell(thread_pool, listen_addr)
            .map_err(|e| error!("ERROR: {}", e))
            .boxed()
            .compat()
        );
}
