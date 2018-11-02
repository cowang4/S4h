#![feature(futures_api, pin, arbitrary_self_types, await_macro, async_await, proc_macro_hygiene, custom_attribute)]

mod hash;
mod key;
mod peer_info;
mod rpc;

use std::env;
use std::net::SocketAddr;

use bincode_transport;
use dotenv;
use failure::{Error};
use futures::{
    compat::{TokioDefaultSpawner},
    future::{FutureExt, TryFutureExt},
    executor::ThreadPool,
};
use log::{error, info};
use tarpc::{
    client, context,
    server::{self, Handler},
};
use tokio;

use crate::{hash::hash, key::{key_fmt}, rpc::{S4hServer, new_stub, serve}};


async fn run(spawner: ThreadPool, is_client: bool, server_addr: SocketAddr, client_addr: SocketAddr) -> Result<(), Error> {
    // server init code
    let s4h_server = S4hServer::new(&server_addr, spawner);
    let my_peer = s4h_server.get_my_peer();
    let server_transport = bincode_transport::listen(&server_addr)?;
    let server = server::new(server::Config::default())
        .incoming(server_transport)
        .respond_with(serve(s4h_server.clone()));

    info!("Running server on {} with id {} ...", &my_peer.addr, key_fmt(&my_peer.id));


    if is_client {
        tokio::spawn(server.unit_error().boxed().compat());

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
    else {
        await!(server);
    }

    Ok(())
}

fn main() {

    // TODO overhaul error handling throughout whole project. Any function that can fail shoud
    // return a Result.
    // TODO work on encapsulation. Make struct members private and helper functions
    // TODO use Arc::clone(x) instead of x.clone()
    // TODO shouldn't use systemclock for timeout, since no clock sync
    // TODO fix when a client drops, but client obj thinks it's connected.

    dotenv::dotenv().expect("dotenv");
    env_logger::init();
    
    let is_client: bool = env::args().nth(1).unwrap_or("server".to_string()).eq(&"client".to_string());

    let listen_addr: SocketAddr = {
        let addr = env::args().nth(2);
        let addr = match &addr {
            None => "127.0.0.1:10234",
            Some(addr) => addr.as_str(),
        };
        addr.parse().expect("Invalid listen addr")
    };

    let peer_addr: SocketAddr = {
        let addr = env::args().nth(3);
        let addr = match &addr {
            None => "127.0.0.1:10235",
            Some(addr) => addr.as_str(),
        };
        addr.parse().expect("Invalid client addr")
    };

    let thread_pool = ThreadPool::new().expect("Create ThreadPool");
    tarpc::init(TokioDefaultSpawner);
    tokio::run(
        run(thread_pool, is_client, listen_addr, peer_addr)
            .map_err(|e| error!("ERROR: {}", e))
            .boxed()
            .compat()
        );
}
