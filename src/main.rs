#![feature(futures_api, pin, arbitrary_self_types, await_macro, async_await, proc_macro_hygiene, custom_attribute)]

mod hash;
mod key;
mod peer_info;
mod rpc;

use std::env;
use std::net::SocketAddr;

use bincode_transport;
use dotenv;
use failure::{Error, format_err};
use futures::{
    prelude::*,
    executor::ThreadPool,
    task::{Spawn, SpawnExt},
};
use log::{error, info};
use tarpc::{
    client, context,
    server::{self, Handler, Server},
};

use crate::{hash::hash, rpc::{S4hServer, new_stub, serve, Service}};


async fn run(spawner: ThreadPool, is_client: bool, server_addr: SocketAddr, client_addr: SocketAddr) -> Result<(), Error> {
    let mut spawner2 = spawner.clone();
    // server init code
    let s4h_server = S4hServer::<ThreadPool>::new(&server_addr, spawner);
    let server_transport = bincode_transport::listen(&server_addr)?;
    let server = Server::new(server::Config::default())
        .incoming(server_transport)
        .take(1)
        .respond_with(serve(s4h_server.clone()));

    info!("Running server...");
    spawner2.spawn(server).map_err(|err| format_err!("Spawning server failed: {:?}", err))?;


    if is_client {
        // client init code
        let client_transport = await!(bincode_transport::connect(&client_addr))?;
        let mut client = await!(new_stub(client::Config::default(), client_transport))?;

        // client test example
        let hello = "Hello, World!".as_bytes();
        let hello_hash = hash(hello);
        let hello_hash2 = hello_hash.clone();
        let store_resp = await!(client.store(context::current(), hello_hash, "ipfs://foobar".into()))?;
        info!("Store response: {}", store_resp);
        let find_val_resp = await!(client.find_value(context::current(), hello_hash2))?;
        info!("Find_val response: {}", find_val_resp);
    }

    Ok(())
}

fn main() {

    // TODO overhaul error handling throughout whole project. Any function that can fail shoud
    // return a Result.

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

    let mut thread_pool = ThreadPool::new().expect("Create ThreadPool");
    thread_pool.run(
        run(thread_pool.clone(), is_client, listen_addr, peer_addr)
            .map_err(|e| error!("ERROR: {}", e))
        ).expect("thread pool spawn \"run\" fn");
}
