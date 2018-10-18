#![feature(futures_api, pin, arbitrary_self_types, await_macro, async_await, proc_macro_hygiene, extern_prelude, custom_attribute)]

mod hash;
mod key;
mod peer_info;
mod rpc;

use std::env;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bincode_transport;
use futures::{
    future::{self, Ready},
    prelude::*,
};
use log::{error, info};
use tarpc::{
    client, context,
    server::{self, Handler, Server},
};
use tokio::timer::Delay;
use tokio_executor;

use crate::{hash::hash, rpc::{S4hServer, Client, new_stub, serve, Service}};


async fn run(is_client: bool, server_addr: SocketAddr, client_addr: SocketAddr) -> io::Result<()> {
    // server init code
    let server_transport = bincode_transport::listen(server_addr)?;
    let server = Server::new(server::Config::default())
        .incoming(server_transport)
        .take(1)
        .repond_with(serve(S4hServer::new()));

    info!("Running server...");
    tokio_executor::spawn(server.unit_error().boxed().compat()).expect("server future to spawn");

    // client init code
    let client_transport = await!(bincode_transport::connect(&client_addr))?;
    let mut client = await!(new_stub(client::Config::default(), client_transport));

    if is_client {
        // client test example
        let hello = "Hello, World!".as_bytes();
        let hello_hash = hash(hello);
        let hello_hash2 = hello_hash.clone();
        let store_resp = await!(client.store(context::current(), hello_hash))?;
        info!("Store response: {}", store_resp);
        let find_val_resp = await!(client.find_value(hello_hash2))?;
        info!("Find_val response: {}", find_val_resp);
    }

    Ok(())
}

fn main() {
    env_logger::init();
    
    let is_client: bool = env::args().nth(1).expect("is_client arg").eq("client".to_string());

    let listen_addr = {
        let mut addr = env::args().nth(2);
        match addr {
            None => "127.0.0.1:10234".to_string(),
            Some(addr) => addr,
        }
    };

    let peer_addr = {
        let mut addr = env::args().nth(3);
        match addr {
            None => "127.0.0.1:10235".to_string(),
            Some(addr) => addr,
        }
    };

    tokio::run(
        run(is_client, listen_addr, peer_addr)
            .map_err(|e| error!("ERROR: {}", e))
            .boxed()
            .compat(),
        );
}
