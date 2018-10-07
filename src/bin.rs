
extern crate env_logger;
extern crate futures;
extern crate s4h_lib;
extern crate tarpc;
extern crate tokio;
extern crate url;
extern crate url_serde;

use std::env;
use std::time::{Duration, Instant};

use futures::Future;
use s4h_lib::{hash, rpc::{S4hServer, FutureClient, FutureServiceExt}};
use tarpc::future::{client::{self, ClientExt}, server};
use tokio::timer::Delay;

pub fn main() {
    env_logger::init();
    
    let client_or_server = env::args().nth(1);

    let listen_addr = {
        let mut addr = env::args().nth(2);
        match addr {
            None => "localhost:10234".to_string(),
            Some(addr) => addr,
        }
    };

    let peer_addr = {
        let mut addr = env::args().nth(3);
        match addr {
            None => "localhost:10235".to_string(),
            Some(addr) => addr,
        }
    };

    let mut reactor = tarpc::tokio_core::reactor::Core::new().unwrap();
    let (handle, server) = S4hServer::new().listen(listen_addr.parse().expect("invalid listen addr"),
                                &reactor.handle(),
                                server::Options::default())
                                .unwrap();
    reactor.handle().spawn(server);

    let client_string = "client".to_string();
    if client_string == client_or_server.expect("client or server arg") {

        let hello = "Hello, World!".as_bytes();
        let hello_hash = hash(hello);
        
        let options = client::Options::default().handle(reactor.handle());
        let client = reactor.run(FutureClient::connect(peer_addr.parse().expect("invalid peer addr"), options)).unwrap();

        let client_actions = client.store((hello_hash.clone(), "ipfs://foobar".into()))
                                .and_then(|_| client.find_value(hello_hash))
                                .map(|resp| println!("{:?}", resp))
                                .map_err(tarpc::Error::from);

        reactor.run(client_actions).unwrap();
    }
    else {
        let when = Instant::now() + Duration::from_millis(100000); //100s
        let task = Delay::new(when)
        .map_err(|e| panic!("delay errored; err={:?}", e))
        .and_then(|_| {
            println!("Shutting down server.");
            handle.shutdown().shutdown()
        });
        println!("Running server...");
        reactor.run(task).unwrap();
    }

}
