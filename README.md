
# S4h: A P2P Search Engine with Explicit Trust

This repository holds the code and tests of a basic implementation of the
search engine described in my Master's Thesis with the same title.

To run this project, you'll need to install Rust (using
[rustup](https://rustup.rs), then in the repository run `cargo test` to run the
included tests.

To read the code (as of version 0.2.0), I recommend reading it from the top down, starting from
`src/main.rs`, then onto `src/server.rs` and `src/state.rs`. Then the next
level of abstraction of state is in `src/peer_info.rs`. Finally, some auxiliary
functions and structures are in `src/client.rs`, `src/key.rs`, `src/hash.rs`, and
`src/reputation.rs`. As of the time of writing this, the code is unfortunately
very poorly commented. Some insight on the design is available in the thesis.
