//! `frp-persistence` — durable storage backend for infinite-db frp.
//!
//! Implements the [`AtomStore`], [`BlockStore`], and [`EdgeStore`] traits from
//! `frp-loom` using [`infinite-db`](https://crates.io/crates/infinite-db)
//! as the on-disk storage engine.
//!
//! # Quick-start
//!
//! ```rust,no_run
//! use frp_persistence::InfiniteDbStore;
//! use frp_loom::store::AtomStore;
//!
//! let mut store = InfiniteDbStore::open("./data").unwrap();
//! // put / get / delete atoms, blocks, edges through the store traits…
//! store.flush().unwrap();
//! ```
//!
//! # Design notes
//!
//! * **Write-through cache** — the store traits return `&Self::Atom` borrowed
//!   references, so deserialized values must live inside the struct. All reads
//!   are served from in-memory `HashMap`s; writes go to both the cache and the
//!   WAL.
//! * **`EdgeTransform::Inline` closures** are not serializable and round-trip
//!   as `PassThrough`. Re-attach closures after loading a graph from the store.
//! * Call [`InfiniteDbStore::flush`] before process exit to seal WAL records
//!   into on-disk blocks.

pub mod error;
pub mod spaces;
pub mod store;

pub use error::PersistenceError;
pub use store::InfiniteDbStore;
