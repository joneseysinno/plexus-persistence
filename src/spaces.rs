use infinite_db::InfiniteDb;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::infinitedb_core::address::SpaceId;

use crate::error::PersistenceError;

/// Space for serialized [`Atom`](frp_domain::atom::Atom) records.
pub const SPACE_ATOMS: SpaceId = SpaceId(1);

/// Space for serialized [`Block`](frp_domain::block::Block) records.
pub const SPACE_BLOCKS: SpaceId = SpaceId(2);

/// Space for serialized [`HyperEdge`](frp_domain::edge::HyperEdge) records.
pub const SPACE_EDGES: SpaceId = SpaceId(3);

/// Register all three frp spaces with `db`.
///
/// Safe to call on an already-opened database — duplicate-name and
/// duplicate-id errors from `infinite-db` are silently ignored so that
/// `open` is idempotent across process restarts.
pub fn register_spaces(db: &mut InfiniteDb) -> Result<(), PersistenceError> {
    let configs = [
        SpaceConfig { id: SPACE_ATOMS,  name: "plexus_atoms".into(),  dims: 1 },
        SpaceConfig { id: SPACE_BLOCKS, name: "plexus_blocks".into(), dims: 1 },
        SpaceConfig { id: SPACE_EDGES,  name: "plexus_edges".into(),  dims: 1 },
    ];

    for config in configs {
        match db.register_space(config) {
            Ok(()) => {}
            // Space already registered from a previous open — fine.
            Err(e) if e.contains("Duplicate") => {}
            Err(e) => return Err(PersistenceError::Db(e)),
        }
    }

    Ok(())
}
