use infinite_db::InfiniteDb;
use infinite_db::infinitedb_core::address::SpaceId;
use infinite_db::infinitedb_core::space::SpaceConfig;

use crate::error::PersistenceError;

/// Space for serialized [`Atom`](frp_domain::atom::Atom) records.
pub const SPACE_ATOMS: SpaceId = SpaceId(1);

/// Space for serialized [`Block`](frp_domain::block::Block) records.
pub const SPACE_BLOCKS: SpaceId = SpaceId(2);

/// Space for serialized [`HyperEdge`](frp_domain::edge::HyperEdge) records.
pub const SPACE_EDGES: SpaceId = SpaceId(3);

const DIMS_1D: usize = 1;

const NAME_ATOMS: &str = "plexus_atoms";
const NAME_BLOCKS: &str = "plexus_blocks";
const NAME_EDGES: &str = "plexus_edges";

fn frp_space_configs() -> [SpaceConfig; 3] {
    [
        SpaceConfig { id: SPACE_ATOMS, name: NAME_ATOMS.into(), dims: DIMS_1D },
        SpaceConfig { id: SPACE_BLOCKS, name: NAME_BLOCKS.into(), dims: DIMS_1D },
        SpaceConfig { id: SPACE_EDGES, name: NAME_EDGES.into(), dims: DIMS_1D },
    ]
}

fn validate_space_dims(name: &str, dims: usize) -> Result<(), PersistenceError> {
    if dims != DIMS_1D {
        return Err(PersistenceError::Db(format!(
            "unsupported FRP space contract for '{name}': expected dims={DIMS_1D}, got {dims}"
        )));
    }
    Ok(())
}

/// Register all three frp spaces with `db`.
///
/// Space contract:
/// - fixed IDs: 1, 2, 3
/// - fixed names: `plexus_atoms`, `plexus_blocks`, `plexus_edges`
/// - fixed dimensionality: 1D key-space for each entity kind
///
/// If a space already exists, only explicit duplicate-id or duplicate-name
/// responses are considered idempotent; all other failures are treated as
/// schema incompatibility and fail fast.
pub fn register_spaces(db: &mut InfiniteDb) -> Result<(), PersistenceError> {
    for config in frp_space_configs() {
        let name = config.name.clone();
        let dims = config.dims;
        match db.register_space(config) {
            Ok(()) => {}
            Err(e) => {
                let is_duplicate_id = e.contains("DuplicateId");
                let is_duplicate_name = e.contains("DuplicateName");
                if !(is_duplicate_id || is_duplicate_name) {
                    return Err(PersistenceError::Db(format!(
                        "space contract rejected for FRP persistence schema: {e}"
                    )));
                }
            }
        }
        validate_space_dims(&name, dims)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn register_spaces_is_idempotent() {
        let dir = TempDir::new().expect("temp dir");
        let mut db = InfiniteDb::open(dir.path()).expect("open db");
        register_spaces(&mut db).expect("first registration");
        register_spaces(&mut db).expect("second registration");
    }
}
