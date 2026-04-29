use std::collections::HashMap;
use std::path::Path;

use infinite_db::InfiniteDb;
use infinite_db::infinitedb_core::address::DimensionVector;

use frp_loom::error::StoreError;
use frp_loom::query::{Query, QueryResult};
use frp_loom::store::{AtomStore, BlockStore, EdgeStore};
use frp_domain::atom::Atom;
use frp_domain::block::Block;
use frp_domain::edge::HyperEdge;
use frp_plexus::{AtomId, BlockId, EdgeId};

use crate::error::PersistenceError;
use crate::spaces::{register_spaces, SPACE_ATOMS, SPACE_BLOCKS, SPACE_EDGES};

// ---------------------------------------------------------------------------
// InfiniteDbStore
// ---------------------------------------------------------------------------

/// A durable store for frp graph entities backed by [`InfiniteDb`].
///
/// Uses a **write-through cache**: all reads are served from in-memory
/// `HashMap`s (required because the store traits return borrowed `&Self::*`
/// references). Writes go to both the in-memory cache and the database WAL.
///
/// Call [`flush`](Self::flush) before process exit to seal buffered WAL
/// records into on-disk blocks.
pub struct InfiniteDbStore {
    db:     InfiniteDb,
    atoms:  HashMap<AtomId,  Atom>,
    blocks: HashMap<BlockId, Block>,
    edges:  HashMap<EdgeId,  HyperEdge>,
}

impl InfiniteDbStore {
    /// Open (or create) the database at `dir`, register all frp spaces,
    /// and warm the in-memory caches by replaying every live record.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let mut db = InfiniteDb::open(dir)
            .map_err(|e| StoreError::Io(format!("failed to open infinite-db: {e}")))?;

        register_spaces(&mut db).map_err(StoreError::from)?;

        let mut store = Self {
            db,
            atoms:  HashMap::new(),
            blocks: HashMap::new(),
            edges:  HashMap::new(),
        };

        store.warm_cache()?;
        Ok(store)
    }

    /// Seal all buffered WAL records into on-disk blocks.
    ///
    /// Call this before process exit to ensure durability.
    pub fn flush(&mut self) -> Result<(), StoreError> {
        for space in [SPACE_ATOMS, SPACE_BLOCKS, SPACE_EDGES] {
            self.db
                .flush(space)
                .map_err(|e| StoreError::Io(format!("flush failed: {e}")))?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Deserialise all live records from `infinite-db` into the caches.
    fn warm_cache(&mut self) -> Result<(), StoreError> {
        // Atoms
        let records = self.db
            .query(SPACE_ATOMS, None)
            .map_err(|e| StoreError::Io(format!("cache warm (atoms): {e}")))?;
        for rec in records {
            let atom: Atom = serde_json::from_slice(&rec.data)
                .map_err(|e| StoreError::Io(format!("deserialize atom: {e}")))?;
            self.atoms.insert(atom.id, atom);
        }

        // Blocks
        let records = self.db
            .query(SPACE_BLOCKS, None)
            .map_err(|e| StoreError::Io(format!("cache warm (blocks): {e}")))?;
        for rec in records {
            let block: Block = serde_json::from_slice(&rec.data)
                .map_err(|e| StoreError::Io(format!("deserialize block: {e}")))?;
            self.blocks.insert(block.id, block);
        }

        // Edges
        let records = self.db
            .query(SPACE_EDGES, None)
            .map_err(|e| StoreError::Io(format!("cache warm (edges): {e}")))?;
        for rec in records {
            let edge: HyperEdge = serde_json::from_slice(&rec.data)
                .map_err(|e| StoreError::Io(format!("deserialize edge: {e}")))?;
            self.edges.insert(edge.id, edge);
        }

        Ok(())
    }

    #[inline]
    fn id_point(kind: &str, raw: u64) -> Result<DimensionVector, StoreError> {
        let id = u32::try_from(raw).map_err(|_| {
            StoreError::Io(format!(
                "{kind} id {raw} exceeds 1D coordinate limit (u32); refusing lossy u64->u32 mapping"
            ))
        })?;
        Ok(DimensionVector::new(vec![id]))
    }
}

// ---------------------------------------------------------------------------
// AtomStore
// ---------------------------------------------------------------------------

impl AtomStore for InfiniteDbStore {
    type Atom = Atom;

    fn get_atom(&self, id: AtomId) -> Result<&Atom, StoreError> {
        self.atoms.get(&id).ok_or_else(|| StoreError::not_found(id.value()))
    }

    fn put_atom(&mut self, atom: Atom) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(&atom)
            .map_err(|e| StoreError::Io(PersistenceError::Serialize(e.to_string()).to_string()))?;
        let point = Self::id_point("atom", atom.id.value())?;
        self.db
            .insert(SPACE_ATOMS, point, bytes)
            .map_err(|e| StoreError::Io(format!("db insert atom: {e}")))?;
        self.atoms.insert(atom.id, atom);
        Ok(())
    }

    fn delete_atom(&mut self, id: AtomId) -> Result<(), StoreError> {
        if !self.atoms.contains_key(&id) {
            return Err(StoreError::not_found(id.value()));
        }
        let point = Self::id_point("atom", id.value())?;
        self.db
            .delete(SPACE_ATOMS, point)
            .map_err(|e| StoreError::Io(format!("db delete atom: {e}")))?;
        self.atoms.remove(&id);
        Ok(())
    }

    fn query_atoms(&self, query: &Query) -> Result<QueryResult<&Atom>, StoreError> {
        let filtered: Vec<&Atom> = self
            .atoms
            .values()
            .filter(|a| {
                if let Some(k) = &query.kind_filter {
                    if a.kind.to_string() != *k {
                        return false;
                    }
                }
                for tag in &query.tag_filter {
                    if !a.meta.tags.contains(tag) {
                        return false;
                    }
                }
                true
            })
            .collect();

        let total = filtered.len();
        let items = filtered
            .into_iter()
            .skip(query.offset)
            .take(query.limit.unwrap_or(usize::MAX))
            .collect();

        Ok(QueryResult::new(items, total, query.offset))
    }
}

// ---------------------------------------------------------------------------
// BlockStore
// ---------------------------------------------------------------------------

impl BlockStore for InfiniteDbStore {
    type Block = Block;

    fn get_block(&self, id: BlockId) -> Result<&Block, StoreError> {
        self.blocks.get(&id).ok_or_else(|| StoreError::not_found(id.value()))
    }

    fn put_block(&mut self, block: Block) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(&block)
            .map_err(|e| StoreError::Io(PersistenceError::Serialize(e.to_string()).to_string()))?;
        let point = Self::id_point("block", block.id.value())?;
        self.db
            .insert(SPACE_BLOCKS, point, bytes)
            .map_err(|e| StoreError::Io(format!("db insert block: {e}")))?;
        self.blocks.insert(block.id, block);
        Ok(())
    }

    fn delete_block(&mut self, id: BlockId) -> Result<(), StoreError> {
        if !self.blocks.contains_key(&id) {
            return Err(StoreError::not_found(id.value()));
        }
        let point = Self::id_point("block", id.value())?;
        self.db
            .delete(SPACE_BLOCKS, point)
            .map_err(|e| StoreError::Io(format!("db delete block: {e}")))?;
        self.blocks.remove(&id);
        Ok(())
    }

    fn query_blocks(&self, query: &Query) -> Result<QueryResult<&Block>, StoreError> {
        let filtered: Vec<&Block> = self
            .blocks
            .values()
            .filter(|b| {
                // Block has no `kind` string — only tag filtering applies.
                for tag in &query.tag_filter {
                    if !b.meta.labels.contains_key(tag.as_str()) {
                        return false;
                    }
                }
                true
            })
            .collect();

        let total = filtered.len();
        let items = filtered
            .into_iter()
            .skip(query.offset)
            .take(query.limit.unwrap_or(usize::MAX))
            .collect();

        Ok(QueryResult::new(items, total, query.offset))
    }
}

// ---------------------------------------------------------------------------
// EdgeStore
// ---------------------------------------------------------------------------

impl EdgeStore for InfiniteDbStore {
    type Edge = HyperEdge;

    fn get_edge(&self, id: EdgeId) -> Result<&HyperEdge, StoreError> {
        self.edges.get(&id).ok_or_else(|| StoreError::not_found(id.value()))
    }

    fn put_edge(&mut self, edge: HyperEdge) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(&edge)
            .map_err(|e| StoreError::Io(PersistenceError::Serialize(e.to_string()).to_string()))?;
        let point = Self::id_point("edge", edge.id.value())?;
        self.db
            .insert(SPACE_EDGES, point, bytes)
            .map_err(|e| StoreError::Io(format!("db insert edge: {e}")))?;
        self.edges.insert(edge.id, edge);
        Ok(())
    }

    fn delete_edge(&mut self, id: EdgeId) -> Result<(), StoreError> {
        if !self.edges.contains_key(&id) {
            return Err(StoreError::not_found(id.value()));
        }
        let point = Self::id_point("edge", id.value())?;
        self.db
            .delete(SPACE_EDGES, point)
            .map_err(|e| StoreError::Io(format!("db delete edge: {e}")))?;
        self.edges.remove(&id);
        Ok(())
    }

    fn query_edges(&self, query: &Query) -> Result<QueryResult<&HyperEdge>, StoreError> {
        // Edges have no kind/tag metadata to filter on — return all with pagination.
        let all: Vec<&HyperEdge> = self.edges.values().collect();
        let total = all.len();
        let items = all
            .into_iter()
            .skip(query.offset)
            .take(query.limit.unwrap_or(usize::MAX))
            .collect();
        Ok(QueryResult::new(items, total, query.offset))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use frp_domain::{
        Atom, AtomKind, AtomMeta, Block, BlockSchema, EdgeSchedule, EdgeTransform, HyperEdge, Meta,
    };
    use frp_loom::store::{AtomStore, BlockStore, EdgeStore};
    use frp_plexus::{AtomId, BlockId, EdgeId, LayerTag, PortId};

    use super::InfiniteDbStore;

    fn open_tmp_store() -> (TempDir, InfiniteDbStore) {
        let dir = TempDir::new().expect("temp dir");
        let store = InfiniteDbStore::open(dir.path()).expect("open store");
        (dir, store)
    }

    #[test]
    fn round_trip_atom_block_edge_through_store() {
        let (_dir, mut store) = open_tmp_store();

        let atom = Atom::new(
            AtomId::new(7),
            AtomKind::Transform,
            AtomMeta::new("roundtrip", LayerTag::Core),
        );
        let block = Block {
            id: BlockId::new(11),
            schema: BlockSchema::new(vec![], vec![]),
            atoms: vec![atom.id],
            meta: Meta::default(),
        };
        let edge = HyperEdge::new(
            EdgeId::new(13),
            vec![PortId::new(1)],
            vec![PortId::new(2)],
            EdgeTransform::PassThrough,
            EdgeSchedule::OnChange,
        );

        store.put_atom(atom.clone()).expect("put atom");
        store.put_block(block.clone()).expect("put block");
        store.put_edge(edge.clone()).expect("put edge");
        store.flush().expect("flush");

        assert_eq!(store.get_atom(atom.id).expect("get atom").id, atom.id);
        assert_eq!(store.get_block(block.id).expect("get block").id, block.id);
        assert_eq!(store.get_edge(edge.id).expect("get edge").id, edge.id);
    }

    #[test]
    fn put_atom_rejects_u64_id_overflow_for_1d_space() {
        let (_dir, mut store) = open_tmp_store();
        let atom = Atom::new(
            AtomId::new(u64::MAX),
            AtomKind::Transform,
            AtomMeta::new("overflow", LayerTag::Core),
        );

        let err = store.put_atom(atom).expect_err("overflow must fail");
        assert!(err.to_string().contains("exceeds 1D coordinate limit"));
    }
}
