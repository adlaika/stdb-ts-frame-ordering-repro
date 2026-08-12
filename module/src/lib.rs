// Minimal module for the TypeScript SDK frame-ordering repro.
//
// One public table with a primary key, and one reducer that delete+inserts that
// key -- the same shape as any row-update in a real app. `padding` exists only
// to control the SIZE of the resulting subscription frame, which is what drives
// decompression time on the client and therefore the race being demonstrated.
use spacetimedb::{reducer, table, ReducerContext, Table};

#[table(accessor = cell, public)]
pub struct Cell {
    #[primary_key]
    pub id: String,
    pub value: i64,
    pub padding: String,
}

/// Delete-then-insert on the primary key. Deltas of this shape do NOT commute:
/// applying two of them out of order leaves the row on the earlier value.
#[reducer]
pub fn write_cell(ctx: &ReducerContext, id: String, value: i64, padding: String) {
    if ctx.db.cell().id().find(&id).is_some() {
        ctx.db.cell().id().delete(&id);
    }
    ctx.db.cell().insert(Cell { id, value, padding });
}
