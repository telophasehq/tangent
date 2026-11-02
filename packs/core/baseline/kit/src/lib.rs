//! What baseline's two plugins agree on: the keys the builder writes in
//! ingest and the renderer reads in the judge, in another process; the event
//! each reads off a record; the time helpers; and the store, as [`Kv`].
//! Nothing else joins them.

pub mod keys;
pub mod kinds;
mod mem;
pub mod time;

pub use mem::Mem;

pub type R<T> = Result<T, String>;

/// A value in the store: baseline writes integers and strings, and reads a
/// value of any other kind as none.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Int(i64),
    Str(String),
}

/// The runtime's state, in baseline's namespace. Every key and value is one
/// of [`keys`]'s.
pub trait Kv {
    fn get(&mut self, key: &str) -> R<Option<Scalar>>;
    fn set(&mut self, key: &str, v: Scalar) -> R<()>;
    fn incr(&mut self, key: &str, by: i64) -> R<()>;
    fn scan(&mut self, lo: &str, hi: &str) -> R<Vec<(String, Scalar)>>;
}
