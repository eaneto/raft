//! Core domain types for the Raft state machine.
//!
//! These newtypes replace bare integers for the three identifiers that run
//! through the paper (Ongaro & Ousterhout, Figure 2): the term number, the log
//! index, and the server id. Wrapping them keeps the type checker honest — a
//! [`Term`] cannot be passed where a [`LogIndex`] is expected — and gives each
//! concept one place to state its invariants.
//!
//! All three are `Copy` and totally ordered. [`NodeId`] is `Ord` on purpose:
//! the driver iterates peers in sorted order so that behaviour never depends on
//! hash-map iteration order, which keeps runs deterministic and replayable.
//!
//! Constructing and comparing these types performs no IO, reads no clock,
//! spawns no threads, and draws no randomness.

use std::fmt;

/// A Raft term: a logical clock that increases monotonically over the life of
/// the cluster.
///
/// Terms are totally ordered and start at [`Term::ZERO`]. A node's current term
/// must never decrease, so this type offers [`Term::next`] but no way to go
/// backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Term(u64);

impl Term {
    /// The term before any election has happened. A freshly initialised node is
    /// at term zero.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw term number.
    #[must_use]
    pub const fn new(term: u64) -> Self {
        Self(term)
    }

    /// The raw term number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next term, used when a follower or candidate starts an election.
    ///
    /// # Panics
    ///
    /// Panics on `u64` overflow. This cannot occur in practice: at one election
    /// per nanosecond, reaching `u64::MAX` would take roughly 585 years.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A position in the replicated log.
///
/// The log is 1-based, matching the paper. [`LogIndex::ZERO`] is the sentinel
/// "before the first entry": it is the `prevLogIndex` an `AppendEntries` carries
/// for an empty log, and the `lastApplied` / `commitIndex` of a node that has
/// applied nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogIndex(u64);

impl LogIndex {
    /// The position before the first log entry.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw index.
    #[must_use]
    pub const fn new(index: u64) -> Self {
        Self(index)
    }

    /// The raw index.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next index.
    ///
    /// # Panics
    ///
    /// Panics on `u64` overflow, which cannot occur in practice (see
    /// [`Term::next`]).
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// The previous index, saturating at [`LogIndex::ZERO`] so that stepping
    /// back from an empty log stays at the sentinel.
    #[must_use]
    pub const fn prev(self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The identifier of a server in the cluster.
///
/// Ordered so that peers can be iterated deterministically (sorted), never in
/// `HashMap` order — see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u64);

impl NodeId {
    /// Wraps a raw server id.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw server id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{LogIndex, NodeId, Term};

    #[test]
    fn term_starts_at_zero_and_only_advances() {
        assert_eq!(Term::ZERO.get(), 0);
        assert_eq!(Term::ZERO.next(), Term::new(1));
        assert_eq!(Term::new(1).next().next(), Term::new(3));
    }

    #[test]
    fn terms_are_totally_ordered() {
        assert!(Term::ZERO < Term::new(1));
        assert!(Term::new(5) > Term::new(4));

        let mut terms = [Term::new(3), Term::ZERO, Term::new(1)];
        terms.sort();
        assert_eq!(terms, [Term::ZERO, Term::new(1), Term::new(3)]);
    }

    #[test]
    fn log_index_arithmetic_saturates_at_zero() {
        assert_eq!(LogIndex::ZERO.prev(), LogIndex::ZERO);
        assert_eq!(LogIndex::ZERO.next(), LogIndex::new(1));
        assert_eq!(LogIndex::new(1).prev(), LogIndex::ZERO);
        assert_eq!(LogIndex::new(10).next(), LogIndex::new(11));
    }

    #[test]
    fn node_ids_sort_by_value() {
        let mut ids = [NodeId::new(3), NodeId::new(1), NodeId::new(2)];
        ids.sort();
        assert_eq!(ids, [NodeId::new(1), NodeId::new(2), NodeId::new(3)]);
    }

    #[test]
    fn display_is_the_bare_number() {
        assert_eq!(Term::new(7).to_string(), "7");
        assert_eq!(LogIndex::new(7).to_string(), "7");
        assert_eq!(NodeId::new(7).to_string(), "7");
    }
}
