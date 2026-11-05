# synclite-rs

`synclite-rs` is the top-level Rust facade for SyncLite.

## Configuration Changes

Repeated `initialize` calls validate configuration against the running device
worker. Device and destination identity, storage/checkpoint paths, sync mode,
table/value/type mappings, and schema/trigger semantics require explicit
reinitialization rather than live replacement. Conflicts identify the setting
without printing its value.

Tracing, retry policy, timeouts, batch sizes, polling, cleanup/error-handling
policy, object-initialization policy, predicate optimization, and separately supplied destination credentials
can change on a reused worker. Updates are queued and take effect between
processing operations, not in the middle of a transaction. PostgreSQL connection
strings may change credentials and nonrouting parameters such as TLS settings,
application names, and timeouts. Host/address, port, effective database, and
schema-affecting session options remain protected. SQLite/DuckDB destination
file targets remain protected as well.

Successful initialization persists destination/runtime settings and the stage
directory for later path-only connection opens. A rejected invariant change does
not replace the saved destination snapshot.
An active snapshot keeps its starting initialization policy until completion.
The one-shot reinitialize override never replaces the saved configured policy.

## Upgrading From 1.1.0

Stop the old application process before starting 1.2.0. Keep the existing source
database, device metadata, stage/work directories, and destination configuration.
A version upgrade alone does not require device reinitialization or deletion of
checkpoints. The ownership registry is process-local and does not survive the
old process; 1.1.0 metadata keys remain readable by 1.2.0.

The legacy-metadata regression covers SQLite and DuckDB sources in both
CONSOLIDATION and REPLICATION modes against a SQLite destination. It checks
preserved identity and existing data, non-default initialization policy, new writes, repeated opens, and worker
restart without a second snapshot. This is not a full test using released
1.1.0 producer binaries or a live PostgreSQL destination.

For full documentation, examples, and setup instructions, see the repository README:

https://github.com/syncliteio/synclite-logger-rust#readme
