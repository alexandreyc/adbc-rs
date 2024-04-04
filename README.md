# Arrow Database Connectivity for Rust

This is a Rust implementation of [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc).

It's still work in progress and should not be used in production.

## Development

In order to run integration tests you must:

- Have the SQLite and PostgreSQL drivers libraries in your dynamic library loader path. For instance, on macOS this can be done by putting `libadbc_driver_sqlite.dylib` and `libadbc_driver_postgresql.dylib` in `~/lib`.
- Define the environement variable `TEST_ADBC_POSTGRESQL_URI` to a valid PostgreSQL URI.
- Run `cargo test`.

## TODOs

- Implement a Rust driver
- Implement the driver exporter
- Add enum for statistics
- Add examples
