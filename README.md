# Polars Rust Lab

Using the Polars data-frames and time-series library in Rust.

See the [Polars homepage](http://pola.rs) for more information.

# Compilation Speed

As soon as Polars is included, the compile time skyrockets.

# Feature Flags

Everything is hidden behind feature flags, so you have to enable the things you need.
See https://pola-rs.github.io/polars-book/user-guide/installation/ for more information.

Example `Cargo.toml` snippet:

```toml
[dependencies]
polars = { version = "0.48.1", features = ["lazy", "random", "dynamic_group_by", "temporal", "dtype-date", "dtype-datetime", "timezones", "strings"] }
```

# Time Series

Enable the `dynamic_group_by` feature in `Cargo.toml` to allow grouping over time windows (they are dynamically computed
at
runtime).

We also enable the `temporal` and `timezones` feature to allow for time series operations and time zone conversions.

```toml
[dependencies]
polars = { version = "0.48.1", features = ["lazy", "random", "dynamic_group_by", "temporal", "dtype-date", "dtype-datetime", "timezones", "strings"] }
```