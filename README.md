# Polars Rust Lab

Using the Polars data-frames and time-series library in Rust.

See the [Polars homepage](http://pola.rs) for more information.

# Feature Flags
Everything is hidden behind feature flags, so you have to enable the things you need.
See https://pola-rs.github.io/polars-book/user-guide/installation/ for more information.

Example `Cargo.toml` snippet:

```toml
[dependencies]
polars = { version = "0.32.1", features = ["lazy", "random", "dynamic_groupby"] }
```

# Time Series

Enable the `dynamic_groupby` feature in `Cargo.toml` to allow grouping over time windows (they are dynamically computed at
runtime).


```toml
[dependencies]
polars = { version = "0.32.1", features = ["lazy", "random", "dynamic_groupby"] }
```