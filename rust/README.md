# Midas-rs

A Rust-based client library designed to interact with the `midas-server`, providing a streamlined interface for accessing data. It is built on top of the `mbn` library for binary encoding, but offers a higher-level abstraction that enables both streaming data and saving it to binary MBN-encoded files.

## Features

- **Stream Data**: Fetch live or historical data directly from the `midas-server` in real-time.
- **Save to File**: Save data to a binary MBN-encoded file for efficient storage and retrieval.
- **Built on MBN**: Leverages the high-performance binary encoding capabilities of the `mbn` library.

## Getting Started

To integrate `midas-rs` into your project, add the following to your `Cargo.toml`:

```toml
[dependencies]
midas_client = { git ="https://github.com/midassystems/midas-rs.git", branch ="main" }
```

## Documentation

Detailed documentation is coming soon. Stay tuned for examples and usage guides!

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests to improve the library.

## License

This project is licensed under the [Apache-2.0 License](LICENSE).
