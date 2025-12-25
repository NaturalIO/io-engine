# io-engine

A Rust library for block-based IO, intended to mask Linux AIO and io_uring underneath.
This project aims to provide a unified, high-performance asynchronous I/O interface for Linux systems.

## Build Requirements

To build `io-engine`, you will need:
-   Rust (stable channel recommended)
-   `clang` and `libclang-dev` (or equivalent development headers for Clang) for `bindgen` to generate FFI bindings for Linux AIO.

### Linux (Debian/Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y clang libclang-dev
```

### Other Systems

Please refer to your system's package manager documentation for installing `clang` and `libclang-dev`.

## Building the Project

To build the project:

```bash
cargo build
```

## Running Tests

To run the tests:

```bash
cargo test
```
