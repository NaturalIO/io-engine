# io-engine

A Rust library for block-based IO, intended to mask Linux AIO and io_uring underneath.
This project aims to provide a unified, high-performance asynchronous I/O interface for Linux systems.

## Build Requirements

To build `io-engine`, you will need:
-   Rust (stable channel recommended)
-   `clang` and `libclang-dev` (or equivalent development headers for Clang) for `bindgen` to generate FFI bindings for Linux AIO.

### Only supports Linux (Debian/Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y clang libclang-dev

### Behavior of Short IO

For read, when reaching the file end, might return 0, or short read. It's the upper level user's
job to check the result and retry.

For write, it's unusual the short write happend to filesystem, it's not efficient to retry as io-uring,
user should retry the IO
