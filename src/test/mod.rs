// Copyright (c) 2025 NaturalIO

mod test_context;
mod test_merge;
mod test_task;

use rand::prelude::*;
use std::os::unix::{ffi::OsStrExt, io::RawFd};
use std::path;

pub struct TempDevFile(pub String);

impl std::ops::Deref for TempDevFile {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for TempDevFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(path::Path::new(&self.0));
        info!("deleted {}", self.0);
    }
}

impl AsRef<path::Path> for TempDevFile {
    fn as_ref(&self) -> &std::path::Path {
        &path::Path::new(&self.0)
    }
}

// Create a temporary file name within the temporary directory configured in the environment.
pub fn make_temp_file() -> TempDevFile {
    let mut rng = rand::rng();
    let mut result = std::env::temp_dir();
    let filename = format!("test-aio-{}.dat", rng.random::<u64>());
    debug!("make_temp_file {}", filename);
    result.push(filename);
    TempDevFile(result.to_str().unwrap().to_string())
}

pub fn setup_log() {
    use captains_log::recipe::stderr_logger;
    let log_config = stderr_logger(log::Level::Debug).test();
    log_config.build().expect("setup_log");
}

pub struct OwnedFd {
    pub fd: RawFd,
}

impl OwnedFd {
    fn new_from_raw_fd(fd: RawFd) -> OwnedFd {
        OwnedFd { fd }
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        let result = unsafe { libc::close(self.fd) };
        assert!(result == 0);
    }
}

// Create a temporary file with some content
pub fn create_temp_file(path: &path::Path) -> OwnedFd {
    OwnedFd::new_from_raw_fd(unsafe {
        let fd = libc::open(
            std::mem::transmute(path.as_os_str().as_bytes().as_ptr()),
            libc::O_DIRECT | libc::O_RDWR | libc::O_CREAT,
        );
        if fd < 0 {
            panic!("create file {} errno={}", path.display(), nix::errno::Errno::last_raw());
        }
        fd
    })
}
