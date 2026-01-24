// Copyright (c) 2025 NaturalIO

mod test_context;
mod test_merge;
mod test_task;

use libc;
use rand::prelude::*;
use std::fs::OpenOptions;
use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt;
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

// Create a temporary file with some content
pub fn create_temp_file(path: &path::Path) -> OwnedFd {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .expect("openfile")
        .into()
}
