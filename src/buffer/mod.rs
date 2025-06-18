mod buffer;
mod utils;

pub use buffer::{Buffer, MAX_BUFFER_SIZE};

pub use utils::*;

#[cfg(feature = "compress")]
pub mod lz4;

#[cfg(test)]
mod test;
