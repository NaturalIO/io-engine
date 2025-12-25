#[macro_use]
extern crate log;
#[macro_use]
extern crate captains_log;

pub mod callback_worker;
pub mod embedded_list;
pub mod merge;
pub mod scheduler;
pub mod tasks; // Keep this for now, will contain context and aio/uring

#[cfg(test)]
extern crate rand;
#[cfg(test)]
mod test;
