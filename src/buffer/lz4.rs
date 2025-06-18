use std::io::{Error, ErrorKind, Result};

use log::*;
use lz4_sys::*;

pub const ERR_LZ4_COMPRESS: &'static str = "lz4_compress_failed";
pub const ERR_LZ4_CDECOMPRESS: &'static str = "lz4_decompress_failed";

#[inline]
pub fn compress_bound(size: i32) -> i32 {
    unsafe { LZ4_compressBound(size) }
}

#[inline]
pub fn compress(src: &[u8], dest: &mut [u8]) -> Result<i32> {
    let compressed_len = unsafe {
        LZ4_compress_default(
            src.as_ptr() as *const libc::c_char,
            dest.as_mut_ptr() as *mut libc::c_char,
            src.len() as i32,
            dest.len() as i32,
        )
    };
    if compressed_len <= 0 {
        trace!(
            "compress fails: src len: {}, dest len : {}, compressed_len: {}",
            src.len(),
            dest.len(),
            compressed_len
        );
        Err(Error::new(ErrorKind::Other, ERR_LZ4_COMPRESS))
    } else {
        Ok(compressed_len)
    }
}

#[inline]
pub fn decompress(src: &[u8], dest: &mut [u8]) -> Result<i32> {
    let decompressed_len = unsafe {
        LZ4_decompress_safe(
            src.as_ptr() as *const libc::c_char,
            dest.as_mut_ptr() as *mut libc::c_char,
            src.len() as i32,
            dest.len() as i32,
        )
    };
    if decompressed_len <= 0 {
        trace!(
            "decompress fails: src len: {}, dest len : {}, decompressed_len: {}",
            src.len(),
            dest.len(),
            decompressed_len
        );
        Err(Error::new(ErrorKind::Other, ERR_LZ4_CDECOMPRESS))
    } else {
        Ok(decompressed_len)
    }
}

#[cfg(test)]
mod tests {

    use std::time::Instant;
    //extern crate cpuprofiler;
    use std::{
        fs::File,
        io::{Read, Write},
    };

    use super::{super::*, *};

    //use self::cpuprofiler::PROFILER;

    #[test]
    fn test_compress() {
        let buf_len: usize = 16 * 1024;
        // prepare
        let mut buffer = Buffer::alloc(buf_len).unwrap();
        getrandom::getrandom(&mut buffer).unwrap();
        let bound = compress_bound(buf_len as i32) as usize;
        println!("compress_bound={}", bound);

        // compress
        let mut compressed_buffer = Buffer::alloc(bound).unwrap();
        let compressed_len = compress(&buffer, &mut compressed_buffer).unwrap();
        let mut _compressed_buffer = Buffer::alloc(compressed_len as usize).unwrap();
        _compressed_buffer.copy_from(0, &compressed_buffer);
        println!("compressed_len={}", _compressed_buffer.len());

        // decompress
        let mut decompressed_buffer = Buffer::alloc(buf_len).unwrap();
        decompressed_buffer.set_zero(0, decompressed_buffer.len());
        let decompressed_len = decompress(&_compressed_buffer, &mut decompressed_buffer).unwrap();
        println!("decompressed_len={}", decompressed_len);
        assert_eq!(&decompressed_buffer[0..decompressed_len as usize], &buffer[0..]);
    }

    #[test]
    fn test_benchmark_compress() {
        let loop_cnt: u64 = 1000000;
        // prepare
        let mut buffer = Buffer::alloc(16 * 1024).unwrap();
        getrandom::getrandom(&mut buffer).unwrap();
        let mut bound = compress_bound(16 * 1024) as usize;
        bound = (bound + 511) / 512 * 512;

        let mut compressed_len = 0;
        //PROFILER.lock().unwrap().start("./compress.profile").unwrap();
        let start_ts = Instant::now();
        for _i in 0..loop_cnt {
            let mut compressed_buffer = Buffer::alloc(bound).unwrap();

            let mut _compressed_len = compress(&buffer, &mut compressed_buffer).unwrap();
            compressed_len = _compressed_len;
        }
        let end_ts = Instant::now();
        //PROFILER.lock().unwrap().stop().unwrap();
        println!(
            "compressed_len={}. compress speed {}(byte)/sec",
            compressed_len,
            ((loop_cnt * 16 * 1024) as f64) / (end_ts.duration_since(start_ts).as_secs_f64())
        );
    }

    #[test]
    fn test_benchmark_decompress() {
        let loop_cnt: u64 = 1000000;
        // prepare
        let mut buffer = Buffer::alloc(16 * 1024).unwrap();
        getrandom::getrandom(&mut buffer).unwrap();
        let mut bound = compress_bound(16 * 1024) as usize;
        println!("compress_bound={}", bound);

        bound = (bound + 511) / 512 * 512;
        let mut compressed_buffer = Buffer::alloc(bound).unwrap();
        let compressed_len = compress(&buffer, &mut compressed_buffer).unwrap();

        let mut decompressed_len = 0;
        //PROFILER.lock().unwrap().start("./decompress.profile").unwrap();
        let start_ts = Instant::now();
        for _i in 0..loop_cnt {
            let mut decompressed_buffer = Buffer::alloc(16 * 1024).unwrap();

            let _decompressed_len = decompress(
                &compressed_buffer[0..compressed_len as usize],
                &mut decompressed_buffer,
            )
            .unwrap();
            decompressed_len = _decompressed_len;
        }
        let end_ts = Instant::now();
        //PROFILER.lock().unwrap().stop().unwrap();
        println!(
            "decompressed_len={}. decompress speed {}(byte)/sec",
            decompressed_len,
            ((loop_cnt * 16 * 1024) as f64) / (end_ts.duration_since(start_ts).as_secs_f64())
        );
    }

    //#[test]
    #[allow(dead_code)]
    fn test_compatibility() {
        let mut src_buffer = Buffer::alloc(40 * 1024).unwrap();
        let mut dst_buffer = Buffer::alloc(40 * 1024).unwrap();
        let mut src_len: usize = 0;
        let mut dst_len: usize = 0;
        let mut compressed_buffer = Buffer::alloc(40 * 1024).unwrap();
        let mut decompressed_buffer = Buffer::alloc(40 * 1024).unwrap();
        {
            let mut file = File::open("src.lz4").unwrap();
            loop {
                let size = file.read(&mut src_buffer[src_len..]).unwrap();
                if size == 0 {
                    break;
                }
                src_len += size;
            }
            println!("src size={}", src_len);

            let compressed_len = compress(&src_buffer, &mut compressed_buffer).unwrap();
            println!("compressed len={}", compressed_len);

            let mut file_res = File::create("dst.lz4.rust").unwrap();
            file_res.write_all(&mut compressed_buffer[0..compressed_len as usize]).unwrap();
        }
        {
            let mut file = File::open("dst.lz4").unwrap();
            loop {
                let size = file.read(&mut dst_buffer[dst_len..]).unwrap();
                if size == 0 {
                    break;
                }
                dst_len += size;
            }
            println!("dst size={}", dst_len);
        }

        let decompressed_len =
            decompress(&dst_buffer[0..dst_len], &mut decompressed_buffer).unwrap();
        println!("decompressed_buffer size={}", decompressed_len);
        assert_eq!(&src_buffer[0..src_len], &decompressed_buffer[0..decompressed_len as usize]);
    }
}
