use v2::buf::Buf;

extern crate memmap;
use self::memmap::{MmapOptions, Mmap};
use std::io::Write;
use std::fs::File;

pub struct MmapBuf {
    f: File,
    m: Mmap,
    pos: usize,
    overflow: bool,
}

impl MmapBuf {
    pub fn new(f: File) -> MmapBuf {
        let mmap = unsafe { Mmap::map(&f) };
        MmapBuf {
            m: mmap.unwrap(),
            f: f,
            pos: 0,
            overflow: false,
        }
    }
}

impl Buf for MmapBuf {
    fn seek(&mut self, pos: usize) -> usize {
        self.pos = pos;
        if self.pos > self.m.len() {
            self.overflow = true;
        }
        self.pos
    }

    #[inline]
    fn readb(&mut self) -> u8 {
        if self.pos >= self.m.len() {
            self.overflow = true;
            0 as u8
        } else {
            let u = self.m[self.pos];
            self.pos += 1;
            u
        }
    }

    #[inline]
    fn writeb(&mut self, u: u8) {
        self.overflow = true;
    }

    fn is_overflow(&self) -> bool {
        self.overflow
    }
}
