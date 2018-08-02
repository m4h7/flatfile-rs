use v2::buf::{ReadBuf};

extern crate memmap;
use self::memmap::{Mmap};
use std::fs::File;

pub struct MmapBuf {
    f: File,
    m: Mmap,
    pos: usize,
}

impl MmapBuf {
    pub fn new(f: File) -> MmapBuf {
        let mmap = unsafe { Mmap::map(&f) };
        MmapBuf {
            m: mmap.unwrap(),
            f: f,
            pos: 0,
        }
    }
}

impl ReadBuf for MmapBuf {
    #[inline]
    fn past_eof(&mut self) -> bool {
        // only report eof once we are PAST it
        self.pos > self.m.len()
    }

    #[inline]
    fn seek(&mut self, pos: usize) -> usize {
        if pos >= self.m.len() {
            self.pos = self.m.len();
        } else {
            self.pos = pos;
        }
        self.pos
    }

    #[inline]
    fn readb(&mut self) -> u8 {
        if self.pos >= self.m.len() {
            // have to advance the pos to make past_eof work
            self.pos += 1;
            0 as u8
        } else {
            let u = self.m[self.pos];
            self.pos += 1;
            u
        }
    }
}
