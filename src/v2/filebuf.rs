use std::fs::File;
use std::io::Write;
use v2::buf::Buf;

pub struct FileBuf {
    f: File,
    fpos: usize,
    buf: Vec<u8>,
    bpos: usize,
    overflow: bool,
}

impl FileBuf {
    fn new(f: File, bufsize: usize) -> FileBuf {
        let mut vec = Vec::with_capacity(bufsize);
        vec.resize(bufsize, 0);

        FileBuf {
            f: f,
            fpos: 0,
            buf: vec,
            bpos: 0,
            overflow: false,
        }
    }
}

impl Buf for FileBuf {
    #[inline]
    fn seek(&mut self, pos: usize) -> usize {
        if self.bpos <= self.buf.len() {
            self.bpos = pos;
        }
        self.overflow = self.bpos >= self.buf.len();
        self.bpos
    }

    #[inline]
    fn readb(&mut self) -> u8 {
        if self.bpos < self.buf.len() {
            let r = self.buf[self.bpos];
            self.bpos += 1;
            r
        } else {
            0 as u8
        }
    }

    #[inline]
    fn writeb(&mut self, b: u8) {
        if self.bpos >= self.buf.len() {
            let res = self.f.write(self.buf.as_slice());
            self.fpos += res.unwrap();
            self.bpos = 0;
        } else {
            self.buf[self.bpos] = b;
            self.bpos += 1;
        }
    }

    fn is_overflow(&self) -> bool {
        self.overflow
    }
}
