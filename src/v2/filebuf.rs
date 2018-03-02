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
    pub fn new(f: File, bufsize: usize) -> FileBuf {
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

    fn flush(&mut self) {
        let res = self.f.write(&self.buf.as_slice()[0..self.bpos]);
        self.fpos += res.unwrap();
    }
}

impl Drop for FileBuf {
    fn drop(&mut self) {
        println!("drop!");
        self.flush();
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
            self.flush();
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
