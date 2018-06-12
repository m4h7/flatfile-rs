use std::fs::File;
use std::io::Write;
use v2::buf::{AppendBuf};

pub struct FileBuf {
    f: File,
    fpos: usize,
    buf: Vec<u8>,
    bpos: usize,
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
        }
    }

    fn flush_all(&mut self) {
        let res = self.f.write(&self.buf.as_slice()[0..self.bpos]);
        self.fpos += res.unwrap();
    }
}

impl Drop for FileBuf {
    fn drop(&mut self) {
        self.flush_all();
    }
}

impl AppendBuf for FileBuf {
    #[inline]
    fn flush(&mut self) {
        self.flush_all();
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
}
