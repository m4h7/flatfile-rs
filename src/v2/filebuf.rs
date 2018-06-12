use std::fs::File;
use std::io::{Read, Write};
use v2::buf::{ReadBuf, AppendBuf};

pub struct ReadFileBuf {
    f: File,
    buf: Vec<u8>,
    // position in the buffer
    bpos: usize,
    // number of bytes read into buf
    bsize: usize,
    // hit eof during the last read
    eof: bool,
}

impl ReadFileBuf {
    pub fn new(mut f: File, bufsize: usize) -> ReadFileBuf {
        let mut vec = Vec::with_capacity(bufsize);
        vec.resize(bufsize, 0);

        let result = f.read(&mut vec);

        let (bsize, eof) = match result {
            Ok(n) => (n, n < vec.len()),
            Err(_) => (0, true)
        };

        ReadFileBuf {
            f: f,
            buf: vec,
            eof: eof,
            bpos: 0,
            bsize: bsize,
        }
    }

    fn refill(&mut self)
    {
        assert!(!self.eof);
        assert!(self.bpos == self.bsize);

        let result = self.f.read(&mut self.buf);
        let (bsize, eof) = match result {
            Ok(n) => (n, n < self.buf.len()),
            Err(_) => (0, true)
        };

        self.bsize = bsize;
        self.eof = eof;
        self.bpos = 0;
    }
}

impl ReadBuf for ReadFileBuf {
    fn seek(&mut self, pos: usize) -> usize {
        panic!("not impl");
    }
    fn readb(&mut self) -> u8 {
        if self.bpos >= self.bsize && !self.eof {
            self.refill();
        }
        if self.bpos < self.bsize {
            let c = self.buf[self.bpos];
            self.bpos += 1;
            c
        } else {
            assert!(self.eof);
            0 as u8
        }
    }
    fn past_eof(&mut self) -> bool {
        self.eof
    }
}

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
