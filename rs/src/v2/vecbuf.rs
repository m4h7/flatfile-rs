use v2::buf::{ReadBuf, AppendBuf};

pub struct Vecbuf {
    buf: Vec<u8>,
    pos: usize,
    eof: bool,
}

impl Vecbuf {
    pub fn new(size: usize) -> Vecbuf {
        let mut vec = Vec::with_capacity(size);
        vec.resize(size, 0);

        Vecbuf {
            buf: vec,
            pos: 0,
            eof: false,
        }
    }

    pub fn reset(&mut self) {
        self.pos = 0;
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    fn set_eof(&mut self) {
        self.eof = true;
    }
}

impl ReadBuf for Vecbuf {
    #[inline]
    fn seek(&mut self, pos: usize) -> usize {
        if self.pos <= self.len() {
            self.pos = pos;
        }
        self.eof = self.pos >= self.len();
        self.pos
    }

    #[inline]
    fn readb(&mut self) -> u8 {
        if self.pos < self.len() {
            let r = self.buf[self.pos];
            self.pos += 1;
            r
        } else {
            0 as u8
        }
    }

    #[inline]
    fn past_eof(&mut self) -> bool {
        self.eof
    }
}

impl AppendBuf for Vecbuf {
    #[inline]
    fn flush(&mut self) {
        // does nothing for Vecbuf
    }

    #[inline]
    fn writeb(&mut self, b: u8) {
        if self.pos >= self.len() {
            // Vecbuf is a fixed length byte buffer
            self.eof = true;
        } else {
            self.buf[self.pos] = b;
            self.pos += 1;
        }
    }
}
