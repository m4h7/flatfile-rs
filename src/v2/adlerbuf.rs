use v2::buf::{ReadBuf, AppendBuf};

extern crate adler32;
use self::adler32::RollingAdler32;

pub struct ReadBufAdler32<'a, T: 'a + ReadBuf> {
    adler32: RollingAdler32,
    target: &'a mut T,
}

pub struct AppendBufAdler32<'a, T: 'a + AppendBuf> {
    adler32: RollingAdler32,
    target: &'a mut T,
}

impl<'a, T: ReadBuf> ReadBuf for ReadBufAdler32<'a, T> {
    fn seek(&mut self, pos: usize) -> usize {
        self.target.seek(pos)
    }
    fn readb(&mut self) -> u8 {
        let b = self.target.readb();
        self.adler32.update(b);
        b
    }
    fn past_eof(&mut self) -> bool {
        self.target.past_eof()
    }
}

impl<'a, T: AppendBuf> AppendBuf for AppendBufAdler32<'a, T> {
    fn flush(&mut self) {
        self.target.flush();
    }
    fn writeb(&mut self, u: u8) {
        self.target.writeb(u);
        self.adler32.update(u);
    }
}

impl<'a, T: ReadBuf> ReadBufAdler32<'a, T> {
    pub fn new(b: &'a mut T) -> ReadBufAdler32<'a, T> {
        ReadBufAdler32 {
            adler32: RollingAdler32::from_value(1),
            target: b,
        }
    }
    pub fn hash(&self) -> u32 {
        self.adler32.hash()
    }
}

impl<'a, T: AppendBuf> AppendBufAdler32<'a, T> {
    pub fn new(b: &'a mut T) -> AppendBufAdler32<'a, T> {
        AppendBufAdler32 {
            adler32: RollingAdler32::from_value(1),
            target: b,
        }
    }
    pub fn hash(&self) -> u32 {
        self.adler32.hash()
    }
}
