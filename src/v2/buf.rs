pub trait ReadBuf {
    fn seek(&mut self, pos: usize) -> usize;
    fn readb(&mut self) -> u8;
    fn past_eof(&mut self) -> bool;
}

pub trait AppendBuf {
    fn writeb(&mut self, u: u8);
    fn flush(&mut self);
}

