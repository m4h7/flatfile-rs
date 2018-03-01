pub trait Buf {
    fn seek(&mut self, pos: usize) -> usize;
    fn readb(&mut self) -> u8;
    fn writeb(&mut self, u: u8);
    fn is_overflow(&self) -> bool;
}
