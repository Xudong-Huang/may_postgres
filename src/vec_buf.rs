use bytes::Bytes;
use std::io::{IoSlice, Write};
use std::mem::MaybeUninit;

const MAX_VEC_BUF: usize = 64;

pub struct VecBufs<W> {
    block: usize,
    pos: usize,
    bufs: Vec<Bytes>,
    io_slice: [IoSlice<'static>; MAX_VEC_BUF],
    writer: W,
}

impl<W: Write> VecBufs<W> {
    pub fn new(writer: W) -> Self {
        VecBufs {
            block: 0,
            pos: 0,
            bufs: Vec::new(),
            io_slice: unsafe { MaybeUninit::uninit().assume_init() },
            writer,
        }
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn write_bytes(&mut self, buf: Bytes) -> std::io::Result<()> {
        let len = self.bufs.len();
        assert!(len < MAX_VEC_BUF);
        let slice = IoSlice::new(unsafe { std::mem::transmute(&buf[..]) });
        self.bufs.push(buf);
        self.io_slice[len] = slice;

        if len + 1 == MAX_VEC_BUF {
            self.flush()?;
        }

        Ok(())
    }

    fn advance(&mut self, n: usize) {
        let mut left = n;
        for buf in self.bufs[self.block..].iter() {
            let len = buf.len() - self.pos;
            if left >= len {
                left -= len;
                self.block += 1;
                self.pos = 0;
            } else {
                self.pos += left;
                let slice = {
                    let buf = &self.bufs[self.block][self.pos..];
                    IoSlice::new(unsafe { std::mem::transmute(buf) })
                };
                self.io_slice[self.block] = slice;
                break;
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.block == self.bufs.len()
    }

    // write all data from the vecs to the writer
    pub fn flush(&mut self) -> std::io::Result<()> {
        while !self.is_empty() {
            let n = self
                .writer
                .write_vectored(&self.io_slice[self.block..self.bufs.len()])?;
            self.advance(n);
        }
        self.bufs.clear();
        self.block = 0;
        self.pos = 0;
        Ok(())
    }
}
