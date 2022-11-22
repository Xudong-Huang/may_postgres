use bytes::Bytes;
use std::io::{IoSlice, Write};

const MAX_VEC_BUF: usize = 256;

pub struct VecBufs {
    block: usize,
    pos: usize,
    bufs: Vec<Bytes>,
    io_slice: [IoSlice<'static>; MAX_VEC_BUF],
}

impl VecBufs {
    pub fn new() -> Self {
        VecBufs {
            block: 0,
            pos: 0,
            bufs: Vec::with_capacity(MAX_VEC_BUF),
            io_slice: unsafe { std::mem::zeroed() },
        }
    }

    pub fn write_bytes<W: Write>(&mut self, buf: Bytes, writer: &mut W) -> std::io::Result<()> {
        let len = self.bufs.len();
        assert!(len < MAX_VEC_BUF);
        self.bufs.push(buf);
        let slice = IoSlice::new(unsafe { std::mem::transmute(&self.bufs[len][..]) });
        self.io_slice[len] = slice;

        if len + 1 == MAX_VEC_BUF {
            self.flush(writer)?;
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
                let slice = IoSlice::new(unsafe { std::mem::transmute(&buf[self.pos..]) });
                self.io_slice[self.block] = slice;
                break;
            }
        }
    }

    // write all data from the vecs to the writer
    pub fn flush<W: Write>(&mut self, writer: &mut W) -> std::io::Result<()> {
        let len = self.bufs.len();
        if self.block == len {
            return Ok(());
        }

        while self.block < len {
            let n = writer.write_vectored(&self.io_slice[self.block..len])?;
            self.advance(n);
        }
        self.bufs.clear();
        self.block = 0;
        self.pos = 0;
        Ok(())
    }
}
