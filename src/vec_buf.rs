use bytes::Bytes;
use std::io::{IoSlice, Write};
use std::mem::MaybeUninit;

const MAX_VEC_BUF: usize = 128;

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
            bufs: Vec::with_capacity(MAX_VEC_BUF),
            io_slice: unsafe { MaybeUninit::zeroed().assume_init() },
            writer,
        }
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn write_bytes(&mut self, buf: Bytes) -> std::io::Result<()> {
        let len = self.bufs.len();
        assert!(len < MAX_VEC_BUF);
        self.bufs.push(buf);
        let slice = IoSlice::new(unsafe { std::mem::transmute(&self.bufs[len][..]) });
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
                let slice = IoSlice::new(unsafe { std::mem::transmute(&buf[self.pos..]) });
                self.io_slice[self.block] = slice;
                break;
            }
        }
    }

    // write all data from the vecs to the writer
    pub fn flush(&mut self) -> std::io::Result<()> {
        let len = self.bufs.len();
        if self.block == len {
            return Ok(());
        }

        while self.block < len {
            let n = self
                .writer
                .write_vectored(&self.io_slice[self.block..len])?;
            self.advance(n);
        }
        self.bufs.clear();
        self.block = 0;
        self.pos = 0;
        Ok(())
    }
}
