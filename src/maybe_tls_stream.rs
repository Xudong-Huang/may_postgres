use may::io::{AsIoData, IoData};

use crate::socket::SocketExt;
use crate::tls::{ChannelBinding, TlsStream};
use std::io::{self, Read, Write};
use std::net::Shutdown;

pub enum MaybeTlsStream<S, T> {
    Raw(S),
    Tls(T),
}

impl<S, T> Read for MaybeTlsStream<S, T>
where
    S: Read,
    T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut *self {
            MaybeTlsStream::Raw(s) => s.read(buf),
            MaybeTlsStream::Tls(s) => s.read(buf),
        }
    }
}

impl<S, T> Write for MaybeTlsStream<S, T>
where
    S: Write,
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut *self {
            MaybeTlsStream::Raw(s) => s.write(buf),
            MaybeTlsStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut *self {
            MaybeTlsStream::Raw(s) => s.flush(),
            MaybeTlsStream::Tls(s) => s.flush(),
        }
    }
}

impl<S, T> TlsStream for MaybeTlsStream<S, T>
where
    S: Read + Write,
    T: TlsStream,
{
    fn channel_binding(&self) -> ChannelBinding {
        match self {
            MaybeTlsStream::Raw(_) => ChannelBinding::none(),
            MaybeTlsStream::Tls(s) => s.channel_binding(),
        }
    }
}

impl<S, T> AsIoData for MaybeTlsStream<S, T>
where
    S: AsIoData,
    T: AsIoData,
{
    fn as_io_data(&self) -> &IoData {
        match self {
            MaybeTlsStream::Raw(s) => s.as_io_data(),
            MaybeTlsStream::Tls(s) => s.as_io_data(),
        }
    }
}

impl<S, T> SocketExt for MaybeTlsStream<S, T>
where
    S: SocketExt,
    T: SocketExt,
{
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        match self {
            MaybeTlsStream::Raw(s) => s.set_nonblocking(nonblocking),
            MaybeTlsStream::Tls(s) => s.set_nonblocking(nonblocking),
        }
    }

    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match self {
            MaybeTlsStream::Raw(s) => s.shutdown(how),
            MaybeTlsStream::Tls(s) => s.shutdown(how),
        }
    }
}
