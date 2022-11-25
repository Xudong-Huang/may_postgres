#[cfg(unix)]
use may::os::unix::net::UnixStream;
use may::{io::AsIoData, net::TcpStream};
use std::io::{self, Read, Write};
use std::net::Shutdown;

#[derive(Debug)]
enum Inner {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

/// The standard stream type used by the crate.
///
/// Requires the `runtime` Cargo feature (enabled by default).
#[derive(Debug)]
pub struct Socket(Inner);

impl Socket {
    pub(crate) fn new_tcp(stream: TcpStream) -> Socket {
        Socket(Inner::Tcp(stream))
    }

    #[cfg(unix)]
    pub(crate) fn new_unix(stream: UnixStream) -> Socket {
        Socket(Inner::Unix(stream))
    }
}

impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.0 {
            Inner::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            Inner::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.0 {
            Inner::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            Inner::Unix(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.0 {
            Inner::Tcp(s) => s.flush(),
            #[cfg(unix)]
            Inner::Unix(s) => s.flush(),
        }
    }
}

impl AsIoData for Socket {
    fn as_io_data(&self) -> &may::io::IoData {
        match &self.0 {
            Inner::Tcp(s) => s.as_io_data(),
            #[cfg(unix)]
            Inner::Unix(s) => s.as_io_data(),
        }
    }
}

pub trait SocketExt: AsIoData + Send + 'static {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

impl SocketExt for TcpStream {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.set_nonblocking(nonblocking)
    }
    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.shutdown(how)
    }
}

impl SocketExt for UnixStream {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.set_nonblocking(nonblocking)
    }
    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.shutdown(how)
    }
}

impl SocketExt for Socket {
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        match &self.0 {
            Inner::Tcp(s) => s.set_nonblocking(nonblocking),
            #[cfg(unix)]
            Inner::Unix(s) => s.set_nonblocking(nonblocking),
        }
    }
    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match &self.0 {
            Inner::Tcp(s) => s.shutdown(how),
            #[cfg(unix)]
            Inner::Unix(s) => s.shutdown(how),
        }
    }
}
