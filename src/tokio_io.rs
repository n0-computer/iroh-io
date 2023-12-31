//! Blocking io for [std::fs::File], using the tokio blocking task pool.
use bytes::Bytes;
use futures::{future::LocalBoxFuture, Future, FutureExt};
use pin_project::pin_project;
use std::{
    io::{self, Read, Seek, SeekFrom},
    marker::PhantomPinned,
    path::PathBuf,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite},
    task::{spawn_blocking, JoinHandle},
};

use crate::AsyncStreamReader;

use super::{make_io_error, AsyncSliceReader, AsyncSliceWriter, AsyncStreamWriter};

const MAX_PREALLOC: usize = 1024 * 16;

/// A wrapper around a [std::fs::File] that implements [AsyncSliceReader] and [AsyncSliceWriter]
#[derive(Debug)]
pub struct File(Option<FileAdapterFsm>);

impl File {
    /// Create a new [File] from a function that creates a [std::fs::File]
    pub async fn create(
        create_file: impl FnOnce() -> io::Result<std::fs::File> + Send + 'static,
    ) -> io::Result<Self> {
        let inner = spawn_blocking(create_file).await.map_err(make_io_error)??;
        Ok(Self::from_std(inner))
    }

    /// Create a new [File] from a [std::fs::File]
    ///
    /// This is fine if you already have a [std::fs::File] and want to use it with [File],
    /// but opening a file is a blocking op that you probably don't want to do in an async context.
    pub fn from_std(file: std::fs::File) -> Self {
        Self(Some(FileAdapterFsm(file)))
    }

    /// Open a [File] from a path
    pub async fn open(path: PathBuf) -> io::Result<Self> {
        Self::create(move || std::fs::File::open(path)).await
    }

    #[cfg(test)]
    pub fn read_contents(&self) -> Vec<u8> {
        let mut std_file = &self.0.as_ref().unwrap().0;
        let mut t = Vec::new();
        // this is not needed since at least for POSIX IO "read your own writes"
        // is guaranteed.
        // std_file.sync_all().unwrap();
        std_file.rewind().unwrap();
        std_file.read_to_end(&mut t).unwrap();
        t
    }
}

/// Futures for the [File]
pub mod file {
    use bytes::Bytes;

    use super::*;

    newtype_future!(
        /// The future returned by [File::read_at]
        #[derive(Debug)]
        ReadAtFuture,
        Asyncify<'a, Bytes, FileAdapterFsm>,
        io::Result<Bytes>
    );
    newtype_future!(
        /// The future returned by [File::len]
        #[derive(Debug)]
        LenFuture,
        Asyncify<'a, u64, FileAdapterFsm>,
        io::Result<u64>
    );
    newtype_future!(
        /// The future returned by [File::write_bytes_at]
        #[derive(Debug)]
        WriteBytesAtFuture,
        Asyncify<'a, (), FileAdapterFsm>,
        io::Result<()>
    );
    newtype_future!(
        /// The future returned by [File::write_at]
        #[derive(Debug)]
        WriteAtFuture,
        Asyncify<'a, (), FileAdapterFsm>,
        io::Result<()>
    );
    newtype_future!(
        /// The future returned by [File::set_len]
        #[derive(Debug)]
        SetLenFuture,
        Asyncify<'a, (), FileAdapterFsm>,
        io::Result<()>
    );
    newtype_future!(
        /// The future returned by [File::sync]
        #[derive(Debug)]
        SyncFuture,
        Asyncify<'a, (), FileAdapterFsm>,
        io::Result<()>
    );

    impl AsyncSliceReader for File {
        type ReadAtFuture<'a> = file::ReadAtFuture<'a>;

        fn read_at(&mut self, offset: u64, len: usize) -> Self::ReadAtFuture<'_> {
            let fut = self
                .0
                .take()
                .map(|t| (t.read_at(offset, len), &mut self.0))
                .into();
            ReadAtFuture(fut)
        }

        type LenFuture<'a> = LenFuture<'a>;

        fn len(&mut self) -> Self::LenFuture<'_> {
            let fut = self.0.take().map(|t| (t.len(), &mut self.0)).into();
            LenFuture(fut)
        }
    }

    impl AsyncSliceWriter for File {
        type WriteBytesAtFuture<'a> = WriteBytesAtFuture<'a>;

        fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> Self::WriteBytesAtFuture<'_> {
            let fut = self
                .0
                .take()
                .map(|t| (t.write_bytes_at(offset, data), &mut self.0))
                .into();
            WriteBytesAtFuture(fut)
        }

        type WriteAtFuture<'a> = WriteAtFuture<'a>;
        fn write_at(&mut self, offset: u64, data: &[u8]) -> Self::WriteAtFuture<'_> {
            let fut = self
                .0
                .take()
                .map(|t| (t.write_at(offset, data), &mut self.0))
                .into();
            WriteAtFuture(fut)
        }

        type SyncFuture<'a> = SyncFuture<'a>;
        fn sync(&mut self) -> Self::SyncFuture<'_> {
            let fut = self.0.take().map(|t| (t.sync(), &mut self.0)).into();
            SyncFuture(fut)
        }

        type SetLenFuture<'a> = SetLenFuture<'a>;
        fn set_len(&mut self, len: u64) -> Self::SetLenFuture<'_> {
            let fut = self.0.take().map(|t| (t.set_len(len), &mut self.0)).into();
            SetLenFuture(fut)
        }
    }
}

/// A future wrapper to unpack the result of a sync computation and store the
/// state on completion, making the io object available again.
#[derive(Debug)]
#[pin_project(project = AsyncifyProj)]
enum Asyncify<'a, R, T> {
    /// we got a future and a handle where we can store the state on completion
    Ok(
        #[pin] tokio::task::JoinHandle<(T, io::Result<R>)>,
        &'a mut Option<T>,
    ),
    /// the handle was busy
    BusyErr,
}

impl<'a, R, T> From<Option<(JoinHandle<(T, io::Result<R>)>, &'a mut Option<T>)>>
    for Asyncify<'a, R, T>
{
    fn from(value: Option<(JoinHandle<(T, io::Result<R>)>, &'a mut Option<T>)>) -> Self {
        match value {
            Some((f, h)) => Self::Ok(f, h),
            None => Self::BusyErr,
        }
    }
}

impl<'a, T: 'a, R> Future for Asyncify<'a, R, T> {
    type Output = io::Result<R>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            AsyncifyProj::Ok(f, h) => f.poll(cx).map(|x| {
                match x {
                    Ok((state, r)) => {
                        // we got a result, so we can store the state
                        **h = Some(state);
                        r
                    }
                    Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
                }
            }),
            AsyncifyProj::BusyErr => Poll::Ready(io::Result::Err(io::Error::new(
                io::ErrorKind::Other,
                "previous io op not polled to completion",
            ))),
        }
    }
}

/// A wrapper around a [std::fs::File] that defines IO operations that spawn blocking tasks.
///
/// This implements all operations of [AsyncSliceReader] and [AsyncSliceWriter] in state
/// passing style.
#[derive(Debug)]
struct FileAdapterFsm(std::fs::File);

impl FileAdapterFsm {
    fn read_at(mut self, offset: u64, len: usize) -> JoinHandle<(Self, io::Result<Bytes>)> {
        fn inner<R: std::io::Read + std::io::Seek>(
            this: &mut R,
            offset: u64,
            len: usize,
            buf: &mut Vec<u8>,
        ) -> io::Result<()> {
            this.seek(SeekFrom::Start(offset))?;
            this.take(len as u64).read_to_end(buf)?;
            Ok(())
        }
        spawn_blocking(move || {
            // len is just the expected len, so if it is too big, we should not allocate
            // the entire size.
            let mut buf = Vec::with_capacity(len.min(MAX_PREALLOC));
            let res = inner(&mut self.0, offset, len, &mut buf);
            (self, res.map(|_| buf.into()))
        })
    }

    fn len(mut self) -> JoinHandle<(Self, io::Result<u64>)> {
        spawn_blocking(move || {
            let res = self.0.seek(SeekFrom::End(0));
            (self, res)
        })
    }
}

impl FileAdapterFsm {
    fn write_bytes_at(mut self, offset: u64, data: Bytes) -> JoinHandle<(Self, io::Result<()>)> {
        fn inner<W: std::io::Write + std::io::Seek>(
            this: &mut W,
            offset: u64,
            buf: &[u8],
        ) -> io::Result<()> {
            this.seek(SeekFrom::Start(offset))?;
            this.write_all(buf)?;
            Ok(())
        }
        spawn_blocking(move || {
            let res = inner(&mut self.0, offset, &data);
            (self, res)
        })
    }

    fn write_at(mut self, offset: u64, bytes: &[u8]) -> JoinHandle<(Self, io::Result<()>)> {
        fn inner<W: std::io::Write + std::io::Seek>(
            this: &mut W,
            offset: u64,
            buf: smallvec::SmallVec<[u8; 16]>,
        ) -> io::Result<()> {
            this.seek(SeekFrom::Start(offset))?;
            this.write_all(&buf)?;
            Ok(())
        }
        let t: smallvec::SmallVec<[u8; 16]> = bytes.into();
        spawn_blocking(move || {
            let res = inner(&mut self.0, offset, t);
            (self, res)
        })
    }

    fn set_len(self, len: u64) -> JoinHandle<(Self, io::Result<()>)> {
        spawn_blocking(move || {
            let res = self.0.set_len(len);
            (self, res)
        })
    }

    fn sync(self) -> JoinHandle<(Self, io::Result<()>)> {
        spawn_blocking(move || {
            let res = self.0.sync_all();
            (self, res)
        })
    }
}

/// Utility to convert an [AsyncWrite] into an [AsyncSliceWriter] by just ignoring the offsets
#[derive(Debug)]
pub struct ConcatenateSliceWriter<W>(W);

impl<W> ConcatenateSliceWriter<W> {
    /// Create a new `ConcatenateSliceWriter` from an inner writer
    pub fn new(inner: W) -> Self {
        Self(inner)
    }

    /// Return the inner writer
    pub fn into_inner(self) -> W {
        self.0
    }
}

impl<W: AsyncWrite + Unpin + 'static> AsyncSliceWriter for ConcatenateSliceWriter<W> {
    type WriteBytesAtFuture<'a> = concatenate_slice_writer::WriteBytesAt<'a, W>;
    fn write_bytes_at(&mut self, _offset: u64, data: Bytes) -> Self::WriteBytesAtFuture<'_> {
        tokio_helper::write_bytes(&mut self.0, data)
    }

    type WriteAtFuture<'a> = concatenate_slice_writer::WriteAt<'a, W>;
    fn write_at<'a>(&'a mut self, _offset: u64, bytes: &'a [u8]) -> Self::WriteAtFuture<'a> {
        tokio_helper::write(&mut self.0, bytes)
    }

    type SyncFuture<'a> = concatenate_slice_writer::Sync<'a, W>;
    fn sync(&mut self) -> Self::SyncFuture<'_> {
        tokio_helper::flush(&mut self.0)
    }

    type SetLenFuture<'a> = futures::future::Ready<io::Result<()>>;
    fn set_len(&mut self, _len: u64) -> Self::SetLenFuture<'_> {
        futures::future::ready(io::Result::Ok(()))
    }
}

/// Futures for [ConcatenateSliceWriter].
pub mod concatenate_slice_writer {
    use super::*;

    pub use tokio_helper::{Flush as Sync, Write as WriteAt, WriteBytes as WriteBytesAt};
}

/// Utility to convert a [tokio::io::AsyncWrite] into an [AsyncStreamWriter].
#[derive(Debug, Clone)]
pub struct TokioStreamWriter<T>(pub T);

impl<T: tokio::io::AsyncWrite + Unpin> AsyncStreamWriter for TokioStreamWriter<T> {
    type WriteFuture<'a> = tokio_stream_writer::Write<'a, T> where Self: 'a;

    fn write<'a>(&'a mut self, data: &'a [u8]) -> Self::WriteFuture<'a> {
        tokio_helper::write(&mut self.0, data)
    }

    type WriteBytesFuture<'a> = tokio_stream_writer::WriteBytes<'a, T> where Self: 'a;

    fn write_bytes(&mut self, data: Bytes) -> Self::WriteBytesFuture<'_> {
        tokio_helper::write_bytes(&mut self.0, data)
    }

    type SyncFuture<'a> = tokio_stream_writer::Sync<'a, T> where Self: 'a;

    fn sync(&mut self) -> Self::SyncFuture<'_> {
        tokio_helper::flush(&mut self.0)
    }
}

/// Utility to convert a [tokio::io::AsyncRead] into an [AsyncStreamReader].
#[derive(Debug, Clone)]
pub struct TokioStreamReader<T>(T);

impl<T: tokio::io::AsyncRead + Unpin> AsyncStreamReader for TokioStreamReader<T> {
    type ReadFuture<'a> = LocalBoxFuture<'a, io::Result<Bytes>>
    where
        Self: 'a;

    fn read(&mut self, len: usize) -> Self::ReadFuture<'_> {
        async move {
            let mut buf = Vec::with_capacity(len.min(MAX_PREALLOC));
            (&mut self.0).take(len as u64).read_to_end(&mut buf).await?;
            Ok(buf.into())
        }
        .boxed_local()
    }
}

/// Futures for [TokioStreamWriter].
pub mod tokio_stream_writer {
    use super::*;

    pub use tokio_helper::{Flush as Sync, Write, WriteBytes};
}

/// Futures copied from tokio, because they are private in tokio
mod tokio_helper {
    use bytes::Buf;

    use super::*;

    /// Future that writes a slice to a writer
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[pin_project]
    pub struct Write<'a, W: ?Sized> {
        writer: &'a mut W,
        buf: &'a [u8],
        // Make this future `!Unpin` for compatibility with async trait methods.
        #[pin]
        _pin: PhantomPinned,
    }

    pub(crate) fn write<'a, W>(writer: &'a mut W, buf: &'a [u8]) -> Write<'a, W>
    where
        W: AsyncWrite + Unpin + ?Sized,
    {
        Write {
            writer,
            buf,
            _pin: PhantomPinned,
        }
    }

    impl<W> Future for Write<'_, W>
    where
        W: AsyncWrite + Unpin + ?Sized,
    {
        type Output = io::Result<()>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let me = self.project();
            while !me.buf.is_empty() {
                let n = ready!(Pin::new(&mut *me.writer).poll_write(cx, me.buf))?;
                {
                    let (_, rest) = std::mem::take(&mut *me.buf).split_at(n);
                    *me.buf = rest;
                }
                if n == 0 {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
            }

            Poll::Ready(Ok(()))
        }
    }

    /// Future that writes Bytes to a writer
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[pin_project]
    pub struct WriteBytes<'a, W: ?Sized> {
        writer: &'a mut W,
        bytes: Bytes,
        // Make this future `!Unpin` for compatibility with async trait methods.
        #[pin]
        _pin: PhantomPinned,
    }

    pub(crate) fn write_bytes<W>(writer: &mut W, bytes: Bytes) -> WriteBytes<'_, W>
    where
        W: AsyncWrite + Unpin + ?Sized,
    {
        WriteBytes {
            writer,
            bytes,
            _pin: PhantomPinned,
        }
    }

    impl<W> Future for WriteBytes<'_, W>
    where
        W: AsyncWrite + Unpin + ?Sized,
    {
        type Output = io::Result<()>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let me = self.project();
            while !me.bytes.is_empty() {
                let n = ready!(Pin::new(&mut *me.writer).poll_write(cx, me.bytes))?;
                // this could panic if n > len, but that would violate the contract of AsyncWrite
                me.bytes.advance(n);
                if n == 0 {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
            }

            Poll::Ready(Ok(()))
        }
    }

    /// Future that flushes a writer
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[pin_project]
    pub struct Flush<'a, A: ?Sized> {
        a: &'a mut A,
        // Make this future `!Unpin` for compatibility with async trait methods.
        #[pin]
        _pin: PhantomPinned,
    }

    /// Creates a future which will entirely flush an I/O object.
    pub(crate) fn flush<A>(a: &mut A) -> Flush<'_, A>
    where
        A: AsyncWrite + Unpin + ?Sized,
    {
        Flush {
            a,
            _pin: PhantomPinned,
        }
    }

    impl<A> Future for Flush<'_, A>
    where
        A: AsyncWrite + Unpin + ?Sized,
    {
        type Output = io::Result<()>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let me = self.project();
            Pin::new(&mut *me.a).poll_flush(cx)
        }
    }
}
