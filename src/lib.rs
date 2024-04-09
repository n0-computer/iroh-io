//! Traits for async local IO, and implementations for local files, http resources and memory.
//! This crate provides two core traits: [AsyncSliceReader] and [AsyncSliceWriter].
//!
//! Memory implementations for Bytes and BytesMut are provided by default, file and http are
//! behind features.
//!
//! Futures for the two core traits are not required to be [Send](std::marker::Send).
//!
//! This allows implementing these traits for types that are not thread safe, such as many
//! embedded databases.
//!
//! It also allows for omitting thread synchronization primitives. E.g. you could use an
//! `Rc<RefCell<BytesMut>>` instead of a much more costly `Arc<Mutex<BytesMut>>`.
//!
//! The downside is that you have to use a local executor, e.g.
//! [LocalPoolHandle](https://docs.rs/tokio-util/latest/tokio_util/task/struct.LocalPoolHandle.html),
//! if you want to do async IO in a background task.
//!
//! A good blog post providing a rationale for these decisions is
//! [Local Async Executors and Why They Should be the Default](https://maciej.codes/2022-06-09-local-async.html).
//!
//! All futures returned by this trait must be polled to completion, otherwise
//! all subsequent calls will produce an io error.
//!
//! So it is fine to e.g. limit a call to read_at with a timeout, but once a future
//! is dropped without being polled to completion, the reader is not useable anymore.
//!
//! All methods, even those that do not modify the underlying resource, take a mutable
//! reference to self. This is to enforce linear access to the underlying resource.
//!
//! In general it is assumed that readers are cheap, so in case of an error you can
//! always get a new reader. Also, if you need concurrent access to the same resource,
//! create multiple readers.
//!
//! One thing you might wonder is why there are separate methods for writing [Bytes] and writing slices.
//! The reason is that if you already have [Bytes] and the underlying writer needs [Bytes], you can avoid
//! an allocation.
#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::io::{self, Cursor};

/// A trait to abstract async reading from different resource.
///
/// This trait does not require the notion of a current position, but instead
/// requires explicitly passing the offset to read_at. In addition to the ability
/// to read at an arbitrary offset, it also provides the ability to get the
/// length of the resource.
///
/// This is similar to the io interface of sqlite.
/// See xRead, xFileSize in <https://www.sqlite.org/c3ref/io_methods.html>
#[allow(clippy::len_without_is_empty)]
pub trait AsyncSliceReader {
    /// Read the entire buffer at the given position.
    ///
    /// Will return at most `len` bytes, but may return fewer if the resource is smaller.
    /// If the range is completely covered by the resource, will return exactly `len` bytes.
    /// If the range is not covered at all by the resource, will return an empty buffer.
    /// It will never return an io error independent of the range as long as the underlying
    /// resource is valid.
    #[must_use = "io futures must be polled to completion"]
    fn read_at(&mut self, offset: u64, len: usize) -> impl Future<Output = io::Result<Bytes>>;

    /// Get the length of the resource
    #[must_use = "io futures must be polled to completion"]
    fn len(&mut self) -> impl Future<Output = io::Result<u64>>;
}

impl<'b, T: AsyncSliceReader> AsyncSliceReader for &'b mut T {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        (**self).read_at(offset, len).await
    }

    async fn len(&mut self) -> io::Result<u64> {
        (**self).len().await
    }
}

impl<T: AsyncSliceReader> AsyncSliceReader for Box<T> {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        (**self).read_at(offset, len).await
    }

    async fn len(&mut self) -> io::Result<u64> {
        (**self).len().await
    }
}

/// Extension trait for [AsyncSliceReader].
pub trait AsyncSliceReaderExt: AsyncSliceReader {
    /// Read the entire resource into a [bytes::Bytes] buffer, if possible.
    #[allow(async_fn_in_trait)]
    async fn read_to_end(&mut self) -> io::Result<Bytes> {
        self.read_at(0, usize::MAX).await
    }
}

impl<T: AsyncSliceReader> AsyncSliceReaderExt for T {}

/// A trait to abstract async writing to different resources.
///
/// This trait does not require the notion of a current position, but instead
/// requires explicitly passing the offset to write_at and write_bytes_at.
/// In addition to the ability to write at an arbitrary offset, it also provides
/// the ability to set the length of the resource.
///
/// This is similar to the io interface of sqlite.
/// See xWrite in <https://www.sqlite.org/c3ref/io_methods.html>
pub trait AsyncSliceWriter: Sized {
    /// Write the entire slice at the given position.
    ///
    /// if self.len < offset + data.len(), the underlying resource will be extended.
    /// if self.len < offset, the gap will be filled with zeros.
    #[must_use = "io futures must be polled to completion"]
    fn write_at(&mut self, offset: u64, data: &[u8]) -> impl Future<Output = io::Result<()>>;

    /// Write the entire Bytes at the given position.
    ///
    /// Use this if you have a Bytes, to avoid allocations.
    /// Other than that it is equivalent to [AsyncSliceWriter::write_at].
    #[must_use = "io futures must be polled to completion"]
    fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> impl Future<Output = io::Result<()>>;

    /// Set the length of the underlying storage.
    #[must_use = "io futures must be polled to completion"]
    fn set_len(&mut self, len: u64) -> impl Future<Output = io::Result<()>>;

    /// Sync any buffers to the underlying storage.
    #[must_use = "io futures must be polled to completion"]
    fn sync(&mut self) -> impl Future<Output = io::Result<()>>;
}

impl<'b, T: AsyncSliceWriter> AsyncSliceWriter for &'b mut T {
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        (**self).write_at(offset, data).await
    }

    async fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> io::Result<()> {
        (**self).write_bytes_at(offset, data).await
    }

    async fn set_len(&mut self, len: u64) -> io::Result<()> {
        (**self).set_len(len).await
    }

    async fn sync(&mut self) -> io::Result<()> {
        (**self).sync().await
    }
}

impl<T: AsyncSliceWriter> AsyncSliceWriter for Box<T> {
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        (**self).write_at(offset, data).await
    }

    async fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> io::Result<()> {
        (**self).write_bytes_at(offset, data).await
    }

    async fn set_len(&mut self, len: u64) -> io::Result<()> {
        (**self).set_len(len).await
    }

    async fn sync(&mut self) -> io::Result<()> {
        (**self).sync().await
    }
}

/// A non seekable reader, e.g. a network socket.
pub trait AsyncStreamReader {
    /// Read at most `len` bytes. To read to the end, pass u64::MAX.
    ///
    /// returns an empty buffer to indicate EOF.
    fn read(&mut self, len: usize) -> impl Future<Output = io::Result<Bytes>>;
}

impl<T: AsyncStreamReader> AsyncStreamReader for &mut T {
    async fn read(&mut self, len: usize) -> io::Result<Bytes> {
        (**self).read(len).await
    }
}

impl AsyncStreamReader for Bytes {
    async fn read(&mut self, len: usize) -> io::Result<Bytes> {
        let res = self.split_to(len.min(Bytes::len(self)));
        Ok(res)
    }
}

impl AsyncStreamReader for &[u8] {
    async fn read(&mut self, len: usize) -> io::Result<Bytes> {
        let len = len.min(self.len());
        let res = Bytes::copy_from_slice(&self[..len]);
        *self = &self[len..];
        Ok(res)
    }
}

impl AsyncStreamReader for BytesMut {
    async fn read(&mut self, len: usize) -> io::Result<Bytes> {
        let res = self.split_to(len.min(BytesMut::len(self)));
        Ok(res.freeze())
    }
}

impl<T: AsyncSliceReader> AsyncStreamReader for Cursor<T> {
    async fn read(&mut self, len: usize) -> io::Result<Bytes> {
        let offset = self.position();
        let res = self.get_mut().read_at(offset, len).await?;
        self.set_position(offset + res.len() as u64);
        Ok(res)
    }
}

/// A non seekable writer, e.g. a network socket.
pub trait AsyncStreamWriter {
    /// Write the entire slice.
    ///
    /// In case of an error, some bytes may have been written.
    fn write(&mut self, data: &[u8]) -> impl Future<Output = io::Result<()>>;

    /// Write the entire bytes.
    ///
    /// In case of an error, some bytes may have been written.
    fn write_bytes(&mut self, data: Bytes) -> impl Future<Output = io::Result<()>>;

    /// Sync any buffers to the underlying storage.
    fn sync(&mut self) -> impl Future<Output = io::Result<()>>;
}

impl<T: AsyncStreamWriter> AsyncStreamWriter for &mut T {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        (**self).write(data).await
    }

    async fn sync(&mut self) -> io::Result<()> {
        (**self).sync().await
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        (**self).write_bytes(data).await
    }
}

impl AsyncStreamWriter for Vec<u8> {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.extend_from_slice(data);
        Ok(())
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        self.extend_from_slice(data.as_ref());
        Ok(())
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsyncStreamWriter for BytesMut {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.extend_from_slice(data);
        Ok(())
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        self.extend_from_slice(data.as_ref());
        Ok(())
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "tokio-io")]
mod tokio_io;
#[cfg(feature = "tokio-io")]
pub use tokio_io::*;

#[cfg(feature = "stats")]
pub mod stats;

#[cfg(feature = "x-http")]
mod http;
#[cfg(feature = "x-http")]
pub use http::*;

/// implementations for [AsyncSliceReader] and [AsyncSliceWriter] for [bytes::Bytes] and [bytes::BytesMut]
mod mem;

#[cfg(feature = "tokio-util")]
impl<L, R> AsyncSliceReader for tokio_util::either::Either<L, R>
where
    L: AsyncSliceReader + 'static,
    R: AsyncSliceReader + 'static,
{
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        match self {
            Self::Left(l) => l.read_at(offset, len).await,
            Self::Right(r) => r.read_at(offset, len).await,
        }
    }

    async fn len(&mut self) -> io::Result<u64> {
        match self {
            Self::Left(l) => l.len().await,
            Self::Right(r) => r.len().await,
        }
    }
}

#[cfg(feature = "tokio-util")]
impl<L, R> AsyncSliceWriter for tokio_util::either::Either<L, R>
where
    L: AsyncSliceWriter + 'static,
    R: AsyncSliceWriter + 'static,
{
    async fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> io::Result<()> {
        match self {
            Self::Left(l) => l.write_bytes_at(offset, data).await,
            Self::Right(r) => r.write_bytes_at(offset, data).await,
        }
    }

    async fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        match self {
            Self::Left(l) => l.write_at(offset, data).await,
            Self::Right(r) => r.write_at(offset, data).await,
        }
    }

    async fn sync(&mut self) -> io::Result<()> {
        match self {
            Self::Left(l) => l.sync().await,
            Self::Right(r) => r.sync().await,
        }
    }

    async fn set_len(&mut self, len: u64) -> io::Result<()> {
        match self {
            Self::Left(l) => l.set_len(len).await,
            Self::Right(r) => r.set_len(len).await,
        }
    }
}

#[cfg(any(feature = "tokio-io", feature = "x-http"))]
fn make_io_error<E>(e: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, e)
}

#[cfg(test)]
mod tests {

    use crate::mem::limited_range;

    use super::*;
    use proptest::prelude::*;
    use std::fmt::Debug;

    #[cfg(feature = "tokio-io")]
    use std::io::Write;

    /// A test server that serves data on a random port, supporting head, get, and range requests
    #[cfg(feature = "x-http")]
    mod test_server {
        use super::*;
        use axum::{routing::get, Extension, Router};
        use hyper::{Body, Request, Response, StatusCode};
        use std::{net::SocketAddr, ops::Range, sync::Arc};

        pub fn serve(data: Vec<u8>) -> (SocketAddr, impl Future<Output = hyper::Result<()>>) {
            // Create an Axum router
            let app = Router::new()
                .route("/", get(handler))
                .layer(Extension(Arc::new(data)));

            // Create the server
            let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], 0));
            let fut = axum::Server::bind(&addr).serve(app.into_make_service());

            // Return the server address and a future that completes when the server is shut down
            (fut.local_addr(), fut)
        }

        async fn handler(state: Extension<Arc<Vec<u8>>>, req: Request<Body>) -> Response<Body> {
            let data = state.0.as_ref();
            // Check if the request contains a "Range" header
            if let Some(range_header) = req.headers().get("Range") {
                if let Ok(range) = parse_range_header(range_header.to_str().unwrap()) {
                    // Extract the requested range from the data
                    let start = range.start.min(data.len());
                    let end = range.end.min(data.len());
                    let sliced_data = &data[start..end];
                    if start == end {
                        return Response::builder()
                            .header("Content-Type", "application/octet-stream")
                            .status(StatusCode::NO_CONTENT)
                            .body(Body::from(vec![]))
                            .unwrap();
                    }

                    // Create a partial response with the sliced data
                    return Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header("Content-Type", "application/octet-stream")
                        .header("Content-Length", sliced_data.len())
                        .header(
                            "Content-Range",
                            format!("bytes {}-{}/{}", start, end - 1, data.len()),
                        )
                        .body(Body::from(sliced_data.to_vec()))
                        .unwrap();
                }
            }

            // Return the full data if no range header was found
            Response::new(data.to_owned().into())
        }

        fn parse_range_header(
            header_value: &str,
        ) -> std::result::Result<Range<usize>, &'static str> {
            let prefix = "bytes=";
            if header_value.starts_with(prefix) {
                let range_str = header_value.strip_prefix(prefix).unwrap();
                if let Some(index) = range_str.find('-') {
                    let start = range_str[..index]
                        .parse()
                        .map_err(|_| "Failed to parse range start")?;
                    let end: usize = range_str[index + 1..]
                        .parse()
                        .map_err(|_| "Failed to parse range end")?;
                    return Ok(start..end + 1);
                }
            }
            Err("Invalid Range header format")
        }
    }

    /// mutable style read smoke test, expects a resource containing 0..100u8
    async fn read_mut_smoke(mut file: impl AsyncSliceReader) -> io::Result<()> {
        let expected = (0..100u8).collect::<Vec<_>>();

        // read the whole file
        let res = file.read_at(0, usize::MAX).await?;
        assert_eq!(res, expected);

        let res = file.len().await?;
        assert_eq!(res, 100);

        // read 3 bytes at offset 0
        let res = file.read_at(0, 3).await?;
        assert_eq!(res, vec![0, 1, 2]);

        // read 10 bytes at offset 95 (near the end of the file)
        let res = file.read_at(95, 10).await?;
        assert_eq!(res, vec![95, 96, 97, 98, 99]);

        // read 10 bytes at offset 110 (after the end of the file)
        let res = file.read_at(110, 10).await?;
        assert_eq!(res, vec![]);

        Ok(())
    }

    /// mutable style write smoke test, expects an empty resource
    async fn write_mut_smoke<F: AsyncSliceWriter, C: Fn(&F) -> Vec<u8>>(
        mut file: F,
        contents: C,
    ) -> io::Result<()> {
        // write 3 bytes at offset 0
        file.write_bytes_at(0, vec![0, 1, 2].into()).await?;
        assert_eq!(contents(&file), &[0, 1, 2]);

        // write 3 bytes at offset 5
        file.write_bytes_at(5, vec![0, 1, 2].into()).await?;
        assert_eq!(contents(&file), &[0, 1, 2, 0, 0, 0, 1, 2]);

        // write a u16 at offset 8
        file.write_at(8, &1u16.to_le_bytes()).await?;
        assert_eq!(contents(&file), &[0, 1, 2, 0, 0, 0, 1, 2, 1, 0]);

        // truncate to 0
        file.set_len(0).await?;
        assert_eq!(contents(&file).len(), 0);

        Ok(())
    }

    #[cfg(feature = "tokio-io")]
    /// Tests the various ways to read from a std::fs::File
    #[tokio::test]
    async fn file_reading_smoke() -> io::Result<()> {
        // create a file with 100 bytes
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&(0..100u8).collect::<Vec<_>>()).unwrap();
        read_mut_smoke(File::from_std(file)).await?;
        Ok(())
    }

    /// Tests the various ways to read from a bytes::Bytes
    #[tokio::test]
    async fn bytes_reading_smoke() -> io::Result<()> {
        let bytes: Bytes = (0..100u8).collect::<Vec<_>>().into();
        read_mut_smoke(bytes).await?;

        Ok(())
    }

    /// Tests the various ways to read from a bytes::BytesMut
    #[tokio::test]
    async fn bytes_mut_reading_smoke() -> io::Result<()> {
        let mut bytes: BytesMut = BytesMut::new();
        bytes.extend(0..100u8);

        read_mut_smoke(bytes).await?;

        Ok(())
    }

    fn bytes_mut_contents(bytes: &BytesMut) -> Vec<u8> {
        bytes.to_vec()
    }

    #[cfg(feature = "tokio-io")]
    #[tokio::test]
    async fn async_slice_writer_smoke() -> io::Result<()> {
        let file = tempfile::tempfile().unwrap();
        write_mut_smoke(File::from_std(file), |x| x.read_contents()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn bytes_mut_writing_smoke() -> io::Result<()> {
        let bytes: BytesMut = BytesMut::new();

        write_mut_smoke(bytes, |x| x.as_ref().to_vec()).await?;

        Ok(())
    }

    fn random_slice(offset: u64, size: usize) -> impl Strategy<Value = (u64, Vec<u8>)> {
        (0..offset, 0..size).prop_map(|(offset, size)| {
            let data = (0..size).map(|x| x as u8).collect::<Vec<_>>();
            (offset, data)
        })
    }

    fn random_write_op(offset: u64, size: usize) -> impl Strategy<Value = WriteOp> {
        prop_oneof![
            20 => random_slice(offset, size).prop_map(|(offset, data)| WriteOp::Write(offset, data)),
            1 => (0..(offset + size as u64)).prop_map(WriteOp::SetLen),
            1 => Just(WriteOp::Sync),
        ]
    }

    fn random_write_ops(offset: u64, size: usize, n: usize) -> impl Strategy<Value = Vec<WriteOp>> {
        prop::collection::vec(random_write_op(offset, size), n)
    }

    fn random_read_ops(offset: u64, size: usize, n: usize) -> impl Strategy<Value = Vec<ReadOp>> {
        prop::collection::vec(random_read_op(offset, size), n)
    }

    fn sequential_offset(mag: usize) -> impl Strategy<Value = isize> {
        prop_oneof![
            20 => Just(0),
            1 => (0..mag).prop_map(|x| x as isize),
            1 => (0..mag).prop_map(|x| -(x as isize)),
        ]
    }

    fn random_read_op(offset: u64, size: usize) -> impl Strategy<Value = ReadOp> {
        prop_oneof![
            20 => (0..offset, 0..size).prop_map(|(offset, len)| ReadOp::ReadAt(offset, len)),
            1 => (sequential_offset(1024), 0..size).prop_map(|(offset, len)| ReadOp::ReadSequential(offset, len)),
            1 => Just(ReadOp::Len),
        ]
    }

    #[derive(Debug, Clone)]
    enum ReadOp {
        // read the data at the given offset
        ReadAt(u64, usize),
        // read the data relative to the previous position
        ReadSequential(isize, usize),
        // ask for the length of the file
        Len,
    }

    #[derive(Debug, Clone)]
    enum WriteOp {
        // write the data at the given offset
        Write(u64, Vec<u8>),
        // set the length of the file
        SetLen(u64),
        // sync the file
        Sync,
    }

    /// reference implementation for a vector of bytes
    fn apply_op(file: &mut Vec<u8>, op: &WriteOp) {
        match op {
            WriteOp::Write(offset, data) => {
                // an empty write is a no-op and does not resize the file
                if data.is_empty() {
                    return;
                }
                let end = offset.saturating_add(data.len() as u64);
                let start = usize::try_from(*offset).unwrap();
                let end = usize::try_from(end).unwrap();
                if end > file.len() {
                    file.resize(end, 0);
                }
                file[start..end].copy_from_slice(data);
            }
            WriteOp::SetLen(offset) => {
                let offset = usize::try_from(*offset).unwrap_or(usize::MAX);
                file.resize(offset, 0);
            }
            WriteOp::Sync => {}
        }
    }

    fn async_test<F: Future>(f: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(f)
    }

    async fn write_op_test<W: AsyncSliceWriter, C: Fn(&W) -> Vec<u8>>(
        ops: Vec<WriteOp>,
        mut bytes: W,
        content: C,
    ) -> io::Result<()> {
        let mut reference = Vec::new();
        for op in ops {
            apply_op(&mut reference, &op);
            match op {
                WriteOp::Write(offset, data) => {
                    AsyncSliceWriter::write_bytes_at(&mut bytes, offset, data.into()).await?;
                }
                WriteOp::SetLen(offset) => {
                    AsyncSliceWriter::set_len(&mut bytes, offset).await?;
                }
                WriteOp::Sync => {
                    AsyncSliceWriter::sync(&mut bytes).await?;
                }
            }
            assert_eq!(content(&bytes), reference.as_slice());
        }
        io::Result::Ok(())
    }

    async fn read_op_test<R: AsyncSliceReader + Debug>(
        ops: Vec<ReadOp>,
        mut file: R,
        actual: &[u8],
    ) -> io::Result<()> {
        let mut current = 0u64;
        for op in ops {
            match op {
                ReadOp::ReadAt(offset, len) => {
                    println!("{:?} {} {}", file, offset, len);
                    let data = AsyncSliceReader::read_at(&mut file, offset, len).await?;
                    assert_eq!(&data, &actual[limited_range(offset, len, actual.len())]);
                    current = offset.checked_add(len as u64).unwrap();
                }
                ReadOp::ReadSequential(offset, len) => {
                    let offset = if offset >= 0 {
                        current.saturating_add(offset as u64)
                    } else {
                        current.saturating_sub((-offset) as u64)
                    };
                    let data = AsyncSliceReader::read_at(&mut file, offset, len).await?;
                    assert_eq!(&data, &actual[limited_range(offset, len, actual.len())]);
                    current = offset.checked_add(len as u64).unwrap();
                }
                ReadOp::Len => {
                    let len = AsyncSliceReader::len(&mut file).await?;
                    assert_eq!(len, actual.len() as u64);
                }
            }
        }
        io::Result::Ok(())
    }

    // #[cfg(feature = "x-http")]
    // #[tokio::test]
    // async fn test_http_range() -> io::Result<()> {
    //     let url = reqwest::Url::parse("https://ipfs.io/ipfs/bafybeiaj2dgwpi6bsisyf4wq7yvj4lqvpbmlmztm35hqyqqjihybnden24/image").unwrap();
    //     let mut resource = HttpAdapter::new(url).await?;
    //     let buf = resource.read_at(0, 100).await?;
    //     println!("buf: {:?}", buf);
    //     let buf = resource.read_at(1000000, 100).await?;
    //     println!("buf: {:?}", buf);
    //     Ok(())
    // }

    #[cfg(feature = "x-http")]
    #[tokio::test]
    #[cfg_attr(target_os = "windows", ignore)]
    async fn http_smoke() {
        let (addr, server) = test_server::serve(b"hello world".to_vec());
        let url = format!("http://{}", addr);
        println!("serving from {}", url);
        let url = reqwest::Url::parse(&url).unwrap();
        let server = tokio::spawn(server);
        let mut reader = HttpAdapter::new(url);
        let len = reader.len().await.unwrap();
        assert_eq!(len, 11);
        println!("len: {:?}", reader);
        let part = reader.read_at(0, 11).await.unwrap();
        assert_eq!(part.as_ref(), b"hello world");
        let part = reader.read_at(6, 5).await.unwrap();
        assert_eq!(part.as_ref(), b"world");
        let part = reader.read_at(6, 10).await.unwrap();
        assert_eq!(part.as_ref(), b"world");
        let part = reader.read_at(100, 10).await.unwrap();
        assert_eq!(part.as_ref(), b"");
        server.abort();
    }

    proptest! {

        #[test]
        fn bytes_write(ops in random_write_ops(1024, 1024, 10)) {
            async_test(write_op_test(ops, BytesMut::new(), bytes_mut_contents)).unwrap();
        }

        #[cfg(feature = "tokio-io")]
        #[test]
        fn file_write(ops in random_write_ops(1024, 1024, 10)) {
            let file = tempfile::tempfile().unwrap();
            async_test(write_op_test(ops, File::from_std(file), |x| x.read_contents())).unwrap();
        }

        #[test]
        fn bytes_read(data in proptest::collection::vec(any::<u8>(), 0..1024), ops in random_read_ops(1024, 1024, 2)) {
            async_test(read_op_test(ops, Bytes::from(data.clone()), &data)).unwrap();
        }

        #[cfg(feature = "tokio-io")]
        #[test]
        fn file_read(data in proptest::collection::vec(any::<u8>(), 0..1024), ops in random_read_ops(1024, 1024, 2)) {
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(&data).unwrap();
            async_test(read_op_test(ops, File::from_std(file), &data)).unwrap();
        }

        #[cfg(feature = "x-http")]
        #[cfg_attr(target_os = "windows", ignore)]
        #[test]
        fn http_read(data in proptest::collection::vec(any::<u8>(), 0..10), ops in random_read_ops(10, 10, 2)) {
            async_test(async move {
                // create a test server. this has to happen in a tokio runtime
                let (addr, server) = test_server::serve(data.clone());
                // spawn the server in a background task
                let server = tokio::spawn(server);
                // create a resource from the server
                let url = reqwest::Url::parse(&format!("http://{}", addr)).unwrap();
                let file = HttpAdapter::new(url);
                // run the test
                read_op_test(ops, file, &data).await.unwrap();
                // stop the server
                server.abort();
            });
        }
    }
}
