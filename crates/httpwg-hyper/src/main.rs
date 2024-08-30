use http_body_util::{BodyExt, StreamBody};
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use tokio::io::AsyncReadExt;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

use hyper_util::server::conn::auto;
use std::{convert::Infallible, fmt::Debug, pin::Pin};
use tokio::sync::mpsc;

use bytes::Bytes;
use futures::Future;
use hyper::{
    body::{Body, Frame},
    service::Service,
    Request, Response,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

pub(crate) struct TestService;

mod async_read_body;

const BLOCK: &[u8] = b"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";

type BoxBody<E> = Pin<Box<dyn Body<Data = Bytes, Error = E> + Send + Sync + 'static>>;

impl<B, E> Service<Request<B>> for TestService
where
    B: Body<Data = Bytes, Error = E> + Send + Sync + Unpin + 'static,
    E: Debug + Send + Sync + 'static,
{
    type Response = Response<BoxBody<E>>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn call(&self, req: Request<B>) -> Self::Future {
        Box::pin(async move {
            let (parts, mut req_body) = req.into_parts();

            let path = parts.uri.path();
            match path {
                "/echo-body" => {
                    let body: BoxBody<E> = Box::pin(req_body);
                    let res = Response::builder().body(body).unwrap();
                    Ok(res)
                }
                _ => {
                    let parts = path.trim_start_matches('/').split('/').collect::<Vec<_>>();

                    // read everything from req body
                    while let Some(_frame) = req_body.frame().await {
                        // got frame, nice
                    }

                    let body: BoxBody<E> =
                        Box::pin(http_body_util::Empty::new().map_err(|_| unreachable!()));

                    if let ["status", code] = parts.as_slice() {
                        let code = code.parse::<u16>().unwrap();
                        let res = Response::builder().status(code).body(body).unwrap();
                        debug!("Replying with {:?} {:?}", res.status(), res.headers());
                        Ok(res)
                    } else if let ["repeat-4k-blocks", repeat] = parts.as_slice() {
                        let repeat = repeat.parse::<usize>().unwrap();

                        // TODO: custom impl of the Body trait to avoid channel overhead
                        let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, E>>(1);

                        tokio::spawn(async move {
                            let block = "this is a fairly large block".repeat(4096);
                            let block = Bytes::from(block);
                            for _ in 0..repeat {
                                let frame = Frame::data(block.clone());
                                let _ = tx.send(Ok(frame)).await;
                            }
                        });

                        let rx = ReceiverStream::new(rx);
                        let body: BoxBody<E> = Box::pin(StreamBody::new(rx));
                        let res = Response::builder().body(body).unwrap();
                        Ok(res)
                    } else if let ["stream-file", name] = parts.as_slice() {
                        let name = name.to_string();

                        // stream 64KB blocks of the file
                        let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, E>>(1);
                        tokio::spawn(async move {
                            let mut file =
                                tokio::fs::File::open(format!("/tmp/stream-file/{name}"))
                                    .await
                                    .unwrap();
                            let mut buf = Vec::with_capacity(64 * 1024);
                            while let Ok(n) = file.read(&mut buf).await {
                                if n == 0 {
                                    break;
                                }
                                let frame = Frame::data(Bytes::copy_from_slice(&buf[..n]));
                                let _ = tx.send(Ok(frame)).await;
                                buf.drain(..n);
                            }
                        });

                        let rx = ReceiverStream::new(rx);
                        let body: BoxBody<E> = Box::pin(StreamBody::new(rx));
                        let res = Response::builder().body(body).unwrap();
                        Ok(res)
                    } else {
                        let body = "it's less dire to lose, than to lose oneself".to_string();
                        let body: BoxBody<E> = Box::pin(body.map_err(|_| unreachable!()));
                        let res = Response::builder().status(200).body(body).unwrap();
                        Ok(res)
                    }
                }
            }
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let port = std::env::var("PORT").unwrap_or("0".to_string());
    let addr = std::env::var("ADDR").unwrap_or("127.0.0.1".to_string());
    let ln = tokio::net::TcpListener::bind(format!("{addr}:{port}"))
        .await
        .unwrap();
    let upstream_addr = ln.local_addr().unwrap();
    println!("I listen on {upstream_addr}");

    #[derive(Debug, Clone, Copy)]
    enum Proto {
        H1,
        H2,
    }

    let proto = match std::env::var("TEST_PROTO")
        .unwrap_or("h1".to_string())
        .as_str()
    {
        "h1" => Proto::H1,
        "h2" => Proto::H2,
        _ => panic!("TEST_PROTO must be either 'h1' or 'h2'"),
    };
    println!("Using {proto:?} protocol (export TEST_PROTO=h1 or TEST_PROTO=h2 to override)");

    while let Ok((stream, _)) = ln.accept().await {
        stream.set_nodelay(true).unwrap();

        tokio::spawn(async move {
            let mut builder = auto::Builder::new(TokioExecutor::new());

            match proto {
                Proto::H1 => {
                    builder = builder.http1_only();
                }
                Proto::H2 => {
                    builder = builder.http2_only();
                }
            }
            builder
                .serve_connection(TokioIo::new(stream), TestService)
                .await
        });
    }
}
