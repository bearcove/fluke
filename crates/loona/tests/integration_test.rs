mod helpers;

use b_x::{BxForResults, BX};
use bytes::BytesMut;
use http::{header, StatusCode};
use httparse::{Status, EMPTY_HEADER};
use loona::buffet::{IntoHalves, ReadOwned, WriteOwned};
use loona::{
    buffet::{PieceCore, RollMut},
    h1, h2, Body, BodyChunk, Encoder, ExpectResponseHeaders, Headers, HeadersExt, Method, Request,
    Responder, Response, ResponseDone, ServerDriver,
};
use pretty_assertions::assert_eq;
use pretty_hex::PrettyHex;
use std::{future::Future, net::SocketAddr, rc::Rc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;
use tracing::debug;

mod proxy;
mod testbed;

// Test ideas:
// headers too large (ups/dos)
// too many headers (ups/dos)
// invalid transfer-encoding header
// chunked transfer-encoding but bad chunks
// timeout while reading request headers from downstream
// timeout while writing request headers to upstream
// connection reset from upstream after request headers
// various timeouts (slowloris, idle body, etc.)
// proxy responds with 4xx, 5xx
// upstream connection resets
// idle timeout while streaming bodies
// proxies status from upstream (200, 404, 500, etc.)
// 204
// 204 with body?
// retry / replay
// re-use upstream connection
// connection close

#[test]
fn serve_api() {
    helpers::run(async move {
        let conf = h1::ServerConf::default();
        let conf = Rc::new(conf);

        struct TestDriver;

        impl<OurEncoder> ServerDriver<OurEncoder> for TestDriver
        where
            OurEncoder: Encoder,
        {
            type Error = BX;

            async fn handle(
                &self,
                _req: loona::Request,
                _req_body: &mut impl Body,
                mut res: Responder<OurEncoder, ExpectResponseHeaders>,
            ) -> b_x::Result<Responder<OurEncoder, ResponseDone>> {
                let mut buf = RollMut::alloc()?;

                buf.put(b"Continue")?;

                res.write_interim_response(Response {
                    status: StatusCode::CONTINUE,
                    ..Default::default()
                })
                .await?;

                buf.put(b"OK")?;

                _ = buf;

                let res = res
                    .write_final_response(Response {
                        status: StatusCode::OK,
                        ..Default::default()
                    })
                    .await?;

                let res = res.finish_body(None).await?;

                Ok(res)
            }
        }

        let (mut client_write, server_read) = loona::buffet::pipe();
        let (server_write, mut client_read) = loona::buffet::pipe();
        let client_buf = RollMut::alloc()?;
        let driver = TestDriver;
        let serve_fut = loona::buffet::spawn(h1::serve(
            (server_read, server_write),
            conf,
            client_buf,
            driver,
        ));

        client_write
            .write_all_owned("GET / HTTP/1.1\r\n\r\n")
            .await?;
        let mut res_buf = BytesMut::new();
        let mut buf = vec![0u8; 1024];
        loop {
            let res;
            (res, buf) = client_read.read_owned(buf).await;
            let n = res.unwrap();
            let chunk = &buf[..n];

            debug!("Got a chunk:\n{:?}", chunk.hex_dump());
            res_buf.extend_from_slice(chunk);

            let mut headers = [EMPTY_HEADER; 16];
            let mut res = httparse::Response::new(&mut headers[..]);
            let body_offset = match res.parse(&res_buf[..]).bx()? {
                Status::Complete(off) => off,
                Status::Partial => {
                    debug!("partial response, continuing");
                    continue;
                }
            };

            debug!("Got a complete response: {res:?}");

            match res.code {
                Some(100) => {
                    assert_eq!(res.reason, Some("Continue"));
                    res_buf = res_buf.split_off(body_offset);
                }
                Some(200) => {
                    assert_eq!(res.reason, Some("OK"));
                    break;
                }
                _ => panic!("unexpected response code {:?}", res.code),
            }
        }

        drop(client_write);

        tokio::time::timeout(Duration::from_secs(5), serve_fut)
            .await
            .bx()?
            .bx()??;

        Ok(())
    })
}

#[test]
fn request_api() {
    helpers::run(async move {
        let (mut server_write, client_read) = loona::buffet::pipe();
        let (client_write, mut server_read) = loona::buffet::pipe();

        let req = Request {
            method: Method::Get,
            uri: "/".parse().unwrap(),
            ..Default::default()
        };

        struct TestDriver;

        impl h1::ClientDriver for TestDriver {
            type Return = ();
            type Error = BX;

            async fn on_informational_response(&mut self, _res: Response) -> b_x::Result<()> {
                todo!("got informational response!")
            }

            async fn on_final_response(
                self,
                res: Response,
                body: &mut impl Body,
            ) -> b_x::Result<Self::Return> {
                debug!(
                    "got final response! content length = {:?}, is chunked = {}",
                    res.headers.content_length(),
                    res.headers.is_chunked_transfer_encoding(),
                );

                while let BodyChunk::Chunk(chunk) = body.next_chunk().await.bx()? {
                    debug!("got a chunk: {:?}", chunk.hex_dump());
                }

                Ok(())
            }
        }

        let driver = TestDriver;
        let request_fut = loona::buffet::spawn(async {
            #[allow(clippy::let_unit_value)]
            let mut body = ();
            h1::request((client_read, client_write), req, &mut body, driver).await
        });

        let mut req_buf = BytesMut::new();
        let mut buf = vec![0u8; 1024];
        loop {
            let res;
            (res, buf) = server_read.read_owned(buf).await;
            let n = res.unwrap();
            let chunk = &buf[..n];
            debug!("Got a chunk:\n{:?}", chunk.hex_dump());
            req_buf.extend_from_slice(chunk);

            let mut headers = [EMPTY_HEADER; 16];
            let mut req = httparse::Request::new(&mut headers[..]);
            let body_offset = match req.parse(&req_buf[..]).bx()? {
                Status::Complete(off) => off,
                Status::Partial => {
                    debug!("partial request, continuing");
                    continue;
                }
            };

            debug!("Got a complete request: {req:?}");
            debug!("body_offset: {body_offset}");

            break;
        }

        let body = "Hi there";
        let body_len = body.len();
        server_write
            .write_all_owned(
                format!("HTTP/1.1 200 OK\r\ncontent-length: {body_len}\r\n\r\n").into_bytes(),
            )
            .await?;
        server_write.write_all_owned(body).await?;
        drop(server_write);

        tokio::time::timeout(Duration::from_secs(5), request_fut)
            .await
            .bx()?
            .bx()??;

        Ok(())
    })
}

#[test]
fn proxy_statuses() {
    #[allow(drop_bounds)]
    async fn client(ln_addr: SocketAddr, _guard: impl Drop) -> b_x::Result<()> {
        let mut socket = TcpStream::connect(ln_addr).await?;
        socket.set_nodelay(true)?;

        let mut buf = BytesMut::with_capacity(256);

        for status in [200, 400, 500] {
            debug!("Asking for a {status}");
            socket
                .write_all_owned(format!("GET /status/{status} HTTP/1.1\r\n\r\n").into_bytes())
                .await?;

            socket.flush().await?;

            debug!("Reading response...");
            'read_response: loop {
                buf.reserve(256);
                let n = socket.read_buf(&mut buf).await?;
                if n == 0 {
                    panic!("unexpected EOF");
                }
                debug!("After read, got {} bytes", buf.len());

                let mut headers = [EMPTY_HEADER; 16];
                let mut res = httparse::Response::new(&mut headers[..]);
                let _body_offset = match res.parse(&buf[..]).bx()? {
                    Status::Complete(off) => off,
                    Status::Partial => continue 'read_response,
                };
                debug!("client got a complete response");
                assert_eq!(res.code, Some(status));

                _ = buf.split();
                break 'read_response;
            }
        }

        debug!("http client shutting down");
        Ok(())
    }

    helpers::run(async move {
        let (upstream_addr, _upstream_guard) = testbed::start().await?;
        let (ln_addr, guard, proxy_fut) = proxy::start(upstream_addr).await?;
        let client_fut = client(ln_addr, guard);

        tokio::try_join!(proxy_fut, client_fut)?;
        debug!("everything has been joined");

        Ok(())
    });
}

#[test]
fn proxy_echo_body_content_len() {
    #[allow(drop_bounds)]
    async fn client(ln_addr: SocketAddr, _guard: impl Drop) -> b_x::Result<()> {
        let socket = TcpStream::connect(ln_addr).await?;
        socket.set_nodelay(true)?;

        let mut buf = BytesMut::with_capacity(256);

        debug!("Sending a small body");
        let body = "Please return to sender.";
        let content_len = body.len();

        let (mut read, mut write) = socket.into_halves();

        let send_fut = async move {
            write
                .write_all_owned(
                    format!("POST /echo-body HTTP/1.1\r\ncontent-length: {content_len}\r\n\r\n")
                        .into_bytes(),
                )
                .await?;
            write.flush().await?;

            write.write_all_owned(body.as_bytes()).await?;
            write.flush().await?;

            Ok::<(), BX>(())
        };
        loona::buffet::spawn(async move {
            if let Err(e) = send_fut.await {
                panic!("Error sending request: {e}");
            }
        });

        debug!("Reading response...");
        'read_response: loop {
            buf.reserve(256);
            let n = read.read_buf(&mut buf).await?;
            if n == 0 {
                panic!("unexpected EOF");
            }
            debug!("After read, got {} bytes", buf.len());

            let mut headers = [EMPTY_HEADER; 16];
            let mut res = httparse::Response::new(&mut headers[..]);
            let body_offset = match res.parse(&buf[..]).bx()? {
                Status::Complete(off) => off,
                Status::Partial => continue 'read_response,
            };
            debug!("client got a response header");
            assert_eq!(res.code, Some(200));

            let mut content_len = None;
            for header in res.headers.iter() {
                debug!(
                    "Got header: {} = {:?}",
                    header.name,
                    std::str::from_utf8(header.value)
                );
                if header.name.eq_ignore_ascii_case("content-length") {
                    content_len = Some(
                        std::str::from_utf8(header.value)
                            .unwrap()
                            .parse::<usize>()
                            .unwrap(),
                    );
                }
            }
            let content_len = content_len.expect("no content-length header");

            let mut buf = buf.split_off(body_offset);

            while buf.len() < content_len {
                debug!("buf has {} bytes, need {}", buf.len(), content_len);
                if buf.capacity() == 0 {
                    buf.reserve(256);
                }
                let n = read.read_buf(&mut buf).await?;
                if n == 0 {
                    panic!("unexpected EOF");
                }
                debug!("just read {n}");
            }
            debug!("Body: {:?}", std::str::from_utf8(&buf[..]));

            break 'read_response;
        }

        debug!("http client shutting down");
        Ok(())
    }

    helpers::run(async move {
        let (upstream_addr, _upstream_guard) = testbed::start().await?;
        let (ln_addr, guard, proxy_fut) = proxy::start(upstream_addr).await?;
        let client_fut = client(ln_addr, guard);

        tokio::try_join!(proxy_fut, client_fut)?;
        debug!("everything has been joined");

        Ok(())
    });
}

#[test]
fn proxy_echo_body_chunked() {
    #[allow(drop_bounds)]
    async fn client(ln_addr: SocketAddr, _guard: impl Drop) -> b_x::Result<()> {
        let socket = TcpStream::connect(ln_addr).await?;
        socket.set_nodelay(true)?;

        let mut buf = BytesMut::with_capacity(256);

        let (mut read, mut write) = socket.into_halves();

        let send_fut = async move {
            write
                .write_all_owned(
                    &b"POST /echo-body HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n"[..],
                )
                .await?;
            write.flush().await?;

            let chunks = ["a first chunk", "a second chunk", "a third and final chunk"];
            for chunk in chunks {
                let chunk_len = chunk.len();
                write
                    .write_all_owned(format!("{chunk_len:x}\r\n{chunk}\r\n").into_bytes())
                    .await?;
                write.flush().await?;
            }

            write.write_all_owned(&b"0\r\n\r\n"[..]).await?;
            write.flush().await?;

            Ok::<(), BX>(())
        };
        loona::buffet::spawn(async move {
            if let Err(e) = send_fut.await {
                panic!("Error sending request: {e}");
            }
        });

        debug!("Reading response...");
        'read_response: loop {
            buf.reserve(256);
            let n = read.read_buf(&mut buf).await?;
            if n == 0 {
                panic!("unexpected EOF");
            }
            debug!("After read, got {} bytes", buf.len());

            let mut headers = [EMPTY_HEADER; 16];
            let mut res = httparse::Response::new(&mut headers[..]);
            let body_offset = match res.parse(&buf[..]).bx()? {
                Status::Complete(off) => off,
                Status::Partial => continue 'read_response,
            };
            debug!("client got a response header");
            assert_eq!(res.code, Some(200));

            let mut chunked = false;
            for header in res.headers.iter() {
                debug!(
                    "Got header: {} = {:?}",
                    header.name,
                    std::str::from_utf8(header.value)
                );
                if header.name.eq_ignore_ascii_case("transfer-encoding") {
                    let value = std::str::from_utf8(header.value).unwrap();
                    if value.eq_ignore_ascii_case("chunked") {
                        chunked = true;
                    }
                }
            }
            assert!(chunked);

            let mut buf = buf.split_off(body_offset);
            let mut body: Vec<u8> = Vec::new();

            loop {
                let status = httparse::parse_chunk_size(&buf[..]).unwrap();
                match status {
                    Status::Complete((offset, chunk_len)) => {
                        debug!("Reading {chunk_len} chunk");
                        buf = buf.split_off(offset);

                        while buf.len() < chunk_len as usize {
                            debug!("buf has {} bytes, need {}", buf.len(), chunk_len);
                            if buf.capacity() == 0 {
                                buf.reserve(256);
                            }
                            let n = read.read_buf(&mut buf).await?;
                            if n == 0 {
                                panic!("unexpected EOF");
                            }
                            debug!("just read {n}");
                        }

                        body.extend_from_slice(&buf[..chunk_len as usize]);
                        buf = buf.split_off(chunk_len as usize);

                        while buf.len() < 2 {
                            if buf.capacity() == 0 {
                                buf.reserve(2);
                            }
                            let n = read.read_buf(&mut buf).await?;
                            if n == 0 {
                                panic!("unexpected EOF");
                            }
                        }

                        assert_eq!(&buf[..2], b"\r\n");
                        buf = buf.split_off(2);

                        if chunk_len == 0 {
                            debug!("Got the last chunk");
                            break;
                        }
                    }
                    Status::Partial => {
                        // partial chunk, reading more
                        buf.reserve(256);
                        let n = read.read_buf(&mut buf).await?;
                        if n == 0 {
                            panic!("unexpected EOF");
                        }
                    }
                }
            }

            debug!("Full response body: {:?}", body.hex_dump());
            let body = String::from_utf8(body)?;
            assert_eq!(body, "a first chunka second chunka third and final chunk");

            break 'read_response;
        }

        debug!("http client shutting down");
        Ok(())
    }

    helpers::run(async move {
        let (upstream_addr, _upstream_guard) = testbed::start().await?;
        let (ln_addr, guard, proxy_fut) = proxy::start(upstream_addr).await?;
        let client_fut = client(ln_addr, guard);

        tokio::try_join!(proxy_fut, client_fut)?;
        debug!("everything has been joined");

        Ok(())
    });
}

enum BodyType {
    ContentLen,
    Chunked,
}

#[test]
fn curl_echo_body_content_len() {
    curl_echo_body(BodyType::ContentLen)
}

#[test]
fn curl_echo_body_chunked() {
    curl_echo_body(BodyType::Chunked)
}

fn curl_echo_body(typ: BodyType) {
    #[allow(drop_bounds)]
    async fn client(typ: BodyType, ln_addr: SocketAddr, _guard: impl Drop) -> b_x::Result<()> {
        let req_body = "Please return to sender";
        let mut cmd = Command::new("curl");

        cmd.arg("--silent");
        cmd.arg("--fail-with-body");
        cmd.arg(format!("http://{ln_addr}/echo-body"));
        cmd.arg("--data").arg(req_body);
        cmd.arg("--header")
            .arg("content-type: application/octet-stream");

        if let BodyType::ContentLen = typ {
            cmd.arg("--header")
                .arg(format!("content-length: {}", req_body.len()));
        } else {
            cmd.arg("--header").arg("transfer-encoding: chunked");
        }

        let output = cmd.output_assert_success().await;
        let res_body = output.stdout;

        debug!("Got body: {:?}", res_body.hex_dump());
        assert_eq!(res_body.len(), req_body.len());
        assert_eq!(res_body, req_body.as_bytes());

        Ok(())
    }

    helpers::run(async move {
        let (upstream_addr, _upstream_guard) = testbed::start().await?;
        let (ln_addr, guard, proxy_fut) = proxy::start(upstream_addr).await?;
        let client_fut = client(typ, ln_addr, guard);
        tokio::try_join!(proxy_fut, client_fut)?;
        debug!("everything has been joined");

        Ok(())
    });
}

#[test]
fn curl_echo_body_noproxy_content_len() {
    curl_echo_body_noproxy(BodyType::ContentLen)
}

#[test]
fn curl_echo_body_noproxy_chunked() {
    curl_echo_body_noproxy(BodyType::Chunked)
}

fn curl_echo_body_noproxy(typ: BodyType) {
    #[allow(drop_bounds)]
    async fn client(typ: BodyType, ln_addr: SocketAddr, _guard: impl Drop) -> b_x::Result<()> {
        let req_body = "Please return to sender";
        let mut cmd = tokio::process::Command::new("curl");

        cmd.arg("--silent");
        cmd.arg("--fail-with-body");
        cmd.arg(format!("http://{ln_addr}/echo-body"));
        cmd.arg("--data").arg(req_body);
        cmd.arg("--header")
            .arg("content-type: application/octet-stream");

        if let BodyType::ContentLen = typ {
            cmd.arg("--header")
                .arg(format!("content-length: {}", req_body.len()));
        } else {
            cmd.arg("--header").arg("transfer-encoding: chunked");
        }

        let output = cmd.output_assert_success().await;
        let res_body = output.stdout;

        debug!("Got body: {:?}", res_body.hex_dump());
        assert_eq!(res_body.len(), req_body.len());
        assert_eq!(res_body, req_body.as_bytes());

        Ok(())
    }

    async fn start_server(
    ) -> b_x::Result<(SocketAddr, impl Drop, impl Future<Output = b_x::Result<()>>)> {
        let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();

        let ln = loona::buffet::net::TcpListener::bind("127.0.0.1:0".parse()?).await?;
        let ln_addr = ln.local_addr()?;

        struct TestDriver;

        impl<OurEncoder> ServerDriver<OurEncoder> for TestDriver
        where
            OurEncoder: Encoder,
        {
            type Error = BX;

            async fn handle(
                &self,
                req: Request,
                req_body: &mut impl Body,
                mut respond: Responder<OurEncoder, ExpectResponseHeaders>,
            ) -> b_x::Result<Responder<OurEncoder, ResponseDone>> {
                if req.headers.expects_100_continue() {
                    debug!("Sending 100-continue");
                    let res = Response {
                        status: StatusCode::CONTINUE,
                        ..Default::default()
                    };
                    respond.write_interim_response(res).await?;
                }

                debug!("Writing final response");
                let res = Response {
                    status: StatusCode::OK,
                    headers: {
                        let mut headers = Headers::default();
                        headers.insert(header::SERVER, "integration-test/1.0".into());
                        headers
                    },
                    ..Default::default()
                };
                let respond = respond
                    .write_final_response_with_body(res, req_body)
                    .await
                    .bx()?;

                debug!("Wrote final response");
                Ok(respond)
            }
        }

        let server_fut = async move {
            let conf = Rc::new(h1::ServerConf::default());

            enum Event {
                Accepted((loona::buffet::net::TcpStream, SocketAddr)),
                ShuttingDown,
            }

            loop {
                let ev = tokio::select! {
                    accept_res = ln.accept() => {
                        Event::Accepted(accept_res?)
                    },
                    _ = &mut rx => {
                        Event::ShuttingDown
                    }
                };

                match ev {
                    Event::Accepted((transport, remote_addr)) => {
                        debug!("Accepted connection from {remote_addr}");

                        let conf = conf.clone();

                        loona::buffet::spawn(async move {
                            let driver = TestDriver;
                            h1::serve(
                                transport.into_halves(),
                                conf,
                                RollMut::alloc().unwrap(),
                                driver,
                            )
                            .await
                            .unwrap();
                            debug!("Done serving h1 connection");
                        });
                    }
                    Event::ShuttingDown => {
                        debug!("Shutting down server");
                        break;
                    }
                }
            }

            debug!("Server shutting down.");

            Ok(())
        };

        Ok((ln_addr, tx, server_fut))
    }

    helpers::run(async move {
        let (ln_addr, guard, server_fut) = start_server().await?;
        let client_fut = client(typ, ln_addr, guard);

        tokio::try_join!(server_fut, client_fut)?;
        debug!("everything has been joined");

        Ok(())
    });
}

#[test]
fn h2_basic_post() {
    #[allow(drop_bounds)]
    async fn client(ln_addr: SocketAddr, _guard: impl Drop) -> b_x::Result<()> {
        let req_body = "Please return to sender";
        let mut cmd = tokio::process::Command::new("curl");

        cmd.arg("--silent");
        cmd.arg("--fail-with-body");
        cmd.arg("--http2-prior-knowledge");
        cmd.arg(format!("http://{ln_addr}/echo-body"));
        cmd.arg("--data").arg(req_body);
        cmd.arg("--header")
            .arg("content-type: application/octet-stream");

        let output = cmd.output_assert_success().await;
        let res_body = output.stdout;

        debug!("Got body: {:?}", res_body.hex_dump());
        assert_eq!(res_body.len(), req_body.len());
        assert_eq!(res_body, req_body.as_bytes());

        Ok(())
    }

    async fn start_server(
    ) -> b_x::Result<(SocketAddr, impl Drop, impl Future<Output = b_x::Result<()>>)> {
        let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();

        let listen_port: u16 = std::env::var("LISTEN_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();
        let ln = loona::buffet::net::TcpListener::bind(format!("127.0.0.1:{listen_port}").parse()?)
            .await?;
        let ln_addr = ln.local_addr()?;

        struct TestDriver;

        impl<OurEncoder> ServerDriver<OurEncoder> for TestDriver
        where
            OurEncoder: Encoder,
        {
            type Error = BX;

            async fn handle(
                &self,
                req: Request,
                req_body: &mut impl Body,
                respond: Responder<OurEncoder, ExpectResponseHeaders>,
            ) -> b_x::Result<Responder<OurEncoder, ResponseDone>> {
                debug!("Got request {req:#?}");

                debug!("Writing final response");
                let res = Response {
                    status: StatusCode::OK,
                    headers: {
                        let mut headers = Headers::default();
                        headers.insert(header::SERVER, "integration-test/1.0".into());
                        headers
                    },
                    ..Default::default()
                };
                let respond = respond
                    .write_final_response_with_body(res, req_body)
                    .await
                    .bx()?;

                debug!("Wrote final response");
                Ok(respond)
            }
        }

        let driver = Rc::new(TestDriver);

        let server_fut = async move {
            let conf = Rc::new(h2::ServerConf::default());

            enum Event {
                Accepted((loona::buffet::net::TcpStream, SocketAddr)),
                ShuttingDown,
            }

            loop {
                let ev = tokio::select! {
                    accept_res = ln.accept() => {
                        Event::Accepted(accept_res?)
                    },
                    _ = &mut rx => {
                        Event::ShuttingDown
                    }
                };

                match ev {
                    Event::Accepted((transport, remote_addr)) => {
                        debug!("Accepted connection from {remote_addr}");

                        let conf = conf.clone();
                        let driver = driver.clone();

                        loona::buffet::spawn(async move {
                            h2::serve(
                                transport.into_halves(),
                                conf,
                                RollMut::alloc().unwrap(),
                                driver,
                            )
                            .await
                            .unwrap();
                            debug!("Done serving h1 connection");
                        });
                    }
                    Event::ShuttingDown => {
                        debug!("Shutting down server");
                        break;
                    }
                }
            }

            debug!("Server shutting down.");

            Ok(())
        };

        Ok((ln_addr, tx, server_fut))
    }

    helpers::run(async move {
        let (ln_addr, guard, server_fut) = start_server().await?;
        let client_fut = client(ln_addr, guard);

        tokio::try_join!(server_fut, client_fut)?;
        debug!("everything has been joined");

        Ok(())
    });
}

#[derive(Debug)]
struct SampleBody {
    chunks_remain: u64,
}

impl Default for SampleBody {
    fn default() -> Self {
        Self { chunks_remain: 128 }
    }
}

impl Body for SampleBody {
    type Error = BX;

    fn content_len(&self) -> Option<u64> {
        None
    }

    fn eof(&self) -> bool {
        self.chunks_remain == 0
    }

    async fn next_chunk(&mut self) -> b_x::Result<BodyChunk> {
        let c = match self.chunks_remain {
            0 => BodyChunk::Done { trailers: None },
            _ => BodyChunk::Chunk(
                PieceCore::Vec(Rc::new(b"this is a big chunk".to_vec().repeat(256))).into(),
            ),
        };

        if let Some(remain) = self.chunks_remain.checked_sub(1) {
            self.chunks_remain = remain;
        }
        Ok(c)
    }
}

// TODO: dedup with h2_basic_post
#[test]
fn h2_basic_get() {
    #[allow(drop_bounds)]
    async fn client(ln_addr: SocketAddr, _guard: impl Drop) -> b_x::Result<()> {
        let mut cmd = tokio::process::Command::new("curl");

        cmd.arg("--silent");
        cmd.arg("--fail-with-body");
        cmd.arg("--http2-prior-knowledge");
        cmd.arg(format!("http://{ln_addr}/stream-big-body"));

        let output = cmd.output_assert_success().await;
        let res_body = output.stdout;

        let ref_body = "this is a big chunk".repeat(256).repeat(128);
        assert_eq!(res_body.len(), ref_body.len());
        assert_eq!(String::from_utf8(res_body).unwrap(), ref_body);

        Ok(())
    }

    async fn start_server(
    ) -> b_x::Result<(SocketAddr, impl Drop, impl Future<Output = b_x::Result<()>>)> {
        let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();

        let listen_port: u16 = std::env::var("LISTEN_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();
        let ln = loona::buffet::net::TcpListener::bind(format!("127.0.0.1:{listen_port}").parse()?)
            .await?;
        let ln_addr = ln.local_addr()?;

        struct TestDriver;

        impl<OurEncoder> ServerDriver<OurEncoder> for TestDriver
        where
            OurEncoder: Encoder,
        {
            type Error = BX;

            async fn handle(
                &self,
                req: Request,
                _req_body: &mut impl Body,
                respond: Responder<OurEncoder, ExpectResponseHeaders>,
            ) -> b_x::Result<Responder<OurEncoder, ResponseDone>> {
                debug!("Got request {req:#?}");

                debug!("Writing final response");
                let res = Response {
                    status: StatusCode::OK,
                    headers: {
                        let mut headers = Headers::default();
                        headers.insert(header::SERVER, "integration-test/1.0".into());
                        headers
                    },
                    ..Default::default()
                };
                let respond = respond
                    .write_final_response_with_body(res, &mut SampleBody::default())
                    .await
                    .bx()?;

                debug!("Wrote final response");
                Ok(respond)
            }
        }

        let driver = Rc::new(TestDriver);

        let server_fut = async move {
            let conf = Rc::new(h2::ServerConf::default());

            enum Event {
                Accepted((loona::buffet::net::TcpStream, SocketAddr)),
                ShuttingDown,
            }

            loop {
                let ev = tokio::select! {
                    accept_res = ln.accept() => {
                        Event::Accepted(accept_res?)
                    },
                    _ = &mut rx => {
                        Event::ShuttingDown
                    }
                };

                match ev {
                    Event::Accepted((transport, remote_addr)) => {
                        debug!("Accepted connection from {remote_addr}");

                        let conf = conf.clone();
                        let driver = driver.clone();

                        loona::buffet::spawn(async move {
                            h2::serve(
                                transport.into_halves(),
                                conf,
                                RollMut::alloc().unwrap(),
                                driver,
                            )
                            .await
                            .unwrap();
                            debug!("Done serving h1 connection");
                        });
                    }
                    Event::ShuttingDown => {
                        debug!("Shutting down server");
                        break;
                    }
                }
            }

            debug!("Server shutting down.");

            Ok(())
        };

        Ok((ln_addr, tx, server_fut))
    }

    helpers::run(async move {
        let (ln_addr, guard, server_fut) = start_server().await?;
        let client_fut = client(ln_addr, guard);

        tokio::try_join!(server_fut, client_fut)?;
        debug!("everything has been joined");

        Ok(())
    });
}

trait CommandExt {
    async fn output_assert_success(&mut self) -> std::process::Output;
}

impl CommandExt for Command {
    async fn output_assert_success(&mut self) -> std::process::Output {
        let output = self.output().await.unwrap();
        if !output.status.success() {
            // print stderr
            eprintln!("command failed with status: {:?}", output.status);
            eprintln!("stderr: {:?}", String::from_utf8_lossy(&output.stderr));
            panic!("command failed with status: {:?} {:?}", output.status, self);
        }
        output
    }
}
