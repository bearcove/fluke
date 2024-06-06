use std::rc::Rc;

use color_eyre::eyre;
use fluke::{
    http::{self, StatusCode},
    Body, BodyChunk, Encoder, ExpectResponseHeaders, Responder, Response, ResponseDone,
};
use fluke_buffet::{IntoHalves, RollMut};
use tracing::Level;
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
    setup_tracing_and_error_reporting();

    fluke_buffet::start(async move {
        let ln = fluke_buffet::net::TcpListener::bind("127.0.0.1:8000".parse().unwrap())
            .await
            .unwrap();

        println!(
            "Listening on {:?} for 'http2 prior knowledge' connections (no TLS)",
            ln.local_addr().unwrap()
        );

        loop {
            let (stream, addr) = ln.accept().await.unwrap();
            tracing::info!(?addr, "Accepted connection");

            fluke_buffet::spawn(async move {
                let server_conf = Rc::new(fluke::h2::ServerConf {
                    ..Default::default()
                });

                let client_buf = RollMut::alloc().unwrap();
                let driver = Rc::new(TestDriver);
                let io = stream.into_halves();
                fluke::h2::serve(io, server_conf, client_buf, driver)
                    .await
                    .unwrap();
                tracing::debug!("http/2 server done");
            });
        }
    });
}

fn setup_tracing_and_error_reporting() {
    color_eyre::install().unwrap();

    let targets = if let Ok(rust_log) = std::env::var("RUST_LOG") {
        rust_log.parse::<Targets>().unwrap()
    } else {
        Targets::new()
            .with_default(Level::INFO)
            .with_target("fluke", Level::DEBUG)
            .with_target("httpwg", Level::DEBUG)
            .with_target("want", Level::INFO)
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_file(false)
        .with_line_number(false)
        .without_time();

    tracing_subscriber::registry()
        .with(targets)
        .with(fmt_layer)
        .init();
}

struct TestDriver;

impl fluke::ServerDriver for TestDriver {
    async fn handle<E: Encoder>(
        &self,
        _req: fluke::Request,
        req_body: &mut impl Body,
        mut res: Responder<E, ExpectResponseHeaders>,
    ) -> eyre::Result<Responder<E, ResponseDone>> {
        // if the client sent `expect: 100-continue`, we must send a 100 status code
        if let Some(h) = _req.headers.get(http::header::EXPECT) {
            if &h[..] == b"100-continue" {
                res.write_interim_response(Response {
                    status: StatusCode::CONTINUE,
                    ..Default::default()
                })
                .await?;
            }
        }

        // then read the full request body
        let mut req_body_len = 0;
        loop {
            let chunk = req_body.next_chunk().await?;
            match chunk {
                BodyChunk::Done { trailers } => {
                    // yey
                    if let Some(trailers) = trailers {
                        tracing::debug!(trailers_len = %trailers.len(), "received trailers");
                    }
                    break;
                }
                BodyChunk::Chunk(chunk) => {
                    req_body_len += chunk.len();
                }
            }
        }
        tracing::debug!(%req_body_len, "read request body");

        tracing::trace!("writing final response");
        let mut res = res
            .write_final_response(Response {
                status: StatusCode::OK,
                ..Default::default()
            })
            .await?;

        tracing::trace!("writing body chunk");
        res.write_chunk("it's less dire to lose, than to lose oneself".into())
            .await?;

        tracing::trace!("finishing body (with no trailers)");
        let res = res.finish_body(None).await?;

        tracing::trace!("we're done");
        Ok(res)
    }
}
