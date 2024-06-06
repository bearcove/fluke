// TODO: remove me, this was only used for a video

use std::rc::Rc;

use fluke::{Body, BodyChunk, Encoder, ExpectResponseHeaders, Responder, Response, ResponseDone};
use fluke_buffet::{IntoHalves, PipeRead, PipeWrite, ReadOwned, RollMut, WriteOwned};
use http::StatusCode;
use tracing::Level;
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

/// Note: this will not work with `cargo test`, since it sets up process-level
/// globals. But it will work with `cargo nextest`, and that's what fluke is
/// standardizing on.
pub(crate) fn setup_tracing_and_error_reporting() {
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

        let mut res = res
            .write_final_response(Response {
                status: StatusCode::OK,
                ..Default::default()
            })
            .await?;

        res.write_chunk("it's less dire to lose, than to lose oneself".into())
            .await?;

        let res = res.finish_body(None).await?;

        Ok(res)
    }
}

pub struct TwoHalves<W, R>(W, R);
impl<W: WriteOwned + 'static, R: ReadOwned + 'static> IntoHalves for TwoHalves<W, R> {
    type Read = R;
    type Write = W;

    fn into_halves(self) -> (Self::Read, Self::Write) {
        (self.1, self.0)
    }
}

pub fn start_server() -> httpwg::Conn<TwoHalves<PipeWrite, PipeRead>> {
    let (server_write, client_read) = fluke::buffet::pipe();
    let (client_write, server_read) = fluke::buffet::pipe();

    let serve_fut = async move {
        let server_conf = Rc::new(fluke::h2::ServerConf {
            ..Default::default()
        });

        let client_buf = RollMut::alloc()?;
        let driver = Rc::new(TestDriver);
        let io = (server_read, server_write);
        fluke::h2::serve(io, server_conf, client_buf, driver).await?;
        tracing::debug!("http/2 server done");
        Ok::<_, eyre::Report>(())
    };

    fluke_buffet::spawn(async move {
        serve_fut.await.unwrap();
    });

    let config = Rc::new(httpwg::Config::default());
    httpwg::Conn::new(config, TwoHalves(client_write, client_read))
}

#[test]
fn rfc9113_3_starting_http2_sends_client_connection_preface() {
    crate::setup_tracing_and_error_reporting();

    fluke_buffet::start(async move {
        let conn = crate::start_server();

        #[rustfmt::skip]
        httpwg
            ::rfc9113
            ::_8_expressing_http_semantics_in_http2
            ::sends_headers_frame_without_path(conn)
            .await.unwrap();
    });
}
