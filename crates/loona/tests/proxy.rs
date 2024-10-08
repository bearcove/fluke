use b_x::{BxForResults, BX};
use http::StatusCode;
use loona::{
    buffet::{
        net::{TcpReadHalf, TcpWriteHalf},
        IntoHalves, RollMut,
    },
    h1, Body, BodyChunk, Encoder, ExpectResponseHeaders, HeadersExt, Responder, Response,
    ResponseDone, ServerDriver,
};
use std::{cell::RefCell, future::Future, net::SocketAddr, rc::Rc};
use tracing::debug;

pub type TransportPool = Rc<RefCell<Vec<(TcpReadHalf, TcpWriteHalf)>>>;

pub struct ProxyDriver {
    pub upstream_addr: SocketAddr,
    pub pool: TransportPool,
}

impl<OurEncoder> ServerDriver<OurEncoder> for ProxyDriver
where
    OurEncoder: Encoder,
{
    type Error = BX;

    async fn handle(
        &self,
        req: loona::Request,
        req_body: &mut impl Body,
        mut respond: Responder<OurEncoder, ExpectResponseHeaders>,
    ) -> Result<Responder<OurEncoder, ResponseDone>, BX> {
        if req.headers.expects_100_continue() {
            debug!("Sending 100-continue");
            let res = Response {
                status: StatusCode::CONTINUE,
                ..Default::default()
            };
            respond.write_interim_response(res).await?;
        }

        let transport = {
            let mut pool = self.pool.borrow_mut();
            pool.pop()
        };

        let transport = if let Some(transport) = transport {
            debug!("re-using existing transport!");
            transport
        } else {
            debug!("making new connection to upstream!");
            loona::buffet::net::TcpStream::connect(self.upstream_addr)
                .await?
                .into_halves()
        };

        let driver = ProxyClientDriver { respond };

        let (transport, res) = h1::request(transport, req, req_body, driver).await?;

        if let Some(transport) = transport {
            let mut pool = self.pool.borrow_mut();
            // FIXME: leaky abstraction, `h1::request` returns both halves of the
            // transport, which are both actually `Rc<TcpStream>`
            pool.push(transport);
        }

        Ok(res)
    }
}

struct ProxyClientDriver<OurEncoder>
where
    OurEncoder: Encoder,
{
    respond: Responder<OurEncoder, ExpectResponseHeaders>,
}

impl<OurEncoder> h1::ClientDriver for ProxyClientDriver<OurEncoder>
where
    OurEncoder: Encoder,
{
    type Return = Responder<OurEncoder, ResponseDone>;
    type Error = BX;

    async fn on_informational_response(&mut self, res: Response) -> Result<(), Self::Error> {
        debug!("Got informational response {}", res.status);
        Ok(())
    }

    async fn on_final_response(
        self,
        res: Response,
        body: &mut impl Body,
    ) -> Result<Self::Return, Self::Error> {
        let respond = self.respond;
        let mut respond = respond.write_final_response(res).await?;

        let trailers = loop {
            match body.next_chunk().await.bx()? {
                BodyChunk::Chunk(chunk) => {
                    respond.write_chunk(chunk).await?;
                }
                BodyChunk::Done { trailers } => {
                    // should we do something here in case of
                    // content-length mismatches or something?
                    break trailers;
                }
            }
        };

        let respond = respond.finish_body(trailers).await?;

        Ok(respond)
    }
}

pub async fn start(
    upstream_addr: SocketAddr,
) -> b_x::Result<(SocketAddr, impl Drop, impl Future<Output = b_x::Result<()>>)> {
    let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();

    let ln = loona::buffet::net::TcpListener::bind("127.0.0.1:0".parse()?).await?;
    let ln_addr = ln.local_addr()?;

    let proxy_fut = async move {
        let conf = Rc::new(h1::ServerConf::default());
        let pool: TransportPool = Default::default();

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

                    let pool = pool.clone();
                    let conf = conf.clone();

                    loona::buffet::spawn(async move {
                        let driver = ProxyDriver {
                            upstream_addr,
                            pool,
                        };
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
                    debug!("Shutting down proxy");
                    break;
                }
            }
        }

        debug!("Proxy server shutting down.");
        drop(pool);

        Ok(())
    };

    Ok((ln_addr, tx, proxy_fut))
}
