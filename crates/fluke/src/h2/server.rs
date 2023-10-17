use std::rc::Rc;

use futures_util::TryFutureExt;
use tracing::debug;

use crate::{
    h2::{
        parse::{self, Frame, FrameType, StreamId},
        read::H2ReadContext,
        types::{ConnState, ConnectionClosed, H2ConnEvent},
    },
    util::read_and_parse,
    ServerDriver,
};
use fluke_buffet::RollMut;
use fluke_maybe_uring::io::{ReadOwned, WriteOwned};

/// HTTP/2 server configuration
pub struct ServerConf {
    pub max_streams: u32,
}

impl Default for ServerConf {
    fn default() -> Self {
        Self { max_streams: 32 }
    }
}

pub async fn serve(
    (mut transport_r, mut transport_w): (impl ReadOwned, impl WriteOwned),
    conf: Rc<ServerConf>,
    mut client_buf: RollMut,
    driver: Rc<impl ServerDriver + 'static>,
) -> eyre::Result<()> {
    let mut state = ConnState::default();
    state.self_settings.max_concurrent_streams = conf.max_streams;

    (client_buf, _) = match read_and_parse(
        parse::preface,
        &mut transport_r,
        client_buf,
        parse::PREFACE.len(),
    )
    .await?
    {
        Some((client_buf, frame)) => (client_buf, frame),
        None => {
            debug!("h2 client closed connection before sending preface");
            return Ok(());
        }
    };
    debug!("read preface");

    let mut out_scratch = RollMut::alloc()?;

    // we have to send a settings frame
    {
        let payload_roll = state.self_settings.into_roll(&mut out_scratch)?;
        let frame_roll = Frame::new(
            FrameType::Settings(Default::default()),
            StreamId::CONNECTION,
        )
        .with_len(payload_roll.len().try_into().unwrap())
        .into_roll(&mut out_scratch)?;

        transport_w
            .writev_all(vec![frame_roll, payload_roll])
            .await?;
        debug!("sent settings frame");
    }

    let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<H2ConnEvent>(32);

    let mut h2_read_cx = H2ReadContext::new(driver.clone(), ev_tx.clone(), state);
    let read_task = h2_read_cx.read_loop(client_buf, transport_r);

    let write_task = super::write::h2_write_loop(ev_rx, transport_w, out_scratch);

    let res = tokio::try_join!(
        read_task.map_err(LoopError::Read),
        write_task.map_err(LoopError::Write),
    );
    if let Err(e) = &res {
        if let LoopError::Read(r) = e {
            if r.downcast_ref::<ConnectionClosed>().is_some() {
                return Ok(());
            }
        }
        debug!("caught error from one of the tasks: {e} / {e:#?}");
    }
    res?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
enum LoopError {
    #[error("read error: {0}")]
    Read(eyre::Report),

    #[error("write error: {0}")]
    Write(eyre::Report),
}