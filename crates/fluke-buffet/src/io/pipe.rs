use tokio::sync::mpsc;

use crate::{Piece, ReadOwned, WriteOwned};

/// Create a new pipe.
pub fn pipe() -> (PipeWrite, PipeRead) {
    let (tx, rx) = mpsc::channel(1);
    (
        PipeWrite { tx },
        PipeRead {
            rx,
            state: Default::default(),
            remain: None,
        },
    )
}

enum PipeEvent {
    Piece(Piece),
    Reset,
    // close is just dropping the channel
}

#[derive(Clone, Copy, Default)]
enum ReadState {
    #[default]
    Live,
    Reset,
    Eof,
}

pub struct PipeRead {
    rx: mpsc::Receiver<PipeEvent>,
    remain: Option<Piece>,
    state: ReadState,
}

impl ReadOwned for PipeRead {
    async fn read_owned<B: crate::IoBufMut>(&mut self, mut buf: B) -> crate::BufResult<usize, B> {
        loop {
            match self.state {
                ReadState::Live => {
                    // good, continue
                }
                ReadState::Reset => {
                    let err = std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "simulated connection reset",
                    );
                    return (Err(err), buf);
                }
                ReadState::Eof => return (Ok(0), buf),
            }

            if self.remain.is_none() {
                match self.rx.recv().await {
                    Some(PipeEvent::Piece(piece)) => {
                        assert!(!piece.is_empty());
                        self.remain = Some(piece);
                    }
                    Some(PipeEvent::Reset) => {
                        self.state = ReadState::Reset;
                        continue;
                    }
                    None => {
                        self.state = ReadState::Eof;
                        continue;
                    }
                }
            }

            let remain = self.remain.take().unwrap();
            let avail = buf.io_buf_mut_capacity();
            let read_size = avail.min(remain.len());
            let (copied, remain) = remain.split_at(read_size);
            assert_eq!(copied.len(), read_size);
            unsafe {
                buf.slice_mut()[..read_size].copy_from_slice(&copied[..]);
            }

            if !remain.is_empty() {
                self.remain = Some(remain);
            }
            return (Ok(read_size), buf);
        }
    }
}

pub struct PipeWrite {
    tx: mpsc::Sender<PipeEvent>,
}

impl PipeWrite {
    /// Simulate a connection reset
    pub async fn reset(self) {
        self.tx.send(PipeEvent::Reset).await.unwrap()
    }
}

impl WriteOwned for PipeWrite {
    async fn write_owned(&mut self, buf: impl Into<Piece>) -> crate::BufResult<usize, Piece> {
        let buf = buf.into();
        if buf.is_empty() {
            // ignore 0-length writes
        }

        if self.tx.send(PipeEvent::Piece(buf.clone())).await.is_err() {
            let err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "simulated broken pipe");
            return (Err(err), buf);
        }

        (Ok(buf.len()), buf)
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(all(test, not(feature = "miri")))]
mod tests {
    use crate::{ReadOwned, WriteOwned};

    use super::pipe;
    use std::{cell::RefCell, rc::Rc};

    #[test]
    fn test_pipe() {
        crate::start(async move {
            let (mut w, mut r) = pipe();
            let wrote_three = Rc::new(RefCell::new(false));

            crate::spawn({
                let wrote_three = wrote_three.clone();
                async move {
                    w.write_all_owned("one").await.unwrap();
                    w.write_all_owned("two").await.unwrap();
                    w.write_all_owned("three").await.unwrap();
                    *wrote_three.borrow_mut() = true;
                    w.write_all_owned("splitread").await.unwrap();
                }
            });

            {
                let buf = vec![0u8; 256];
                let (res, buf) = r.read_owned(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"one");
            }

            assert!(!*wrote_three.borrow());

            {
                let buf = vec![0u8; 256];
                let (res, buf) = r.read_owned(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"two");
            }

            tokio::task::yield_now().await;
            assert!(*wrote_three.borrow());

            {
                let buf = vec![0u8; 256];
                let (res, buf) = r.read_owned(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"three");
            }

            {
                let buf = vec![0u8; 5];
                let (res, buf) = r.read_owned(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"split");

                let buf = vec![0u8; 256];
                let (res, buf) = r.read_owned(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"read");
            }

            {
                let buf = vec![0u8; 0];
                let (res, _) = r.read_owned(buf).await;
                let n = res.unwrap();
                assert_eq!(n, 0, "reached EOF");
            }
        })
    }

    #[test]
    fn test_pipe_fragmented_read() {
        crate::start(async move {
            let (mut w, mut r) = pipe();

            crate::spawn({
                async move {
                    w.write_all_owned("two-part").await.unwrap();
                    w.reset().await;
                }
            });

            for _ in 0..5 {
                tokio::task::yield_now().await;
            }

            {
                let buf = vec![0u8; 4];
                let (res, buf) = r.read_owned(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"two-");
            }

            {
                let buf = vec![0u8; 4];
                let (res, buf) = r.read_owned(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"part");
            }

            {
                let buf = vec![0u8; 0];
                let (res, _) = r.read_owned(buf).await;
                let err = res.unwrap_err();
                assert_eq!(
                    err.kind(),
                    std::io::ErrorKind::ConnectionReset,
                    "reached EOF"
                );
            }
        })
    }
}
