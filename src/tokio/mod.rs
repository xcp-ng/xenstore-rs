//! Tokio async implementation.
//!
//! Alike Unix implementation, uses either xenstored socket or xenbus/xenstore device.
//!
//! This implementation uses a underlying task to multiplex the concurrent
//! accesses and manage watchers. If this underlying task dies (e.g dead xenstore socket),
//! all future operations will fail with [io::ErrorKind::BrokenPipe] and all watchers
//! will yield [None].

mod device;

use std::{env, io};

use futures::task::Spawn;
use log::{debug, error};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::UnixStream,
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::{wire::XsMessage, xs_async::XsAsyncImpl};

/// Tokio Xenstore implementation.
///
/// It can be cloned and used concurrently by multiple tasks.
#[derive(Clone, Debug)]
pub struct XsTokio;

struct TokioSpawner;

impl Spawn for TokioSpawner {
    fn spawn_obj(
        &self,
        future: futures::task::FutureObj<'static, ()>,
    ) -> Result<(), futures::task::SpawnError> {
        tokio::task::spawn(future);
        Ok(())
    }
}

impl XsTokio {
    /// Try to open Xenstore interface.
    /// Attempt in order :
    ///  - `/run/xenstored/socket` (unix domain socket)
    ///  - [crate::wire::XENBUS_DEVICE_PATH] (xenstore device)
    pub async fn new() -> io::Result<XsAsyncImpl> {
        let xsd_path =
            env::var("XENSTORED_PATH").unwrap_or_else(|_| "/run/xenstored/socket".to_string());

        // Use xenstored socket first
        if let Ok(stream) = UnixStream::connect(xsd_path).await {
            return Ok(launch_xenstore_task(stream)?);
        }

        Ok(launch_xenstore_task(device::XsDevice::new().await?)?)
    }
}

pub fn launch_xenstore_task<S>(xs_stream: S) -> io::Result<XsAsyncImpl>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (rx, tx) = tokio::io::split(xs_stream);
    let (response_tx, xs_receiver) = flume::bounded(4);
    let (xs_sender, request_rx) = flume::bounded(4);

    // Message receiver task
    tokio::spawn(async move {
        let mut rx = rx.compat();
        while let Ok(message) = XsMessage::read_message_async(&mut rx).await {
            debug!("< {message:?}");

            if response_tx.send_async(message).await.is_err() {
                break;
            }
        }

        error!("Read message failure");
    });

    // Message sender task
    tokio::spawn(async move {
        let mut tx = tx.compat_write();
        while let Ok(message) = request_rx.recv_async().await {
            debug!("> {message:?}");

            if let Err(e) = XsMessage::write_message_async(&message, &mut tx).await {
                error!("Write message failure {e}");
                break;
            }
        }
    });

    XsAsyncImpl::new(TokioSpawner, xs_receiver, xs_sender)
}
