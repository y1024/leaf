use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::{io, pin::Pin};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::stream::Stream;
use futures::FutureExt;
use futures::{
    task::{Context, Poll},
    Future,
};

use crate::{proxy::*, session::Session};

use super::QuicProxyStream;

struct Incoming {
    endpoint: quinn::Endpoint,
    connectings: Vec<quinn::Connecting>,
    conns: Vec<quinn::Connection>,
    incoming_closed: bool,
}

impl Incoming {
    pub fn new(endpoint: quinn::Endpoint) -> Self {
        Incoming {
            endpoint,
            connectings: Vec::new(),
            conns: Vec::new(),
            incoming_closed: false,
        }
    }
}

impl Stream for Incoming {
    type Item = AnyBaseInboundTransport;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.incoming_closed {
            let mut connectings = Vec::new();
            let mut incoming_closed = false;
            loop {
                match self.endpoint.accept().boxed().poll_unpin(cx) {
                    Poll::Ready(Some(connecting)) => {
                        connectings.push(connecting);
                    }
                    Poll::Ready(None) => {
                        incoming_closed = true;
                        break;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
            self.incoming_closed = incoming_closed;
            self.connectings.append(&mut connectings);
        }

        let mut conns = Vec::new();
        let mut completed = Vec::new();
        for (idx, connecting) in self.connectings.iter_mut().enumerate() {
            match Pin::new(connecting).poll(cx) {
                Poll::Ready(Ok(conn)) => {
                    conns.push(conn);
                    completed.push(idx);
                }
                Poll::Ready(Err(e)) => {
                    log::debug!("QUIC connect failed: {}", e);
                    completed.push(idx);
                }
                Poll::Pending => (),
            }
        }
        if !conns.is_empty() {
            self.conns.append(&mut conns);
        }

        #[allow(unused_must_use)]
        for idx in completed.iter().rev() {
            self.connectings.swap_remove(*idx);
        }

        let mut stream: Option<Self::Item> = None;
        let mut completed = Vec::new();
        for (idx, conn) in self.conns.iter_mut().enumerate() {
            match conn.accept_bi().boxed().poll_unpin(cx) {
                Poll::Ready(Ok((send, recv))) => {
                    let mut sess = Session {
                        source: conn.remote_address(),
                        ..Default::default()
                    };
                    sess.stream_id = Some(send.id().index());
                    stream.replace(AnyBaseInboundTransport::Stream(
                        Box::new(QuicProxyStream { recv, send }),
                        sess,
                    ));
                    break;
                }
                Poll::Ready(Err(e)) => {
                    log::debug!("new quic bidirectional stream failed: {}", e);
                    completed.push(idx);
                }
                Poll::Pending => (),
            }
        }
        for idx in completed.iter().rev() {
            self.conns.remove(*idx);
        }

        if let Some(stream) = stream.take() {
            Poll::Ready(Some(stream))
        } else if self.incoming_closed && self.connectings.is_empty() && self.conns.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

fn quic_err<E>(error: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, error)
}

pub struct Handler {
    server_config: quinn::ServerConfig,
}

impl Handler {
    pub fn new(certificate: String, certificate_key: String, alpns: Vec<String>) -> Result<Self> {
        let (cert, key) =
            fs::read(&certificate).and_then(|x| Ok((x, fs::read(&certificate_key)?)))?;

        let cert = match Path::new(&certificate).extension().map(|ext| ext.to_str()) {
            Some(Some(ext)) if ext == "der" => {
                vec![rustls::Certificate(cert)]
            }
            _ => rustls_pemfile::certs(&mut &*cert)?
                .into_iter()
                .map(rustls::Certificate)
                .collect(),
        };

        let key = match Path::new(&certificate_key)
            .extension()
            .map(|ext| ext.to_str())
        {
            Some(Some(ext)) if ext == "der" => rustls::PrivateKey(key),
            _ => {
                let pkcs8 = rustls_pemfile::pkcs8_private_keys(&mut &*key)?;
                match pkcs8.into_iter().next() {
                    Some(x) => rustls::PrivateKey(x),
                    None => {
                        let rsa = rustls_pemfile::rsa_private_keys(&mut &*key)?;
                        match rsa.into_iter().next() {
                            Some(x) => rustls::PrivateKey(x),
                            None => {
                                let rsa = rustls_pemfile::ec_private_keys(&mut &*key)?;
                                match rsa.into_iter().next() {
                                    Some(x) => rustls::PrivateKey(x),
                                    None => {
                                        return Err(anyhow!("no private keys found",));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        let mut crypto = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert, key)?;

        for alpn in alpns {
            crypto.alpn_protocols.push(alpn.as_bytes().to_vec());
        }

        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_concurrent_bidi_streams(quinn::VarInt::from_u32(64));
        transport_config.max_idle_timeout(Some(quinn::IdleTimeout::from(quinn::VarInt::from_u32(
            300_000,
        ))));
        transport_config
            .congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        server_config.transport_config(Arc::new(transport_config));

        Ok(Self { server_config })
    }
}

#[async_trait]
impl InboundDatagramHandler for Handler {
    async fn handle<'a>(&'a self, socket: AnyInboundDatagram) -> io::Result<AnyInboundTransport> {
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(self.server_config.clone()),
            socket.into_std()?,
            quinn::TokioRuntime,
        )
        .map_err(quic_err)?;
        Ok(InboundTransport::Incoming(Box::new(Incoming::new(
            endpoint,
        ))))
    }
}
