use std::net::SocketAddr;
use tokio::net::ToSocketAddrs;

use crate::{Client, ClientOptions, NativeclickError, convert::UnitValue};

/// [`bb8`] connection manager for [`Client`]: plaintext TCP by default, TLS with
/// [`ConnectionManager::with_tls`].
#[derive(Clone)]
pub struct ConnectionManager {
    destination: Vec<SocketAddr>,
    options: ClientOptions,
    prequel: Option<String>,
    #[cfg(feature = "tls")]
    tls: Option<(
        rustls_pki_types::ServerName<'static>,
        tokio_rustls::TlsConnector,
    )>,
}

impl ConnectionManager {
    pub async fn new<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
    ) -> std::io::Result<Self> {
        Ok(Self {
            destination: tokio::net::lookup_host(destination).await?.collect(),
            options,
            prequel: None,
            #[cfg(feature = "tls")]
            tls: None,
        })
    }

    /// Connects over TLS (rustls), checking the server certificate against `name`.
    /// The connector carries the root certificates and the optional client certificate.
    #[cfg(feature = "tls")]
    pub fn with_tls(
        mut self,
        name: rustls_pki_types::ServerName<'static>,
        connector: tokio_rustls::TlsConnector,
    ) -> Self {
        self.tls = Some((name, connector));
        self
    }

    /// Runs `prequel` on every new connection, before handing it out.
    pub fn with_prequel(mut self, prequel: impl Into<String>) -> Self {
        self.prequel = Some(prequel.into());
        self
    }
}

impl bb8::ManageConnection for ConnectionManager {
    type Connection = Client;
    type Error = NativeclickError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        #[cfg(feature = "tls")]
        let client = match &self.tls {
            Some((name, connector)) => {
                Client::connect_tls(
                    &self.destination[..],
                    self.options.clone(),
                    name.clone(),
                    connector,
                )
                .await?
            }
            None => Client::connect(&self.destination[..], self.options.clone()).await?,
        };
        #[cfg(not(feature = "tls"))]
        let client = Client::connect(&self.destination[..], self.options.clone()).await?;
        if let Some(prequel) = &self.prequel {
            client.execute(prequel).await?;
        }
        Ok(client)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let _ = conn.query_one::<UnitValue<String>>("select '';").await?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed()
    }
}
