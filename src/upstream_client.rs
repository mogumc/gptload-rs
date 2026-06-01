use base64::{engine::general_purpose, Engine as _};
use hyper::client::connect::{Connected, Connection};
use hyper::client::HttpConnector;
use hyper::service::Service;
use hyper::{Body, Client, Request, Response, Uri};
use hyper_proxy::{Intercept, Proxy, ProxyConnector};
use hyper_rustls::HttpsConnectorBuilder;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_socks::tcp::Socks5Stream;

type DirectConnector = hyper_rustls::HttpsConnector<HttpConnector>;
type HttpProxyConnector = ProxyConnector<HttpConnector>;
type SocksHttpsConnector = hyper_rustls::HttpsConnector<SocksConnector>;

#[derive(Clone)]
pub enum UpstreamClient {
    Direct(Client<DirectConnector, Body>),
    HttpProxy(Client<HttpProxyConnector, Body>),
    Socks5(Client<SocksHttpsConnector, Body>),
}

impl UpstreamClient {
    pub fn new(proxy: Option<&str>) -> anyhow::Result<Self> {
        match proxy {
            Some(proxy) if proxy.starts_with("http://") || proxy.starts_with("https://") => {
                Ok(Self::HttpProxy(build_http_proxy_client(proxy)?))
            }
            Some(proxy) if proxy.starts_with("socks5://") => {
                Ok(Self::Socks5(build_socks5_client(proxy)?))
            }
            Some(proxy) => anyhow::bail!("unsupported proxy scheme: {proxy}"),
            None => Ok(Self::Direct(build_direct_client())),
        }
    }

    pub async fn request(&self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        match self {
            Self::Direct(client) => client.request(req).await,
            Self::HttpProxy(client) => client.request(req).await,
            Self::Socks5(client) => client.request(req).await,
        }
    }
}

fn build_direct_client() -> Client<DirectConnector, Body> {
    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .build();

    Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(64)
        .build::<_, Body>(https)
}

fn build_http_proxy_client(proxy_url: &str) -> anyhow::Result<Client<HttpProxyConnector, Body>> {
    let parsed = ParsedProxy::parse(proxy_url)?;
    let proxy_uri: Uri = parsed.proxy_uri.parse()?;
    let mut proxy = Proxy::new(Intercept::All, proxy_uri);
    proxy.force_connect();
    if let Some((username, password)) = parsed.auth {
        let raw = format!("{username}:{password}");
        let encoded = general_purpose::STANDARD.encode(raw.as_bytes());
        let value = http::HeaderValue::from_str(&format!("Basic {encoded}"))?;
        proxy.set_header(http::header::PROXY_AUTHORIZATION, value);
    }

    let mut http = HttpConnector::new();
    http.enforce_http(false);
    let connector = ProxyConnector::from_proxy(http, proxy)?;

    Ok(Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(64)
        .build::<_, Body>(connector))
}

fn build_socks5_client(proxy_url: &str) -> anyhow::Result<Client<SocksHttpsConnector, Body>> {
    let parsed = ParsedProxy::parse(proxy_url)?;
    let connector = SocksConnector {
        proxy_addr: parsed.proxy_addr,
        auth: parsed.auth,
    };
    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .wrap_connector(connector);

    Ok(Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(64)
        .build::<_, Body>(https))
}

struct ParsedProxy {
    proxy_uri: String,
    proxy_addr: String,
    auth: Option<(String, String)>,
}

impl ParsedProxy {
    fn parse(input: &str) -> anyhow::Result<Self> {
        let (scheme, rest) = input
            .split_once("://")
            .ok_or_else(|| anyhow::anyhow!("proxy URL missing scheme"))?;
        let (authority, suffix) = match rest.split_once('/') {
            Some((a, s)) => (a, format!("/{s}")),
            None => (rest, String::new()),
        };
        let (auth, host_port) = match authority.rsplit_once('@') {
            Some((userinfo, host_port)) => {
                let (username, password) = userinfo
                    .split_once(':')
                    .map(|(u, p)| (percent_decode(u), percent_decode(p)))
                    .unwrap_or_else(|| (percent_decode(userinfo), String::new()));
                (Some((username, password)), host_port)
            }
            None => (None, authority),
        };
        if host_port.is_empty() {
            anyhow::bail!("proxy URL missing host");
        }
        let proxy_uri = format!("{scheme}://{host_port}{suffix}");
        Ok(Self {
            proxy_uri,
            proxy_addr: host_port.to_string(),
            auth,
        })
    }
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .to_string()
}

#[derive(Clone, Debug)]
pub(crate) struct SocksConnector {
    proxy_addr: String,
    auth: Option<(String, String)>,
}

impl Service<Uri> for SocksConnector {
    type Response = SocksIo;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<SocksIo, io::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let proxy_addr = self.proxy_addr.clone();
        let auth = self.auth.clone();
        Box::pin(async move {
            let host = dst
                .host()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing host"))?
                .to_string();
            let port = dst.port_u16().unwrap_or_else(|| {
                if dst.scheme() == Some(&http::uri::Scheme::HTTPS) {
                    443
                } else {
                    80
                }
            });
            let target = (host, port);
            let stream = match auth {
                Some((username, password)) => {
                    Socks5Stream::connect_with_password(
                        proxy_addr.as_str(),
                        target,
                        &username,
                        &password,
                    )
                    .await
                }
                None => Socks5Stream::connect(proxy_addr.as_str(), target).await,
            }
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok(SocksIo { inner: stream })
        })
    }
}

pub(crate) struct SocksIo {
    inner: Socks5Stream<TcpStream>,
}

impl AsyncRead for SocksIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for SocksIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Connection for SocksIo {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}
