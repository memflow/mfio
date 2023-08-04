use clap::Parser;
use futures::StreamExt;
#[cfg(unix)]
use mfio::backend::integrations::tokio::Tokio;
use mfio::backend::*;
use mfio_rt::{NativeRt, Tcp, TcpListenerHandle};
use std::net::SocketAddr;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:4446")]
    bind: SocketAddr,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let mut fs = NativeRt::default();

    fs.block_on(async {
        let listener = fs.bind(args.bind).await?;
        mfio_netfs::server(&fs, listener).await;
        Ok(())
    })
}
