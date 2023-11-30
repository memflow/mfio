use clap::Parser;
use log::*;
use mfio::backend::*;
use mfio_rt::{NativeRt, Tcp};
use std::net::SocketAddr;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:4446")]
    bind: SocketAddr,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!(
        "mfio-netfs is a dangerous PoC that violates memory safety. Do not run in production!"
    );
    warn!("mfio-netfs is a dangerous PoC that violates memory safety. Do not run in production!");
    info!("Grep for 'memunsafe' to see details.");

    let args = Args::parse();

    let fs = NativeRt::default();

    fs.block_on(async {
        let listener = fs.bind(args.bind).await?;
        info!("Bound to {}", args.bind);
        mfio_netfs::server(&fs, listener).await;
        Ok(())
    })
}
