use clap::Parser;
#[cfg(unix)]
use mfio::backend::integrations::tokio::Tokio;
use mfio::backend::*;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:4446")]
    bind: SocketAddr,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let mut fs = mfio_rt::NativeRt::default();

    let listener = TcpListener::bind(args.bind).await?;

    let (tx, rx) = flume::bounded(4);

    let accept = async move {
        while let Ok((stream, _)) = listener.accept().await {
            if let Ok(stream) = stream.into_std() {
                if tx.send_async(stream).await.is_err() {
                    break;
                }
            }
        }
    };

    #[cfg(unix)]
    let serve = Tokio::run_with_mut(&mut fs, |fs| mfio_netfs::server(fs, rx.into_stream()));
    #[cfg(not(unix))]
    let serve = Null::run_with_mut(&mut fs, |fs| mfio_netfs::server(fs, rx.into_stream()));

    tokio::join!(accept, serve);

    Ok(())
}
