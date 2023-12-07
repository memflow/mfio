use mfio::backend::*;
use mfio::error::Result;
use mfio::traits::*;
use mfio_netfs::NetworkFs;
use mfio_rt::*;

fn main() -> Result<()> {
    let rt = NativeRt::default();
    let rt = NetworkFs::with_fs("127.0.0.1:4446".parse().unwrap(), rt.into(), true)?;

    rt.block_on(async {
        let file = rt
            .open(Path::new("Cargo.toml"), OpenOptions::new().read(true))
            .await?;

        let mut buf = vec![];
        file.read_to_end(0, &mut buf).await?;

        let data = String::from_utf8_lossy(&buf);
        println!("{data}");

        Ok(())
    })
}
