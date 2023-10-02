use mfio::error::Error;
use mfio::io::*;

#[derive(Default)]
pub struct DeferredPackets {
    packets: Vec<(AnyPacket, Option<Error>)>,
}

impl Drop for DeferredPackets {
    fn drop(&mut self) {
        self.flush();
    }
}

impl DeferredPackets {
    pub fn ok(&mut self, p: impl Into<AnyPacket>) {
        self.packets.push((p.into(), None));
    }

    pub fn error(&mut self, p: impl Into<AnyPacket>, err: Error) {
        self.packets.push((p.into(), Some(err)))
    }

    pub fn flush(&mut self) {
        self.packets
            .drain(0..)
            .filter_map(|(p, e)| Some(p).zip(e))
            .for_each(|(p, e)| p.error(e));
    }
}
