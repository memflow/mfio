use mfio::error::{Error, Location, State, Subject, INTERNAL_ERROR};

pub fn io_err(state: State) -> Error {
    Error {
        code: INTERNAL_ERROR,
        location: Location::Backend,
        subject: Subject::Io,
        state,
    }
}

pub fn from_io_error(err: std::io::Error) -> Error {
    io_err(err.kind().into())
}
