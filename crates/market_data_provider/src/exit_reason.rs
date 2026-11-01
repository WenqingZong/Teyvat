/// Why the provider exited, as its process exit code; 0 is success. Add a variant per new kind of fatal failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExitReason {
    /// An exchange worker could not get its API key from Phanes; raised as a panic payload from the worker thread.
    ApiKeyUnavailable = 1,
    // TODO: FatalError should mean a config error only, and a server that stopped with an error
    // belongs to UnexpectedPanic. UnexpectedPanic is the fallback, so it should be the last variant.
    /// The config is invalid, the port is taken, or the server stopped with an error.
    FatalError = 2,
    UnexpectedPanic = 3,
    /// A subscription could not connect to an exchange endpoint; raised as a panic payload from the worker thread.
    ExchangeUnreachable = 4,
}

impl ExitReason {
    pub fn code(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_count_up_from_one() {
        assert_eq!(1, ExitReason::ApiKeyUnavailable.code());
        assert_eq!(2, ExitReason::FatalError.code());
        assert_eq!(3, ExitReason::UnexpectedPanic.code());
        assert_eq!(4, ExitReason::ExchangeUnreachable.code());
    }
}
