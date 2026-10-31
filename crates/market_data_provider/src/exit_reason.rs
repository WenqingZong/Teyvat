/// Why the provider exited, as its process exit code; 0 is success. Add a variant per new kind of fatal failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExitReason {
    /// An exchange worker could not get its API key from Phanes; raised as a panic payload from the worker thread.
    ApiKeyUnavailable = 1,
    /// The config is invalid, the port is taken, or the server stopped with an error.
    FatalError = 2,
    UnexpectedPanic = 3,
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
    }
}
