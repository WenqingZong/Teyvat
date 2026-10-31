use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoLocal;

pub fn init_tracing() -> WorkerGuard {
    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(writer)
        .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f".to_owned()))
        .with_file(true)
        .with_line_number(true)
        .init();
    guard
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_tracing_installs_global_subscriber() {
        let _guard = init_tracing();
        assert!(tracing::dispatcher::has_been_set());
    }
}
