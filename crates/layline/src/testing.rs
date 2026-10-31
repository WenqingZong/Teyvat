//! Fixtures and checks for the tests of every crate, so each test does not spell out the same
//! instrument. Production code never calls these.
use crate::noise::{StaticKey, generate_keypair};
use crate::time::get_current_timestamp_nanoseconds;
use crate::types::{
    Bbo, Currency, Exchange, Instrument, LatencyMetric, MarketDataType, Subscription, Topic, Trade,
    UnderlyingType,
};
use secrecy::SecretBox;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{read_dir, read_to_string};
use std::path::{Path, PathBuf};

pub fn instrument(
    exchange: Exchange,
    base: Currency,
    quote: Currency,
    underlying_type: UnderlyingType,
) -> Instrument {
    Instrument {
        exchange,
        base,
        quote,
        underlying_type,
    }
}

pub fn topic(instrument: Instrument, market_data_type: MarketDataType) -> Topic {
    Topic {
        instrument,
        market_data_type,
    }
}

pub fn subscription(directory: &Path, topic: Topic) -> Subscription {
    Subscription {
        topic,
        path: topic.file_path(directory),
    }
}

pub fn bbo(instrument: Instrument, sequence: usize) -> Bbo {
    Bbo {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        ask_price: rust_decimal::dec!(101.5),
        ask_size: rust_decimal::dec!(2),
        bid_price: rust_decimal::dec!(101.4),
        bid_size: rust_decimal::dec!(4),
        sequence,
    }
}

pub fn trade(instrument: Instrument, sequence: usize) -> Trade {
    Trade {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        price: rust_decimal::dec!(101.5),
        size: rust_decimal::dec!(0.25),
        taker_side: crate::types::Side::Buy,
        sequence,
    }
}

/// A fresh static X25519 key pair: the private half wrapped as services hold it, and the public half.
pub fn test_keypair() -> (SecretBox<StaticKey>, StaticKey) {
    let keypair = generate_keypair().expect("keypair generation succeeds");
    (
        SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(&keypair.private)),
        keypair.public.try_into().expect("public key is 32 bytes"),
    )
}

/// Panics if the top-level modules of the crate at `manifest_directory` import each other in a
/// cycle, which Rust accepts but which erases layering. Follows `use crate::<module>` lines outside
/// `#[cfg(test)]`; the crate root is `src/lib.rs`, or `src/main.rs` for a binary-only crate.
pub fn assert_acyclic_modules(manifest_directory: &Path) {
    let source = manifest_directory.join("src");
    let root_file = ["lib.rs", "main.rs"]
        .iter()
        .map(|name| source.join(name))
        .find(|path| path.is_file())
        .expect("a crate has src/lib.rs or src/main.rs");
    let root = read_to_string(root_file).expect("the crate root is expected to be readable");
    let modules = top_level_modules(&root);
    let mut files = Vec::new();
    rust_files(&source, &mut files);

    let mut edges: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let module = module_name(&path, &source);
        if module == "crate" {
            continue;
        }
        let top = module.split('/').next().expect("a module has a name");
        let text = read_to_string(&path).expect("source file is expected to be readable");
        let production = text
            .split("#[cfg(test)]")
            .next()
            .expect("split always yields a first part");
        for line in production.lines() {
            let line = line.trim();
            if line.starts_with("//") {
                continue;
            }
            let Some(target) = first_crate_segment(line) else {
                continue;
            };
            if target != top && modules.contains(target) {
                edges
                    .entry(top.to_owned())
                    .or_default()
                    .insert(target.to_owned());
            }
        }
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for module in edges.keys() {
        visit(module, &edges, &mut visiting, &mut visited);
    }
}

fn rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in read_dir(directory).expect("source directory is expected to be readable") {
        let path = entry
            .expect("directory entry is expected to be readable")
            .path();
        if path.is_dir() {
            rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn top_level_modules(root: &str) -> BTreeSet<String> {
    root.lines()
        .filter_map(|line| {
            let line = line.trim();
            let rest = line
                .strip_prefix("pub mod ")
                .or_else(|| line.strip_prefix("mod "))?;
            rest.strip_suffix(';').map(str::to_owned)
        })
        .collect()
}

fn module_name(path: &Path, source: &Path) -> String {
    let relative = path
        .strip_prefix(source)
        .expect("source file is expected to live under src");
    let mut parts: Vec<String> = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    let file = parts.pop().expect("a source file has a name");
    if file == "lib.rs" || file == "main.rs" {
        return "crate".to_owned();
    }
    let file = file.strip_suffix(".rs").expect("source files end in .rs");
    if file != "mod" {
        parts.push(file.to_owned());
    }
    parts.join("/")
}

fn first_crate_segment(line: &str) -> Option<&str> {
    let rest = line
        .strip_prefix("use crate::")
        .or_else(|| line.strip_prefix("pub use crate::"))?;
    let segment = rest.split([':', ';', '{']).next()?.trim();
    (!segment.is_empty()).then_some(segment)
}

fn visit(
    module: &str,
    edges: &BTreeMap<String, BTreeSet<String>>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) {
    if visited.contains(module) {
        return;
    }
    if !visiting.insert(module.to_owned()) {
        panic!("module import cycle through {module}: {visiting:?}");
    }
    if let Some(targets) = edges.get(module) {
        for target in targets {
            visit(target, edges, visiting, visited);
        }
    }
    visiting.remove(module);
    visited.insert(module.to_owned());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, write};
    use tempfile::tempdir;

    fn write_crate(files: &[(&str, &str)]) -> tempfile::TempDir {
        let directory = tempdir().expect("temp dir is created");
        for (path, text) in files {
            let path = directory.path().join("src").join(path);
            create_dir_all(path.parent().expect("a source file has a directory"))
                .expect("source directory is created");
            write(path, text).expect("source file is written");
        }
        directory
    }

    #[test]
    fn layered_modules_pass() {
        let directory = write_crate(&[
            ("lib.rs", "mod a;\nmod b;\n"),
            ("a.rs", "use crate::b::Thing;\n"),
            ("b/mod.rs", "pub struct Thing;\n"),
        ]);
        assert_acyclic_modules(directory.path());
    }

    #[test]
    fn imports_in_tests_are_ignored() {
        let directory = write_crate(&[
            ("main.rs", "mod a;\nmod b;\n"),
            ("a.rs", "use crate::b::Thing;\n"),
            (
                "b.rs",
                "pub struct Thing;\n#[cfg(test)]\nmod tests {\nuse crate::a;\n}\n",
            ),
        ]);
        assert_acyclic_modules(directory.path());
    }

    #[test]
    #[should_panic(expected = "module import cycle")]
    fn cycle_panics() {
        let directory = write_crate(&[
            ("lib.rs", "mod a;\nmod b;\n"),
            ("a.rs", "use crate::b::Thing;\npub struct Other;\n"),
            ("b.rs", "pub use crate::a::Other;\npub struct Thing;\n"),
        ]);
        assert_acyclic_modules(directory.path());
    }
}
