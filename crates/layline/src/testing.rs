//! Fixtures and checks for the tests of every crate, so each test does not spell out the same
//! instrument. Production code never calls these.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{read_dir, read_to_string};
use std::path::{Path, PathBuf};

use rust_decimal::dec;
use secrecy::SecretBox;

use crate::noise::{StaticKey, generate_keypair};
use crate::time::get_current_timestamp_nanoseconds;
use crate::types::{
    Bbo, Currency, Exchange, Instrument, LatencyMetric, MarketDataType, Side, StrategyName,
    Subscription, Topic, Trade, UnderlyingType,
};

pub fn test_instrument(
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

pub fn test_topic(instrument: Instrument, market_data_type: MarketDataType) -> Topic {
    Topic {
        instrument,
        market_data_type,
    }
}

pub fn test_subscription(
    root_directory: &Path,
    strategy_name: &StrategyName,
    topic: Topic,
) -> Subscription {
    Subscription {
        topic,
        path: topic.file_path(root_directory, strategy_name),
    }
}

pub fn test_bbo(instrument: Instrument, sequence: usize) -> Bbo {
    Bbo {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        ask_price: dec!(101.5),
        ask_size: dec!(2),
        bid_price: dec!(101.4),
        bid_size: dec!(4),
        sequence,
    }
}

pub fn test_trade(instrument: Instrument, sequence: usize) -> Trade {
    Trade {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        price: dec!(101.5),
        size: dec!(0.25),
        taker_side: Side::Buy,
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

/// Standard library crates: the first import group.
const STANDARD_LIBRARY_CRATES: [&str; 3] = ["alloc", "core", "std"];
/// Manifest tables whose keys name the crates a package depends on.
const DEPENDENCY_TABLES: [&str; 3] = [
    "[dependencies]",
    "[dev-dependencies]",
    "[build-dependencies]",
];
/// Directories of a package whose sources `assert_grouped_imports` checks.
const SOURCE_DIRECTORIES: [&str; 4] = ["src", "tests", "benches", "examples"];

/// The three import groups, in the order they must appear.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ImportGroup {
    StandardLibrary,
    ThirdParty,
    /// `crate`, `self`, `super`, workspace crates and child modules: code in this repository.
    Repository,
}

/// Panics unless the `use` items of the package at `manifest_directory` come in blank-line
/// separated groups, in this order: the standard library (`std`, `core`, `alloc`), third-party
/// crates (dependencies in its `Cargo.toml` that are not workspace members), and code in this
/// repository (`crate`, `self`, `super`, workspace crates, child modules). Each group appears at
/// most once per run of `use` items; rustfmt sorts within each group. Checks `src`, `tests`,
/// `benches` and `examples`.
pub fn assert_grouped_imports(manifest_directory: &Path) {
    let manifest = read_to_string(manifest_directory.join("Cargo.toml"))
        .expect("the package manifest is expected to be readable");
    let workspace_crates = workspace_crate_names(manifest_directory);
    let third_party_crates: BTreeSet<String> = dependency_names(&manifest)
        .into_iter()
        .filter(|name| !workspace_crates.contains(name))
        .collect();
    let mut files = Vec::new();
    for directory in SOURCE_DIRECTORIES {
        let path = manifest_directory.join(directory);
        if path.is_dir() {
            rust_files(&path, &mut files);
        }
    }
    let mut violations = Vec::new();
    for path in files {
        let text = read_to_string(&path).expect("source file is expected to be readable");
        for line_number in import_group_violations(&text, &third_party_crates) {
            violations.push(format!("{}:{line_number}", path.display()));
        }
    }
    assert!(
        violations.is_empty(),
        "imports must be grouped as std, third-party, this repository, separated by blank lines:\n{}",
        violations.join("\n")
    );
}

/// Package names of the workspace root and of every crate under its `crates/`.
fn workspace_crate_names(manifest_directory: &Path) -> BTreeSet<String> {
    let root = manifest_directory
        .ancestors()
        .find(|directory| {
            read_to_string(directory.join("Cargo.toml"))
                .is_ok_and(|text| text.lines().any(|line| line.trim() == "[workspace]"))
        })
        .expect("the package is expected to be in a workspace");
    let mut manifests = vec![root.join("Cargo.toml")];
    for entry in read_dir(root.join("crates")).expect("crates/ is expected to be readable") {
        let path = entry
            .expect("directory entry is expected to be readable")
            .path();
        manifests.push(path.join("Cargo.toml"));
    }
    manifests
        .iter()
        .filter_map(|manifest| read_to_string(manifest).ok())
        .filter_map(|text| package_name(&text))
        .collect()
}

/// The `name` of a manifest's `[package]` table, spelled as Rust paths spell it.
fn package_name(manifest: &str) -> Option<String> {
    manifest
        .lines()
        .skip_while(|line| line.trim() != "[package]")
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with('['))
        .find_map(|line| line.trim().strip_prefix("name = "))
        .map(|name| name.trim_matches('"').replace('-', "_"))
}

/// Every crate a manifest's dependency tables name, spelled as Rust paths spell it.
fn dependency_names(manifest: &str) -> Vec<String> {
    let mut in_dependency_table = false;
    let mut names = Vec::new();
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_dependency_table = DEPENDENCY_TABLES.contains(&line);
            continue;
        }
        if !in_dependency_table || line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(key) = line.split(['.', '=', ' ']).next() {
            names.push(key.replace('-', "_"));
        }
    }
    names
}

/// 1-based line numbers of the `use` items that break the grouping.
fn import_group_violations(text: &str, third_party_crates: &BTreeSet<String>) -> Vec<usize> {
    let mut violations = Vec::new();
    let mut run_groups: Vec<ImportGroup> = Vec::new();
    let mut blank_since_last_import = false;
    let mut inside_import = false;
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if inside_import {
            inside_import = !line.ends_with(';');
            continue;
        }
        if line.is_empty() {
            blank_since_last_import = true;
            continue;
        }
        if line.starts_with("//") || line.starts_with("#[") {
            continue;
        }
        let Some(path) = import_path(line) else {
            run_groups.clear();
            blank_since_last_import = false;
            continue;
        };
        let group = classify_import(path, third_party_crates);
        if let Some(&last_group) = run_groups.last() {
            let in_order = if blank_since_last_import {
                group > last_group
            } else {
                group == last_group
            };
            if !in_order {
                violations.push(index + 1);
            }
        }
        if blank_since_last_import || run_groups.is_empty() {
            run_groups.push(group);
        }
        blank_since_last_import = false;
        inside_import = !line.ends_with(';');
    }
    violations
}

/// The path after `use`, for a `use` item with or without visibility; `None` for any other line.
fn import_path(line: &str) -> Option<&str> {
    let item = match line.strip_prefix("pub") {
        Some(after_pub) => {
            let after_visibility = match after_pub.strip_prefix('(') {
                Some(restriction) => &restriction[restriction.find(')')? + 1..],
                None => after_pub,
            };
            after_visibility.strip_prefix(' ')?
        }
        None => line,
    };
    item.strip_prefix("use ")
}

fn classify_import(path: &str, third_party_crates: &BTreeSet<String>) -> ImportGroup {
    let root = path
        .trim_start_matches("::")
        .split([':', ';', '{', ' '])
        .next()
        .unwrap_or_default();
    if STANDARD_LIBRARY_CRATES.contains(&root) {
        ImportGroup::StandardLibrary
    } else if third_party_crates.contains(root) {
        ImportGroup::ThirdParty
    } else {
        ImportGroup::Repository
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{create_dir_all, write};

    use tempfile::tempdir;

    use super::*;

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

    fn third_party() -> BTreeSet<String> {
        ["serde", "tokio"].into_iter().map(str::to_owned).collect()
    }

    #[test]
    fn three_groups_in_order_pass() {
        let text = "use std::path::Path;\n\nuse serde::Serialize;\nuse tokio::spawn;\n\nuse crate::types::Topic;\nuse layline::bark::{\n    BarkMessage,\n};\n";
        assert!(import_group_violations(text, &third_party()).is_empty());
    }

    #[test]
    fn mixed_group_fails() {
        let text = "use serde::Serialize;\nuse std::path::Path;\n";
        assert_eq!(vec![2], import_group_violations(text, &third_party()));
    }

    #[test]
    fn groups_out_of_order_fail() {
        let text = "use crate::types::Topic;\n\nuse std::path::Path;\n";
        assert_eq!(vec![3], import_group_violations(text, &third_party()));
    }

    #[test]
    fn split_group_fails() {
        let text = "use std::path::Path;\n\nuse std::sync::Arc;\n";
        assert_eq!(vec![3], import_group_violations(text, &third_party()));
    }

    #[test]
    fn other_items_start_a_new_run() {
        let text = "use crate::types::Topic;\n\nmod tests {\n    use super::*;\n    use std::path::Path;\n}\n";
        assert_eq!(vec![5], import_group_violations(text, &third_party()));
    }

    #[test]
    fn visibility_and_child_modules_count() {
        let text = "pub use std::path::Path;\n\npub(crate) use bbo::Bbo;\n";
        assert!(import_group_violations(text, &third_party()).is_empty());
    }

    #[test]
    fn manifest_tables_name_dependencies() {
        let manifest = "[package]\nname = \"market-data\"\n\n[dependencies]\nlayline.workspace = true\ntokio = { workspace = true }\n\n[dev-dependencies]\ntempfile = \"3\"\n\n[lints]\nworkspace = true\n";
        assert_eq!(Some("market_data".to_owned()), package_name(manifest));
        assert_eq!(
            vec!["layline", "tokio", "tempfile"],
            dependency_names(manifest)
        );
    }
}
