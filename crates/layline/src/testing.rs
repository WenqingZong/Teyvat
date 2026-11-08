//! Fixtures and checks for the tests of every crate, so each test does not spell out the same
//! instrument. Production code never calls these.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{Permissions, read_dir, read_to_string, set_permissions, write};
use std::iter::{once, repeat_n};
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use rust_decimal::dec;
use secrecy::SecretBox;

use crate::noise::{StaticKey, generate_keypair};
use crate::process_metrics::{MemoryUsage, ProcessSample, ThreadSample};
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
        path: topic.ring_key().file_path(root_directory, strategy_name),
    }
}

/// A two-thread sample with every field set, as Celestia sends it.
pub fn test_process_sample() -> ProcessSample {
    ProcessSample {
        timestamp_seconds: NonZeroU32::new(1_800_000_000).expect("not zero"),
        cpu_percent: 12.5,
        memory: MemoryUsage {
            anonymous_kibibytes: 4096,
            file_kibibytes: 8192,
            shared_memory_kibibytes: 1024,
        },
        open_file_descriptors: 12,
        voluntary_context_switches_per_second: 3.5,
        involuntary_context_switches_per_second: 0.5,
        tcp_received_bytes_per_second: 1024.0,
        tcp_sent_bytes_per_second: 512.0,
        threads: vec![
            ThreadSample {
                thread_id: 100,
                name: "main".to_owned(),
                cpu_percent: 12.0,
                voluntary_context_switches_per_second: 3.0,
                involuntary_context_switches_per_second: 0.5,
            },
            ThreadSample {
                thread_id: 101,
                name: "process_monitor".to_owned(),
                cpu_percent: 0.5,
                voluntary_context_switches_per_second: 0.5,
                involuntary_context_switches_per_second: 0.0,
            },
        ],
        sampling_duration_microseconds: 2000,
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

/// A fresh static X25519 key pair, private and public half, both wrapped as services hold them.
pub fn test_keypair() -> (SecretBox<StaticKey>, SecretBox<StaticKey>) {
    let keypair = generate_keypair().expect("keypair generation succeeds");
    (
        SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(&keypair.private)),
        SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(&keypair.public)),
    )
}

/// The mode `crate::secrets` requires of config and key files.
const OWNER_ONLY_MODE: u32 = 0o600;

/// Writes `content` to `path` as a chmod 600 file, as configs and private keys must be.
pub fn write_secret_file(path: &Path, content: &str) {
    write(path, content).expect("writing the secret file is expected to not fail");
    set_permissions(path, Permissions::from_mode(OWNER_ONLY_MODE))
        .expect("setting permissions is expected to not fail");
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
        let path = path.display();
        for line_number in import_group_violations(&text, &third_party_crates) {
            violations.push(format!("{path}:{line_number}"));
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

/// How a format placeholder names its argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlaceholderKind {
    /// `{}`, `{:?}`, `{0}`: an argument after the format string, by position.
    Positional,
    /// `{name}`, `{name:?}`: a variable or constant in scope, or a named argument.
    Named,
}

/// Panics if a string literal of the package at `manifest_directory` mixes positional placeholders
/// (`{}`, `{:?}`, `{0}`) with named ones (`{name}`): a format string uses one style, and an
/// expression such as `path.display()` is bound to a local first. Checks `build.rs`, `src`,
/// `tests`, `benches` and `examples`, skipping comments.
pub fn assert_consistent_format_strings(manifest_directory: &Path) {
    let mut files = Vec::new();
    let build_script = manifest_directory.join("build.rs");
    if build_script.is_file() {
        files.push(build_script);
    }
    for directory in SOURCE_DIRECTORIES {
        let path = manifest_directory.join(directory);
        if path.is_dir() {
            rust_files(&path, &mut files);
        }
    }
    let mut violations = Vec::new();
    for path in files {
        let text = read_to_string(&path).expect("source file is expected to be readable");
        let path = path.display();
        for (line_number, literal) in string_literals(&text) {
            if mixes_placeholder_kinds(&literal) {
                violations.push(format!("{path}:{line_number}: {literal:?}"));
            }
        }
    }
    let joined_violations = violations.join("\n");
    assert!(
        violations.is_empty(),
        "format strings must not mix positional placeholders ({{}}) with named ones ({{name}}):\n{joined_violations}"
    );
}

/// Every string literal outside comments, with the 1-based line it starts on, as written in the
/// source (escapes not processed). Covers plain, byte and raw strings; char literals and lifetimes
/// are skipped so that `'"'` opens no string.
fn string_literals(text: &str) -> Vec<(usize, String)> {
    let characters: Vec<char> = text.chars().collect();
    let mut literals = Vec::new();
    let mut line_number = 1;
    let mut index = 0;
    while index < characters.len() {
        let start = index;
        let rest = &characters[index..];
        index = if rest.starts_with(&['/', '/']) {
            find_from(&characters, index, &['\n']).unwrap_or(characters.len())
        } else if rest.starts_with(&['/', '*']) {
            find_from(&characters, index + 2, &['*', '/']).map_or(characters.len(), |end| end + 2)
        } else if let Some(hash_count) = raw_string_opening(&characters, index) {
            let content_start =
                index + raw_string_prefix_length(&characters, index) + hash_count + 1;
            let closing: Vec<char> = once('"').chain(repeat_n('#', hash_count)).collect();
            let content_end =
                find_from(&characters, content_start, &closing).unwrap_or(characters.len());
            literals.push((
                line_number,
                characters[content_start..content_end].iter().collect(),
            ));
            (content_end + closing.len()).min(characters.len())
        } else if characters[index] == '"' {
            let content_end = plain_string_end(&characters, index + 1);
            literals.push((
                line_number,
                characters[index + 1..content_end].iter().collect(),
            ));
            (content_end + 1).min(characters.len())
        } else if characters[index] == '\'' {
            char_literal_end(&characters, index)
        } else {
            index + 1
        };
        line_number += characters[start..index]
            .iter()
            .filter(|&&character| character == '\n')
            .count();
    }
    literals
}

/// The first index at or after `from` where `pattern` starts.
fn find_from(characters: &[char], from: usize, pattern: &[char]) -> Option<usize> {
    (from..characters.len()).find(|&index| characters[index..].starts_with(pattern))
}

/// The number of `#` of a raw string (`r"`, `r#"`, `br#"`) opening at `index`, which must not
/// continue an identifier such as `letter"`.
fn raw_string_opening(characters: &[char], index: usize) -> Option<usize> {
    let continues_identifier = index > 0 && is_identifier_character(characters[index - 1]);
    if continues_identifier {
        return None;
    }
    let after_prefix = index + raw_string_prefix_length(characters, index);
    if after_prefix == index {
        return None;
    }
    let hash_count = characters[after_prefix..]
        .iter()
        .take_while(|&&character| character == '#')
        .count();
    (characters.get(after_prefix + hash_count) == Some(&'"')).then_some(hash_count)
}

/// 1 for `r`, 2 for `br`, 0 when no raw string prefix starts at `index`.
fn raw_string_prefix_length(characters: &[char], index: usize) -> usize {
    match (characters.get(index), characters.get(index + 1)) {
        (Some('r'), _) => 1,
        (Some('b'), Some('r')) => 2,
        _ => 0,
    }
}

fn is_identifier_character(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

/// The index of the `"` closing a plain string whose content starts at `content_start`.
fn plain_string_end(characters: &[char], content_start: usize) -> usize {
    let mut index = content_start;
    while index < characters.len() && characters[index] != '"' {
        index += if characters[index] == '\\' { 2 } else { 1 };
    }
    index.min(characters.len())
}

/// The index after a char literal (`'x'`, `'\n'`, `'\u{7f}'`) opening at `index`, or after the
/// quote alone when it starts a lifetime such as `'a`.
fn char_literal_end(characters: &[char], index: usize) -> usize {
    match characters.get(index + 1) {
        Some('\\') => {
            find_from(characters, index + 2, &['\'']).map_or(characters.len(), |end| end + 1)
        }
        Some(_) if characters.get(index + 2) == Some(&'\'') => index + 3,
        _ => index + 1,
    }
}

/// Whether `literal` holds both a positional and a named placeholder. `{{` and `}}` are escaped
/// braces, not placeholders.
fn mixes_placeholder_kinds(literal: &str) -> bool {
    let unescaped = literal.replace("{{", "").replace("}}", "");
    let mut kinds = Vec::new();
    for candidate in unescaped.split('{').skip(1) {
        let Some((placeholder, _)) = candidate.split_once('}') else {
            continue;
        };
        if let Some(kind) = placeholder_kind(placeholder) {
            kinds.push(kind);
        }
    }
    kinds.contains(&PlaceholderKind::Positional) && kinds.contains(&PlaceholderKind::Named)
}

/// The kind of the placeholder between `{` and `}`, or `None` for text that only looks like one,
/// such as `{"key": 1`.
fn placeholder_kind(placeholder: &str) -> Option<PlaceholderKind> {
    let argument = placeholder
        .split_once(':')
        .map_or(placeholder, |(argument, _)| argument);
    let mut argument_characters = argument.chars();
    match argument_characters.next() {
        None => Some(PlaceholderKind::Positional),
        Some(first) if first.is_ascii_digit() => argument
            .chars()
            .all(|character| character.is_ascii_digit())
            .then_some(PlaceholderKind::Positional),
        Some(first) if first.is_alphabetic() || first == '_' => argument_characters
            .all(is_identifier_character)
            .then_some(PlaceholderKind::Named),
        Some(_) => None,
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

    // Inputs that mix placeholder kinds are joined from pieces, so that no literal in this file
    // mixes them and the package's own format string check still passes.

    #[test]
    fn mixed_placeholder_kinds_are_found() {
        assert!(mixes_placeholder_kinds(
            &["value {}", " of {name}"].concat()
        ));
        assert!(mixes_placeholder_kinds(&["{0}", " {name:?}"].concat()));
    }

    #[test]
    fn one_placeholder_kind_passes() {
        assert!(!mixes_placeholder_kinds("{} {:?} {0} {:>8}"));
        assert!(!mixes_placeholder_kinds(
            "{name} {name:#?} {GIT_COMMIT_HASH}"
        ));
        assert!(!mixes_placeholder_kinds("{{}} {name}"));
        assert!(!mixes_placeholder_kinds("{\"key\": 1} {}"));
        assert!(!mixes_placeholder_kinds("{ unclosed"));
    }

    #[test]
    fn string_literals_skip_comments_and_char_literals() {
        let text = [
            "// \"{}",
            " {name}\"\n",
            "fn first<'a>(text: &'a str) -> char { '\"' }\n",
            "let raw = r#\"say \"hi\"\"#;\n",
            "let bytes = b\"\\\"quoted\\\"\";\n",
            "/* \"in block\" */ let plain = \"a\\\"b\"; let escaped = '\\'';\n",
            "let raw_bytes = br\"x\"; let r#type = 1;\n",
        ]
        .concat();
        assert_eq!(
            vec![
                (3, r#"say "hi""#.to_owned()),
                (4, r#"\"quoted\""#.to_owned()),
                (5, r#"a\"b"#.to_owned()),
                (6, "x".to_owned()),
            ],
            string_literals(&text)
        );
    }

    #[test]
    #[should_panic(expected = "format strings must not mix")]
    fn mixed_format_string_panics() {
        let source = [
            "fn describe(x: u8) -> String { format!(\"{}",
            " {x}\", x) }\n",
        ]
        .concat();
        let directory = write_crate(&[("lib.rs", &source)]);
        assert_consistent_format_strings(directory.path());
    }

    #[test]
    fn consistent_format_strings_pass_including_build_script() {
        let directory = write_crate(&[("lib.rs", "fn f(x: u8) -> String { format!(\"{x}\") }\n")]);
        write(
            directory.path().join("build.rs"),
            "fn main() { let _ = format!(\"{} {}\", 1, 2); }\n",
        )
        .expect("build script is written");
        assert_consistent_format_strings(directory.path());
    }
}
