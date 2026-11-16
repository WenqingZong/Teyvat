//! Source checks for the tests of every crate. This crate is a dev-dependency only,
//! so production binaries never contain it.
//!
//! The source checks parse Rust with `syn` and manifests with `toml`; nothing here re-implements
//! either format.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{read_dir, read_to_string};
use std::path::{Path, PathBuf};

use proc_macro2::{TokenStream, TokenTree};
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::{Attribute, File, Item, Lit, LitStr, Meta, UseTree};
use toml::Value as TomlValue;

/// Panics if the top-level modules of the crate at `manifest_directory` import each other in a
/// cycle, which Rust accepts but which erases layering. Follows `use crate::<module>` items outside
/// `#[cfg(test)]`; the crate root is `src/lib.rs`, or `src/main.rs` for a binary-only crate.
pub fn assert_acyclic_modules(manifest_directory: &Path) {
    let source = manifest_directory.join("src");
    let root_file = ["lib.rs", "main.rs"]
        .iter()
        .map(|name| source.join(name))
        .find(|path| path.is_file())
        .expect("a crate has src/lib.rs or src/main.rs");
    let root = parse_file(&root_file);
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
        let file = parse_file(&path);
        for target in production_crate_imports(&file) {
            if target != top && modules.contains(&target) {
                edges.entry(top.to_owned()).or_default().insert(target);
            }
        }
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for module in edges.keys() {
        visit(module, &edges, &mut visiting, &mut visited);
    }
}

fn production_crate_imports(file: &File) -> BTreeSet<String> {
    let mut imports = BTreeSet::new();
    collect_production_imports(&file.items, &mut imports);
    imports
}

fn collect_production_imports(items: &[Item], imports: &mut BTreeSet<String>) {
    for item in items {
        match item {
            Item::Use(item_use) if !is_cfg_test(&item_use.attrs) => {
                collect_crate_use(&item_use.tree, imports);
            }
            Item::Mod(item_mod) if !is_cfg_test(&item_mod.attrs) => {
                if let Some((_, items)) = &item_mod.content {
                    collect_production_imports(items, imports);
                }
            }
            _ => {}
        }
    }
}

fn collect_crate_use(tree: &UseTree, imports: &mut BTreeSet<String>) {
    let UseTree::Path(path) = tree else {
        return;
    };
    if path.ident != "crate" {
        return;
    }
    collect_after_crate(&path.tree, imports);
}

fn collect_after_crate(tree: &UseTree, imports: &mut BTreeSet<String>) {
    match tree {
        UseTree::Name(name) => {
            insert_module(&name.ident.to_string(), imports);
        }
        UseTree::Rename(rename) => {
            insert_module(&rename.ident.to_string(), imports);
        }
        UseTree::Path(path) => {
            insert_module(&path.ident.to_string(), imports);
        }
        UseTree::Group(group) => {
            for item in &group.items {
                collect_after_crate(item, imports);
            }
        }
        UseTree::Glob(_) => {}
    }
}

fn insert_module(module: &str, imports: &mut BTreeSet<String>) {
    if module != "self" {
        imports.insert(module.to_owned());
    }
}

fn top_level_modules(file: &File) -> BTreeSet<String> {
    file.items
        .iter()
        .filter_map(|item| match item {
            Item::Mod(item_mod) if !is_cfg_test(&item_mod.attrs) => {
                Some(item_mod.ident.to_string())
            }
            _ => None,
        })
        .collect()
}

fn parse_file(path: &Path) -> File {
    let text = read_to_string(path).expect("source file is expected to be readable");
    syn::parse_file(&text).unwrap_or_else(|error| {
        panic!("{} is not valid Rust: {error}", path.display());
    })
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
const DEPENDENCY_TABLES: [&str; 3] = ["dependencies", "dev-dependencies", "build-dependencies"];
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

/// One `use` item in a source file, with the lines its span covers.
struct Import {
    start_line: usize,
    end_line: usize,
    group: ImportGroup,
    mixes_groups: bool,
}

/// Panics unless the `use` items of the package at `manifest_directory` come in blank-line
/// separated groups, in this order: the standard library (`std`, `core`, `alloc`), third-party
/// crates (dependencies in its `Cargo.toml` that are not workspace members), and code in this
/// repository (`crate`, `self`, `super`, workspace crates, child modules). Each group appears at
/// most once per run of `use` items; rustfmt sorts within each group. Checks `src`, `tests`,
/// `benches` and `examples`.
pub fn assert_grouped_imports(manifest_directory: &Path) {
    let manifest_text = read_to_string(manifest_directory.join("Cargo.toml"))
        .expect("the package manifest is expected to be readable");
    let manifest: TomlValue =
        toml::from_str(&manifest_text).expect("the package manifest is valid TOML");
    let workspace_crates = workspace_crate_names(manifest_directory);
    let third_party_crates = dependency_names(&manifest, &workspace_crates);

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
        let file = syn::parse_file(&text).unwrap_or_else(|error| {
            panic!("{} is not valid Rust: {error}", path.display());
        });
        let mut imports = Vec::new();
        collect_use_items(&file.items, &mut imports, &third_party_crates);
        imports.sort_by_key(|import| import.start_line);
        let lines: Vec<&str> = text.lines().collect();
        for line_number in import_group_violations(&imports, &lines) {
            let path = path.display();
            violations.push(format!("{path}:{line_number}"));
        }
    }
    assert!(
        violations.is_empty(),
        "imports must be grouped as std, third-party, this repository, separated by blank lines:\n{}",
        violations.join("\n")
    );
}

fn collect_use_items(
    items: &[Item],
    imports: &mut Vec<Import>,
    third_party_crates: &BTreeSet<String>,
) {
    for item in items {
        match item {
            Item::Use(item_use) => {
                let span = item_use.span();
                let mut roots = BTreeSet::new();
                collect_use_roots(&item_use.tree, &mut roots);
                let groups: BTreeSet<ImportGroup> = roots
                    .iter()
                    .map(|root| classify_import(root, third_party_crates))
                    .collect();
                let mixes_groups = groups.len() > 1;
                imports.push(Import {
                    start_line: span.start().line,
                    end_line: span.end().line,
                    group: groups.into_iter().next().unwrap_or(ImportGroup::Repository),
                    mixes_groups,
                });
            }
            Item::Mod(item_mod) => {
                if let Some((_, items)) = &item_mod.content {
                    collect_use_items(items, imports, third_party_crates);
                }
            }
            _ => {}
        }
    }
}

fn collect_use_roots(tree: &UseTree, roots: &mut BTreeSet<String>) {
    match tree {
        UseTree::Path(path) => {
            roots.insert(path.ident.to_string());
        }
        UseTree::Name(name) => {
            roots.insert(name.ident.to_string());
        }
        UseTree::Rename(rename) => {
            roots.insert(rename.ident.to_string());
        }
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_roots(item, roots);
            }
        }
        UseTree::Glob(_) => {}
    }
}

fn classify_import(root: &str, third_party_crates: &BTreeSet<String>) -> ImportGroup {
    if STANDARD_LIBRARY_CRATES.contains(&root) {
        ImportGroup::StandardLibrary
    } else if matches!(root, "crate" | "self" | "super") {
        ImportGroup::Repository
    } else if third_party_crates.contains(root) {
        ImportGroup::ThirdParty
    } else {
        ImportGroup::Repository
    }
}

fn import_group_violations(imports: &[Import], lines: &[&str]) -> Vec<usize> {
    let mut violations = Vec::new();
    let mut run: Vec<ImportGroup> = Vec::new();
    let mut seen: BTreeSet<ImportGroup> = BTreeSet::new();
    let mut previous: Option<&Import> = None;
    for import in imports {
        if import.mixes_groups {
            violations.push(import.start_line);
        }
        let mut reset = false;
        let mut separator = false;
        if let Some(previous) = previous
            && import.start_line > previous.end_line + 1
        {
            let between = &lines[previous.end_line..import.start_line - 1];
            let has_code = between.iter().any(|line| is_code_line(line));
            let has_blank = between.iter().any(|line| line.trim().is_empty());
            reset = has_code;
            separator = !has_code && has_blank;
        }
        if reset {
            run.clear();
            seen.clear();
        }
        if run.is_empty() {
            run.push(import.group);
            seen.insert(import.group);
        } else if separator {
            let last = *run.last().expect("a nonempty run has a last group");
            if import.group <= last || seen.contains(&import.group) {
                violations.push(import.start_line);
            }
            run.push(import.group);
            seen.insert(import.group);
        } else {
            let last = *run.last().expect("a nonempty run has a last group");
            if import.group != last {
                violations.push(import.start_line);
            }
        }
        previous = Some(import);
    }
    violations
}

/// Lines of code between imports, as opposed to blank lines, comments and attributes.
fn is_code_line(line: &str) -> bool {
    let line = line.trim();
    !line.is_empty()
        && !line.starts_with("//")
        && !line.starts_with("#[")
        && !line.starts_with("#![")
        && !line.starts_with("/*")
        && !line.starts_with('*')
        && !line.starts_with("*/")
}

/// Package names of the workspace root and of every crate under its `crates/`.
fn workspace_crate_names(manifest_directory: &Path) -> BTreeSet<String> {
    let workspace_root = manifest_directory
        .ancestors()
        .find(|directory| {
            read_manifest(&directory.join("Cargo.toml"))
                .is_some_and(|manifest| manifest.get("workspace").is_some())
        })
        .unwrap_or(manifest_directory);
    let mut manifests = vec![workspace_root.join("Cargo.toml")];
    let crates_directory = workspace_root.join("crates");
    if crates_directory.is_dir() {
        for entry in read_dir(&crates_directory).expect("crates/ is expected to be readable") {
            let path = entry
                .expect("directory entry is expected to be readable")
                .path();
            manifests.push(path.join("Cargo.toml"));
        }
    }
    manifests
        .iter()
        .filter_map(|manifest| read_manifest(manifest))
        .filter_map(|manifest| package_name(&manifest))
        .collect()
}

fn read_manifest(path: &Path) -> Option<TomlValue> {
    read_to_string(path)
        .ok()
        .and_then(|text| toml::from_str(&text).ok())
}

/// The `name` of a manifest's `[package]` table, spelled as Rust paths spell it.
fn package_name(manifest: &TomlValue) -> Option<String> {
    manifest
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(TomlValue::as_str)
        .map(|name| name.replace('-', "_"))
}

/// Every crate a manifest's dependency tables name, spelled as Rust paths spell it.
fn dependency_names(manifest: &TomlValue, workspace_crates: &BTreeSet<String>) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    collect_dependency_names(manifest, &mut names);
    names.retain(|name| !workspace_crates.contains(name));
    names
}

fn collect_dependency_names(value: &TomlValue, names: &mut BTreeSet<String>) {
    let Some(table) = value.as_table() else {
        return;
    };
    for (key, value) in table {
        if key.as_str() == "workspace" {
            continue;
        }
        if DEPENDENCY_TABLES.contains(&key.as_str())
            && let Some(dependencies) = value.as_table()
        {
            for name in dependencies.keys() {
                names.insert(name.replace('-', "_"));
            }
        }
        collect_dependency_names(value, names);
    }
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
        let file = syn::parse_file(&text).unwrap_or_else(|error| {
            panic!("{} is not valid Rust: {error}", path.display());
        });
        let mut collector = StringLiteralCollector::default();
        collector.visit_file(&file);
        let path = path.display();
        for (line_number, literal) in collector.literals {
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

/// Every string literal outside attributes, which are skipped so doc comments do not count.
#[derive(Default)]
struct StringLiteralCollector {
    literals: Vec<(usize, String)>,
}

impl<'ast> Visit<'ast> for StringLiteralCollector {
    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        // Doc comments are `#[doc = "..."]`, not format strings; `#[error("...")]` and other
        // list attributes are, so scan their tokens.
        if attribute.path().is_ident("doc") {
            return;
        }
        if let Meta::List(list) = &attribute.meta {
            collect_macro_literals(list.tokens.clone(), &mut self.literals);
        }
    }

    fn visit_lit_str(&mut self, literal: &'ast LitStr) {
        self.literals
            .push((literal.span().start().line, literal.value()));
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        // `format!`, `info!` and friends keep their arguments as tokens, not as typed literals.
        collect_macro_literals(mac.tokens.clone(), &mut self.literals);
    }
}

fn collect_macro_literals(stream: TokenStream, literals: &mut Vec<(usize, String)>) {
    for tree in stream {
        match tree {
            TokenTree::Group(group) => collect_macro_literals(group.stream(), literals),
            TokenTree::Literal(literal) => {
                if let Lit::Str(literal) = Lit::new(literal) {
                    literals.push((literal.span().start().line, literal.value()));
                }
            }
            _ => {}
        }
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

fn is_identifier_character(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

/// Whether a `#[cfg(...)]` attribute is exactly `#[cfg(test)]`.
fn is_cfg_test(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        if !attribute.path().is_ident("cfg") {
            return false;
        }
        let Meta::List(list) = &attribute.meta else {
            return false;
        };
        list.tokens.to_string().trim() == "test"
    })
}

#[cfg(test)]
mod tests {
    use std::fs::{create_dir_all, write};
    use std::panic::catch_unwind;

    use tempfile::tempdir;

    use super::*;

    fn write_crate(files: &[(&str, &str)], manifest: Option<&str>) -> tempfile::TempDir {
        let directory = tempdir().expect("temp dir is created");
        if let Some(manifest) = manifest {
            write(directory.path().join("Cargo.toml"), manifest).expect("manifest is written");
        }
        for (path, text) in files {
            let path = directory.path().join("src").join(path);
            create_dir_all(path.parent().expect("a source file has a directory"))
                .expect("source directory is created");
            write(path, text).expect("source file is written");
        }
        directory
    }

    const MANIFEST: &str = "[package]\nname = \"test-crate\"\n\n[dependencies]\nserde = \"1\"\n";

    #[test]
    fn layered_modules_pass() {
        let directory = write_crate(
            &[
                ("lib.rs", "mod a;\nmod b;\n"),
                ("a.rs", "use crate::b::Thing;\n"),
                ("b/mod.rs", "pub struct Thing;\n"),
            ],
            None,
        );
        assert_acyclic_modules(directory.path());
    }

    #[test]
    fn grouped_crate_imports_are_followed() {
        let directory = write_crate(
            &[
                ("lib.rs", "mod a;\nmod b;\n"),
                ("a.rs", "use crate::{b::Thing};\n"),
                ("b.rs", "use crate::{a::Other};\npub struct Thing;\n"),
            ],
            None,
        );
        let panic = catch_unwind(|| assert_acyclic_modules(directory.path()));
        assert!(panic.is_err(), "the grouped imports form a cycle");
    }

    #[test]
    fn imports_in_tests_are_ignored() {
        let directory = write_crate(
            &[
                ("main.rs", "mod a;\nmod b;\n"),
                ("a.rs", "use crate::b::Thing;\n"),
                (
                    "b.rs",
                    "pub struct Thing;\n#[cfg(test)]\nmod tests {\nuse crate::a;\n}\n",
                ),
            ],
            None,
        );
        assert_acyclic_modules(directory.path());
    }

    #[test]
    #[should_panic(expected = "module import cycle")]
    fn cycle_panics() {
        let directory = write_crate(
            &[
                ("lib.rs", "mod a;\nmod b;\n"),
                ("a.rs", "use crate::b::Thing;\npub struct Other;\n"),
                ("b.rs", "pub use crate::a::Other;\npub struct Thing;\n"),
            ],
            None,
        );
        assert_acyclic_modules(directory.path());
    }

    #[test]
    fn three_groups_in_order_pass() {
        let directory = write_crate(
            &[(
                "lib.rs",
                "use std::path::Path;\n\nuse serde::Serialize;\n\nuse crate::Thing;\n",
            )],
            Some(MANIFEST),
        );
        assert_grouped_imports(directory.path());
    }

    #[test]
    #[should_panic(expected = "must be grouped")]
    fn mixed_group_fails() {
        let directory = write_crate(
            &[(
                "lib.rs",
                "use serde::Serialize;\nuse std::path::Path;\n\nuse crate::Thing;\n",
            )],
            Some(MANIFEST),
        );
        assert_grouped_imports(directory.path());
    }

    #[test]
    #[should_panic(expected = "must be grouped")]
    fn split_group_fails() {
        let directory = write_crate(
            &[(
                "lib.rs",
                "use std::path::Path;\n\nuse std::sync::Arc;\n\nuse crate::Thing;\n",
            )],
            Some(MANIFEST),
        );
        assert_grouped_imports(directory.path());
    }

    #[test]
    #[should_panic(expected = "must be grouped")]
    fn groups_out_of_order_fail() {
        let directory = write_crate(
            &[("lib.rs", "use crate::Thing;\n\nuse std::path::Path;\n")],
            Some(MANIFEST),
        );
        assert_grouped_imports(directory.path());
    }

    #[test]
    fn visibility_and_child_modules_count() {
        let directory = write_crate(
            &[(
                "lib.rs",
                "pub use std::path::Path;\n\npub(crate) use crate::Thing;\n",
            )],
            Some(MANIFEST),
        );
        assert_grouped_imports(directory.path());
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
    fn consistent_format_strings_pass_including_build_script() {
        let directory = write_crate(
            &[("lib.rs", "fn f(x: u8) -> String { format!(\"{x}\") }\n")],
            Some(MANIFEST),
        );
        write(
            directory.path().join("build.rs"),
            "fn main() { let _ = format!(\"{} {}\", 1, 2); }\n",
        )
        .expect("build script is written");
        assert_consistent_format_strings(directory.path());
    }

    #[test]
    #[should_panic(expected = "format strings must not mix")]
    fn macro_format_string_panics() {
        let directory = write_crate(
            &[(
                "lib.rs",
                "fn f(x: u8) { let _ = format!(\"{} {x}\", x); }\n",
            )],
            Some(MANIFEST),
        );
        assert_consistent_format_strings(directory.path());
    }

    #[test]
    fn doc_comments_are_ignored() {
        let directory = write_crate(
            &[("lib.rs", "/// {} {name}\nfn f() { let _ = 1; }\n")],
            Some(MANIFEST),
        );
        assert_consistent_format_strings(directory.path());
    }

    #[test]
    #[should_panic(expected = "format strings must not mix")]
    fn attribute_format_string_panics() {
        let directory = write_crate(
            &[("lib.rs", "#[error(\"{} {name}\")]\nfn f() {}\n")],
            Some(MANIFEST),
        );
        assert_consistent_format_strings(directory.path());
    }

    #[test]
    #[should_panic(expected = "format strings must not mix")]
    fn raw_format_string_panics() {
        let directory = write_crate(
            &[("lib.rs", "fn f() -> &'static str { r#\"{} {name}\"# }\n")],
            Some(MANIFEST),
        );
        assert_consistent_format_strings(directory.path());
    }
}
