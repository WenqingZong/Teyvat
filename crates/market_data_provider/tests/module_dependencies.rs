//! Guard against module-level import cycles, which Rust accepts but which erase layering.
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("source directory is expected to be readable") {
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

fn top_level_modules(lib: &str) -> BTreeSet<String> {
    lib.lines()
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

#[test]
fn module_dependencies_are_acyclic() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let lib = fs::read_to_string(source.join("lib.rs")).expect("lib.rs is expected to be readable");
    let modules = top_level_modules(&lib);
    let mut files = Vec::new();
    rust_files(&source, &mut files);

    let mut edges: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let module = module_name(&path, &source);
        if module == "crate" {
            continue;
        }
        let top = module.split('/').next().expect("a module has a name");
        let text = fs::read_to_string(&path).expect("source file is expected to be readable");
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
