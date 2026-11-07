//! Bakes the commit being built into the `GIT_COMMIT_HASH` environment variable, which
//! `layline::build::GIT_COMMIT_HASH` reads at compile time. A build outside a git checkout fails.

use std::io::{Write, stdout};
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let commit_hash = git(&["rev-parse", "HEAD"]);
    let git_directory = PathBuf::from(git(&["rev-parse", "--absolute-git-dir"]));
    let common_directory = PathBuf::from(git(&[
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
    ]));
    // HEAD moves when a checkout rewrites `HEAD` or a commit rewrites the branch's ref, which is a
    // loose file under `refs/heads` or a line of `packed-refs`. A path that does not exist would
    // make cargo rerun this script on every build, so only existing ones are watched.
    let watched_paths = [
        git_directory.join("HEAD"),
        common_directory.join("refs").join("heads"),
        common_directory.join("packed-refs"),
    ];
    let mut directives = stdout().lock();
    for path in watched_paths.iter().filter(|path| path.exists()) {
        writeln!(directives, "cargo::rerun-if-changed={}", path.display())
            .expect("cargo reads build script directives from stdout");
    }
    writeln!(directives, "cargo::rustc-env=GIT_COMMIT_HASH={commit_hash}")
        .expect("cargo reads build script directives from stdout");
}

/// Runs git in this crate's directory and returns its trimmed stdout.
fn git(arguments: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args(arguments)
        .output()
        .expect("git must be installed to build layline");
    assert!(
        output.status.success(),
        "git {} failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git prints UTF-8")
        .trim()
        .to_owned()
}
