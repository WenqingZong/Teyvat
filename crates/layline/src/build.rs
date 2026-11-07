//! Which commit and cargo profile this binary was built from.

/// Release builds push alerts through Bark and refuse to start without it; debug builds log them instead.
pub const RELEASE_BUILD: bool = !cfg!(debug_assertions);

/// The full hash of the commit checked out when layline was built, set by `build.rs`. Uncommitted
/// changes in the working tree are not reflected.
pub const GIT_COMMIT_HASH: &str = env!("GIT_COMMIT_HASH");

/// `"release"` or `"debug"`, as reported by every service's `GET /info`.
pub fn build_profile() -> &'static str {
    if RELEASE_BUILD { "release" } else { "debug" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_matches_release_flag() {
        let expected = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        assert_eq!(expected, build_profile());
    }

    #[test]
    fn commit_hash_is_a_full_hexadecimal_hash() {
        /// A SHA-1 object name; a SHA-256 repository would have 64.
        const SHA1_HEX_LENGTH: usize = 40;
        assert_eq!(SHA1_HEX_LENGTH, GIT_COMMIT_HASH.len());
        assert!(
            GIT_COMMIT_HASH
                .chars()
                .all(|character| character.is_ascii_hexdigit()),
            "{GIT_COMMIT_HASH}"
        );
    }
}
