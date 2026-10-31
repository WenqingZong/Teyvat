//! Which cargo profile this binary was built with.

/// Release builds push alerts through Bark and refuse to start without it; debug builds log them instead.
pub const RELEASE_BUILD: bool = !cfg!(debug_assertions);

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
}
