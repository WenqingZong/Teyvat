use std::path::Path;

use layline::testing::assert_consistent_format_strings;

#[test]
fn format_strings_use_one_placeholder_style() {
    assert_consistent_format_strings(Path::new(env!("CARGO_MANIFEST_DIR")));
}
