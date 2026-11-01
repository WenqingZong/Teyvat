use std::path::Path;

use layline::testing::assert_grouped_imports;

#[test]
fn imports_are_grouped() {
    assert_grouped_imports(Path::new(env!("CARGO_MANIFEST_DIR")));
}
