use std::path::Path;

use layline::testing::assert_acyclic_modules;

#[test]
fn module_dependencies_are_acyclic() {
    assert_acyclic_modules(Path::new(env!("CARGO_MANIFEST_DIR")));
}
