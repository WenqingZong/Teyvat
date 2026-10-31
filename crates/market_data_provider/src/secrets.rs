use anyhow::{Context, Result, bail};
use secrecy::SecretString;
use std::fs::{metadata, read_to_string};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Any of these bits set means group or others can access the file; secrets must be chmod 600.
const GROUP_AND_OTHERS_PERMISSION_BITS: u32 = 0o077;

/// Refuses a file that group or others can access.
pub(crate) fn read_secret_file(path: &Path) -> Result<SecretString> {
    let mode = metadata(path)
        .with_context(|| format!("cannot read {}", path.display()))?
        .permissions()
        .mode();
    if mode & GROUP_AND_OTHERS_PERMISSION_BITS != 0 {
        bail!(
            "{} must not be readable by group or others (chmod 600)",
            path.display()
        );
    }
    let text = read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    Ok(SecretString::from(text))
}
