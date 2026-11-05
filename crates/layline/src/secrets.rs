//! Secret files on disk: config files holding credentials and private keys, all chmod 600.

use std::fs::{metadata, read_to_string};
use std::io::Error as IoError;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use secrecy::{ExposeSecret, SecretString};
use serde::de::DeserializeOwned;
use toml::de::Error as TomlError;

/// Any of these bits set means group or others can access the file; secrets must be chmod 600.
const GROUP_AND_OTHERS_PERMISSION_BITS: u32 = 0o077;

#[derive(Debug, thiserror::Error)]
pub enum SecretFileError {
    #[error("cannot read {path}: {source}")]
    Read { path: PathBuf, source: IoError },
    #[error("{0} must not be readable by group or others (chmod 600)")]
    InsecurePermissions(PathBuf),
    #[error("invalid {}: {description}", path.display())]
    InvalidToml { path: PathBuf, description: String },
}

/// Refuses a file that group or others can access.
pub fn read_secret_file(path: &Path) -> Result<SecretString, SecretFileError> {
    let read_error = |source| SecretFileError::Read {
        path: path.to_owned(),
        source,
    };
    let mode = metadata(path).map_err(read_error)?.permissions().mode();
    if mode & GROUP_AND_OTHERS_PERMISSION_BITS != 0 {
        return Err(SecretFileError::InsecurePermissions(path.to_owned()));
    }
    Ok(SecretString::from(
        read_to_string(path).map_err(read_error)?,
    ))
}

/// Reads a chmod 600 TOML file into `Config`. A parse error never quotes the file, whose lines
/// may hold secrets.
pub fn read_secret_toml<Config: DeserializeOwned>(path: &Path) -> Result<Config, SecretFileError> {
    let toml_content = read_secret_file(path)?;
    toml::from_str(toml_content.expose_secret()).map_err(|mut error: TomlError| {
        // Without its input, toml leaves out the quoted line and names the key path instead.
        error.set_input(None);
        SecretFileError::InvalidToml {
            path: path.to_owned(),
            description: error.to_string().trim_end().replace('\n', " "),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::fs::{Permissions, set_permissions};

    use serde::Deserialize;
    use tempfile::tempdir;

    use super::*;
    use crate::testing::write_secret_file;

    const GROUP_READABLE_MODE: u32 = 0o640;
    const SECRET_VALUE: &str = "needle";

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Credentials {
        secret: SecretString,
    }

    #[test]
    fn reads_owner_only_file() {
        let directory = tempdir().expect("temp dir is created");
        let path = directory.path().join("secret.txt");
        write_secret_file(&path, SECRET_VALUE);
        let secret = read_secret_file(&path).expect("owner-only file is read");
        assert_eq!(SECRET_VALUE, secret.expose_secret());
    }

    #[test]
    fn refuses_file_others_can_read() {
        let directory = tempdir().expect("temp dir is created");
        let path = directory.path().join("secret.txt");
        write_secret_file(&path, SECRET_VALUE);
        set_permissions(&path, Permissions::from_mode(GROUP_READABLE_MODE))
            .expect("permissions are set");
        assert!(matches!(
            read_secret_file(&path),
            Err(SecretFileError::InsecurePermissions(_))
        ));
    }

    #[test]
    fn missing_file_is_a_read_error() {
        let directory = tempdir().expect("temp dir is created");
        assert!(matches!(
            read_secret_file(&directory.path().join("missing.txt")),
            Err(SecretFileError::Read { .. })
        ));
    }

    #[test]
    fn reads_owner_only_toml() {
        let directory = tempdir().expect("temp dir is created");
        let path = directory.path().join("config.toml");
        write_secret_file(&path, &format!("secret = \"{SECRET_VALUE}\"\n"));
        let credentials: Credentials = read_secret_toml(&path).expect("the fields match");
        assert_eq!(SECRET_VALUE, credentials.secret.expose_secret());
    }

    #[test]
    fn invalid_toml_names_the_field_but_never_quotes_the_file() {
        let directory = tempdir().expect("temp dir is created");
        let path = directory.path().join("config.toml");
        write_secret_file(
            &path,
            &format!("secret = \"{SECRET_VALUE}\"\nmisspelled_secret = \"{SECRET_VALUE}\"\n"),
        );
        let error = read_secret_toml::<Credentials>(&path)
            .expect_err("an unknown field is expected to be refused");
        let message = error.to_string();
        assert!(message.contains("misspelled_secret"), "{message}");
        assert!(!message.contains(SECRET_VALUE), "{message}");
    }
}
