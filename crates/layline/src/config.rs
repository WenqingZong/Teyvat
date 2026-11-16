//! Small wrappers that make config structs print readably with `#[derive(Debug)]`.

use std::fmt::{Debug, Display, Formatter, Result as FormatResult};
use std::ops::Deref;

use serde::Deserialize;
use url::Url;

/// A URL that `Debug` prints as itself, so a config struct can derive `Debug` and still put the URL
/// on one line.
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct LogUrl(pub Url);

impl Debug for LogUrl {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        Display::fmt(&self.0, formatter)
    }
}

impl Display for LogUrl {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        Display::fmt(&self.0, formatter)
    }
}

impl Deref for LogUrl {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Url> for LogUrl {
    fn from(url: Url) -> Self {
        Self(url)
    }
}

impl From<LogUrl> for Url {
    fn from(url: LogUrl) -> Self {
        url.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "https://phanes.example/celestia";

    #[test]
    fn debug_prints_the_url_on_one_line() {
        let url = LogUrl(Url::parse(URL).expect("the test url is valid"));
        assert_eq!(URL, format!("{url}"));
        assert_eq!(URL, format!("{url:?}"));
    }

    #[test]
    fn a_log_url_deserializes_from_a_string() {
        let url: LogUrl = toml::from_str(&format!("url = \"{URL}\""))
            .map(|table: UrlTable| table.url)
            .expect("the url parses");
        assert_eq!(URL, url.as_str());
    }

    #[derive(Deserialize)]
    struct UrlTable {
        url: LogUrl,
    }
}
