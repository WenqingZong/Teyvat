//! Names of the services behind the IP filter, for logs and push notifications.

/// The services behind the IP filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    Gnosis,
    Celestia,
}

/// Logs are English, push notifications Chinese.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    English,
    Chinese,
}

impl Service {
    pub fn to_string(self, language: Language) -> String {
        match (self, language) {
            (Self::Gnosis, Language::English) => "Gnosis",
            (Self::Gnosis, Language::Chinese) => "神之心请求",
            (Self::Celestia, Language::English) => "Celestia",
            (Self::Celestia, Language::Chinese) => "天空岛监控连接",
        }
        .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_names_in_both_languages() {
        assert_eq!(Service::Gnosis.to_string(Language::English), "Gnosis");
        assert_eq!(Service::Gnosis.to_string(Language::Chinese), "神之心请求");
        assert_eq!(Service::Celestia.to_string(Language::English), "Celestia");
        assert_eq!(
            Service::Celestia.to_string(Language::Chinese),
            "天空岛监控连接"
        );
    }
}
