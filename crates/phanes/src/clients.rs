//! The clients of phanes.toml and who is calling: every service knows a client by the public key
//! it signs that service's handshakes with. In release builds the IP filter also names the client
//! `CF-Connecting-IP` belongs to, and both must be the same client. `PhanesConfig::load` has
//! checked that no address and no public key belongs to two clients.

use std::collections::BTreeMap;
use std::iter::once;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use secrecy::{ExposeSecret, SecretBox, SecretString};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer};

use crate::service::Service;
use layline::gnosis::GnosisId;
use layline::noise::{StaticKey, decode_key};

/// A `[clients.<name>]` table of phanes.toml.
pub type ClientName = String;

/// Every `[clients.<name>]` table, shared by the IP filter and every service.
pub type Clients = BTreeMap<ClientName, ClientConfig>;

/// A `[clients.<name>]` table: where the client calls from, the public key it uses for each
/// service, and the gnoses it may fetch.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// The addresses Cloudflare reports in `CF-Connecting-IP`.
    pub ipv4: Ipv4Addr,
    pub ipv6: Option<Ipv6Addr>,
    #[serde(deserialize_with = "deserialize_public_key")]
    pub client_gnosis_service_public_key: SecretBox<StaticKey>,
    #[serde(deserialize_with = "deserialize_public_key")]
    pub client_celestia_service_public_key: SecretBox<StaticKey>,
    /// May be empty: such a client only uses Celestia.
    pub allowed_gnosis_ids: Vec<GnosisId>,
}

impl ClientConfig {
    pub fn addresses(&self) -> impl Iterator<Item = IpAddr> {
        once(IpAddr::V4(self.ipv4)).chain(self.ipv6.map(IpAddr::V6))
    }

    pub fn public_key(&self, service: Service) -> &SecretBox<StaticKey> {
        match service {
            Service::Gnosis => &self.client_gnosis_service_public_key,
            Service::Celestia => &self.client_celestia_service_public_key,
        }
    }
}

/// A base64 X25519 public key, refused when parsing if it does not decode.
fn deserialize_public_key<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<SecretBox<StaticKey>, D::Error> {
    let encoded_public_key = SecretString::deserialize(deserializer)?;
    decode_key(&encoded_public_key)
        .map_err(|_| DeserializeError::custom("expected a base64 X25519 public key"))
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum IdentificationError {
    /// The key is not logged: public keys are kept from everyone outside the allow list.
    #[error("public key belongs to no client")]
    UnknownPublicKey,
    #[error(
        "signed with the key of client {key_client_name} but sent from an address of client {ip_client_name}"
    )]
    AddressOfAnotherClient {
        key_client_name: ClientName,
        ip_client_name: ClientName,
    },
}

/// The client that signed with `public_key` for `service`. `ip_client_name_option` is the client
/// the request's address belongs to, `None` when no IP filter runs (debug builds).
pub fn identify<'a>(
    clients: &'a Clients,
    service: Service,
    public_key: &SecretBox<StaticKey>,
    ip_client_name_option: Option<&ClientName>,
) -> Result<(&'a ClientName, &'a ClientConfig), IdentificationError> {
    let (key_client_name, client) = clients
        .iter()
        .find(|(_, client)| {
            client.public_key(service).expose_secret() == public_key.expose_secret()
        })
        .ok_or(IdentificationError::UnknownPublicKey)?;
    match ip_client_name_option {
        Some(ip_client_name) if ip_client_name != key_client_name => {
            Err(IdentificationError::AddressOfAnotherClient {
                key_client_name: key_client_name.clone(),
                ip_client_name: ip_client_name.clone(),
            })
        }
        _ => Ok((key_client_name, client)),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use layline::testing::test_keypair;

    const OSAKA: &str = "Osaka";
    const TOKYO: &str = "Tokyo";
    /// Arbitrary documentation address (RFC 5737) with no business meaning.
    const OSAKA_IPV4: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);

    /// A client at `ipv4` with fresh public keys, allowed no gnosis; tests replace what they need.
    pub(crate) fn test_client(ipv4: Ipv4Addr) -> ClientConfig {
        ClientConfig {
            ipv4,
            ipv6: None,
            client_gnosis_service_public_key: test_keypair().1,
            client_celestia_service_public_key: test_keypair().1,
            allowed_gnosis_ids: Vec::new(),
        }
    }

    #[test]
    fn client_is_known_by_its_key_for_the_service_and_must_match_its_address() {
        let osaka = ClientName::from(OSAKA);
        let tokyo = ClientName::from(TOKYO);
        let clients = Clients::from([(osaka.clone(), test_client(OSAKA_IPV4))]);
        let gnosis_public_key = clients[OSAKA].public_key(Service::Gnosis);
        let identified_name = |service, ip_client_name_option| {
            identify(&clients, service, gnosis_public_key, ip_client_name_option)
                .map(|(client_name, _)| client_name.clone())
        };

        assert_eq!(Ok(osaka.clone()), identified_name(Service::Gnosis, None));
        assert_eq!(
            Ok(osaka.clone()),
            identified_name(Service::Gnosis, Some(&osaka))
        );
        assert_eq!(
            Err(IdentificationError::AddressOfAnotherClient {
                key_client_name: osaka.clone(),
                ip_client_name: tokyo.clone(),
            }),
            identified_name(Service::Gnosis, Some(&tokyo))
        );
        assert_eq!(
            Err(IdentificationError::UnknownPublicKey),
            identified_name(Service::Celestia, None)
        );
    }
}
