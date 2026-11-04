//! Who is calling: every service knows its clients by the public key each signs its handshakes
//! with. In release builds the IP filter also names the client `CF-Connecting-IP` belongs to, and
//! both must be the same client.

use secrecy::{ExposeSecret, SecretBox};

use layline::noise::StaticKey;

/// A `[clients.<name>]` table of phanes.toml.
pub type ClientName = String;

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

/// The clients of one service, with the public key each uses for it. A handful of clients, so a
/// lookup just compares every key.
#[derive(Debug, Default)]
pub struct ClientsByPublicKey(Vec<(SecretBox<StaticKey>, ClientName)>);

impl ClientsByPublicKey {
    /// Adds a client. A service tells its clients apart by key, so a key another client already
    /// holds is refused, with that client's name.
    pub fn insert(
        &mut self,
        public_key: SecretBox<StaticKey>,
        client_name: ClientName,
    ) -> Result<(), ClientName> {
        if let Some(holder_name) = self.holder_of(&public_key) {
            return Err(holder_name.clone());
        }
        self.0.push((public_key, client_name));
        Ok(())
    }

    /// The client that holds `public_key`. `ip_client_name_option` is the client the request's
    /// address belongs to, `None` when no IP filter runs (debug builds).
    pub fn identify(
        &self,
        public_key: &SecretBox<StaticKey>,
        ip_client_name_option: Option<&ClientName>,
    ) -> Result<ClientName, IdentificationError> {
        let key_client_name = self
            .holder_of(public_key)
            .ok_or(IdentificationError::UnknownPublicKey)?;
        match ip_client_name_option {
            Some(ip_client_name) if ip_client_name != key_client_name => {
                Err(IdentificationError::AddressOfAnotherClient {
                    key_client_name: key_client_name.clone(),
                    ip_client_name: ip_client_name.clone(),
                })
            }
            _ => Ok(key_client_name.clone()),
        }
    }

    fn holder_of(&self, public_key: &SecretBox<StaticKey>) -> Option<&ClientName> {
        self.0
            .iter()
            .find(|(client_public_key, _)| {
                client_public_key.expose_secret() == public_key.expose_secret()
            })
            .map(|(_, client_name)| client_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use layline::noise::copy_key;
    use layline::testing::test_keypair;

    const OSAKA: &str = "Osaka";
    const TOKYO: &str = "Tokyo";

    #[test]
    fn client_is_known_by_its_key_and_must_match_its_address() {
        let (_, osaka_public_key) = test_keypair();
        let (_, stranger_public_key) = test_keypair();
        let osaka = ClientName::from(OSAKA);
        let tokyo = ClientName::from(TOKYO);
        let mut clients = ClientsByPublicKey::default();
        clients
            .insert(copy_key(&osaka_public_key), osaka.clone())
            .expect("the first client has a fresh key");
        assert_eq!(
            Err(osaka.clone()),
            clients.insert(copy_key(&osaka_public_key), tokyo.clone())
        );

        assert_eq!(Ok(osaka.clone()), clients.identify(&osaka_public_key, None));
        assert_eq!(
            Ok(osaka.clone()),
            clients.identify(&osaka_public_key, Some(&osaka))
        );
        assert_eq!(
            Err(IdentificationError::AddressOfAnotherClient {
                key_client_name: osaka.clone(),
                ip_client_name: tokyo.clone(),
            }),
            clients.identify(&osaka_public_key, Some(&tokyo))
        );
        assert_eq!(
            Err(IdentificationError::UnknownPublicKey),
            clients.identify(&stranger_public_key, Some(&osaka))
        );
    }
}
