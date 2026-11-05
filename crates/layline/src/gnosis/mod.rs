//! Gnosis, the API key release of Phanes: the wire protocol, both halves of it, shared by Phanes
//! and its clients, and every gnosis (API key) Phanes can release, one file per exchange. One
//! HTTP round trip:
//!
//! ```text
//! client -> server   POST /gnosis   body = Noise message 1 (e, es, s, ss), payload = Request
//! server -> client   200            body = Noise message 2 (e, ee, se), payload = Response
//! ```
//!
//! A request that fails the handshake, or whose key belongs to no known client, gets an empty
//! 404; an authenticated client gets an encrypted `Response` either way. Each `GnosisId` holds one
//! key type, and a client fetches a gnosis by its type, which picks the id.

pub mod binance;

use std::num::NonZeroU64;

use hex::encode as encode_hex;
use rand::fill;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Error as JsonError, from_slice, to_vec};
use snow::{Error as NoiseError, HandshakeState};
use strum::{Display, EnumIter, EnumString};

use crate::noise::{
    SecretBuffer, StaticKey, build_initiator, build_responder, decrypt, encrypt, read_first_message,
};
use crate::time::NANOSECONDS_PER_SECOND;

/// Mixed into the handshake hash so messages can't be reused by another service.
pub const GNOSIS_PROLOGUE: &[u8] = b"gnosis";
pub const GNOSIS_PATH: &str = "/gnosis";
pub const NONCE_LENGTH: usize = 16;

/// Every gnosis Phanes can release. Serde, `Display` and `FromStr` all spell it in snake_case, as
/// the `[gnoses]` table of phanes.toml does.
#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumIter, EnumString, Eq, Hash, PartialEq, Serialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GnosisId {
    BinanceReal,
    BinanceDemo,
    BinanceSbeReal,
    BinanceSbeDemo,
}

/// A key type, and the id of its live and demo gnosis, so a client cannot fetch one id as another type.
pub trait Gnosis: DeserializeOwned {
    fn gnosis_id(demo: bool) -> GnosisId;
}

/// `SecretString` refuses to serialize on its own; only the Noise payload may expose it.
fn serialize_secret<S: Serializer>(
    secret: &SecretString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(secret.expose_secret())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    /// An id Phanes does not know fails to parse, so the request is refused as malformed.
    pub gnosis_id: GnosisId,
    /// Unix seconds on the client.
    pub timestamp: u64,
    /// Random per-request value, hex-encoded `NONCE_LENGTH` bytes.
    pub nonce: String,
}

/// Phanes answers with the gnosis itself; the client reads it as the key type it asked for, so
/// the type parameter is whichever key type, a `Gnosis` of its own.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response<Gnosis> {
    Granted { gnosis: Gnosis },
    Denied { reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum GnosisError {
    #[error("noise: {0}")]
    Noise(#[from] NoiseError),
    #[error("payload: {0}")]
    Payload(#[from] JsonError),
}

/// Server side: one handshake per incoming request.
pub struct Responder {
    state: HandshakeState,
    client_public_key: SecretBox<StaticKey>,
    payload: SecretBuffer,
}

impl Responder {
    /// Fails unless `first_message` was written for Gnosis by the holder of the private half of
    /// the public key it carries. Whether that key belongs to a known client is for the caller to
    /// check, before acting on the request.
    pub fn read_request(
        gnosis_private_key: &SecretBox<StaticKey>,
        first_message: &[u8],
    ) -> Result<Self, GnosisError> {
        let mut state = build_responder(GNOSIS_PROLOGUE, gnosis_private_key)?;
        let (client_public_key, payload) = read_first_message(&mut state, first_message)?;
        Ok(Self {
            state,
            client_public_key,
            payload,
        })
    }

    /// The public key the client signed message 1 with.
    pub fn client_public_key(&self) -> &SecretBox<StaticKey> {
        &self.client_public_key
    }

    /// Parses the payload of message 1.
    pub fn parse_request(&self) -> Result<Request, GnosisError> {
        Ok(from_slice(self.payload.expose_secret())?)
    }

    pub fn write_response<Gnosis: Serialize>(
        mut self,
        response: &Response<Gnosis>,
    ) -> Result<Vec<u8>, GnosisError> {
        let payload = SecretBox::new(Box::new(to_vec(response)?));
        Ok(encrypt(&mut self.state, payload.expose_secret())?)
    }
}

/// Client side: build the first message, then decrypt the reply with the same state.
pub struct Initiator(HandshakeState);

impl Initiator {
    pub fn write_request(
        client_private_key: &SecretBox<StaticKey>,
        gnosis_public_key: &SecretBox<StaticKey>,
        request: &Request,
    ) -> Result<(Self, Vec<u8>), GnosisError> {
        let mut state = build_initiator(GNOSIS_PROLOGUE, client_private_key, gnosis_public_key)?;
        let first_message = encrypt(&mut state, &to_vec(request)?)?;
        Ok((Self(state), first_message))
    }

    /// A granted gnosis must have exactly the fields of `Gnosis`.
    pub fn read_response<Gnosis: DeserializeOwned>(
        mut self,
        second_message: &[u8],
    ) -> Result<Response<Gnosis>, GnosisError> {
        let payload = decrypt(&mut self.0, second_message)?;
        Ok(from_slice(payload.expose_secret())?)
    }
}

impl Request {
    /// A request for `gnosis_id` stamped with `now`, in Unix nanoseconds, and a fresh nonce.
    pub fn new(gnosis_id: GnosisId, now: NonZeroU64) -> Self {
        let mut nonce = [0u8; NONCE_LENGTH];
        fill(&mut nonce);
        Self {
            gnosis_id,
            timestamp: now.get() / NANOSECONDS_PER_SECOND,
            nonce: encode_hex(nonce),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::{from_str, to_string};
    use strum::IntoEnumIterator;

    use super::*;
    use crate::gnosis::binance::{BinanceApiKey, BinanceSbeApiKey};
    use crate::testing::test_keypair;

    /// 2026-10-24 00:00:00.5 UTC.
    const NOW: NonZeroU64 = NonZeroU64::new(1_792_800_000_500_000_000).expect("not zero");
    const API_KEY: &str = "dummy-api-key";
    const SECRET_KEY: &str = "dummy-secret-key";

    fn binance_api_key() -> BinanceApiKey {
        from_str(&format!(
            r#"{{"api_key": "{API_KEY}", "secret_key": "{SECRET_KEY}"}}"#
        ))
        .expect("the key parses")
    }

    /// Message 1 from a fresh client, and the server's view of it.
    fn request_round(gnosis_id: GnosisId) -> (Initiator, Responder, SecretBox<StaticKey>, Request) {
        let (gnosis_private_key, gnosis_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let request = Request::new(gnosis_id, NOW);
        let (initiator, first_message) =
            Initiator::write_request(&client_private_key, &gnosis_public_key, &request)
                .expect("client writes request");
        let responder = Responder::read_request(&gnosis_private_key, &first_message)
            .expect("server reads request");
        (initiator, responder, client_public_key, request)
    }

    #[test]
    fn granted_gnosis_reaches_the_client_as_its_type() {
        let (initiator, responder, client_public_key, request) =
            request_round(GnosisId::BinanceDemo);
        assert_eq!(
            client_public_key.expose_secret(),
            responder.client_public_key().expose_secret()
        );
        assert_eq!(responder.parse_request().expect("payload parses"), request);
        assert_eq!(2 * NONCE_LENGTH, request.nonce.len());
        assert_eq!(1_792_800_000, request.timestamp);

        let second_message = responder
            .write_response(&Response::Granted {
                gnosis: binance_api_key(),
            })
            .expect("server writes response");
        match initiator
            .read_response::<BinanceApiKey>(&second_message)
            .expect("client reads response")
        {
            Response::Granted { gnosis } => {
                assert_eq!(API_KEY, gnosis.api_key().expose_secret());
                assert_eq!(SECRET_KEY, gnosis.secret_key().expose_secret());
            }
            Response::Denied { reason } => panic!("unexpected denial: {reason}"),
        }
    }

    #[test]
    fn gnosis_of_another_type_is_refused_without_quoting_it() {
        let (initiator, responder, _, _) = request_round(GnosisId::BinanceSbeDemo);
        let second_message = responder
            .write_response(&Response::Granted {
                gnosis: binance_api_key(),
            })
            .expect("server writes response");
        let message = initiator
            .read_response::<BinanceSbeApiKey>(&second_message)
            .expect_err("a trading key is not an SBE key")
            .to_string();
        assert!(message.contains("secret_key"), "{message}");
        assert!(!message.contains(SECRET_KEY), "{message}");
    }

    #[test]
    fn request_is_not_plaintext() {
        let (_, gnosis_public_key) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let (_, first_message) = Initiator::write_request(
            &client_private_key,
            &gnosis_public_key,
            &Request::new(GnosisId::BinanceSbeReal, NOW),
        )
        .expect("client writes request");
        let gnosis_id = b"binance_sbe_real";
        assert!(
            !first_message
                .windows(gnosis_id.len())
                .any(|window| window == gnosis_id)
        );
    }

    #[test]
    fn response_from_wrong_server_is_rejected() {
        let (_, gnosis_public_key) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let (impostor_private_key, _) = test_keypair();
        let (initiator, first_message) = Initiator::write_request(
            &client_private_key,
            &gnosis_public_key,
            &Request::new(GnosisId::BinanceDemo, NOW),
        )
        .expect("client writes request");
        assert!(Responder::read_request(&impostor_private_key, &first_message).is_err());
        assert!(initiator.read_response::<BinanceApiKey>(b"junk").is_err());
    }

    #[test]
    fn debug_hides_the_gnosis() {
        let response = Response::Granted {
            gnosis: binance_api_key(),
        };
        let debug = format!("{response:?}");
        assert!(!debug.contains(API_KEY), "{debug}");
        assert!(!debug.contains(SECRET_KEY), "{debug}");
    }

    #[test]
    fn gnosis_id_spellings_agree() {
        for gnosis_id in GnosisId::iter() {
            let name = gnosis_id.to_string();
            assert_eq!(
                format!("\"{name}\""),
                to_string(&gnosis_id).expect("serializes")
            );
            assert_eq!(Ok(gnosis_id), GnosisId::from_str(&name));
        }
        assert_eq!("binance_sbe_demo", GnosisId::BinanceSbeDemo.to_string());
    }
}
