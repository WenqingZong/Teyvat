//! Gnosis (API key release) wire protocol, shared by Phanes and its client. One HTTP round trip:
//!
//! ```text
//! client -> server   POST /gnosis   body = Noise message 1 (e, es, ss), payload = Request
//! server -> client   200            body = Noise message 2 (e, ee, se), payload = Response
//! ```
//!
//! A request that fails the handshake gets an empty 404; an authenticated client gets an
//! encrypted `Response` either way.

use hex::encode as encode_hex;
use rand::fill;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Error as JsonError, from_slice, to_vec};
use snow::{Error as NoiseError, HandshakeState};
use std::num::NonZeroU64;

use crate::noise::{SecretBuffer, StaticKey, build_handshake, decrypt, encrypt};

/// Mixed into the handshake hash so messages can't be reused by another service.
pub const GNOSIS_PROLOGUE: &[u8] = b"gnosis";
pub const GNOSIS_PATH: &str = "/gnosis";
pub const NONCE_LENGTH: usize = 16;
const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub key_id: String,
    /// Unix seconds on the client.
    pub timestamp: u64,
    /// Random per-request value, hex-encoded `NONCE_LENGTH` bytes.
    pub nonce: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Granted {
        #[serde(serialize_with = "serialize_secret")]
        secret: SecretString,
    },
    Denied {
        reason: String,
    },
}

/// `SecretString` refuses to serialize on its own; only the Noise payload may expose it.
fn serialize_secret<S: Serializer>(
    secret: &SecretString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(secret.expose_secret())
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
    payload: SecretBuffer,
}

impl Responder {
    /// Fails if `first_message` was not produced by the holder of `client_public_key`'s
    /// private key. On success the client is authenticated, even if the payload is junk.
    pub fn read_request(
        gnosis_private_key: &SecretBox<StaticKey>,
        client_public_key: &StaticKey,
        first_message: &[u8],
    ) -> Result<Self, GnosisError> {
        let mut state = build_handshake(
            GNOSIS_PROLOGUE,
            gnosis_private_key,
            client_public_key,
            false,
        )?;
        let payload = decrypt(&mut state, first_message)?;
        Ok(Self { state, payload })
    }

    /// Parses the authenticated payload of message 1.
    pub fn parse_request(&self) -> Result<Request, GnosisError> {
        Ok(from_slice(self.payload.expose_secret())?)
    }

    pub fn write_response(mut self, response: &Response) -> Result<Vec<u8>, GnosisError> {
        let payload = SecretBox::new(Box::new(to_vec(response)?));
        Ok(encrypt(&mut self.state, payload.expose_secret())?)
    }
}

/// Client side: build the first message, then decrypt the reply with the same state.
pub struct Initiator(HandshakeState);

impl Initiator {
    pub fn write_request(
        client_private_key: &SecretBox<StaticKey>,
        gnosis_public_key: &StaticKey,
        request: &Request,
    ) -> Result<(Self, Vec<u8>), GnosisError> {
        let mut state =
            build_handshake(GNOSIS_PROLOGUE, client_private_key, gnosis_public_key, true)?;
        let first_message = encrypt(&mut state, &to_vec(request)?)?;
        Ok((Self(state), first_message))
    }

    pub fn read_response(mut self, second_message: &[u8]) -> Result<Response, GnosisError> {
        let payload = decrypt(&mut self.0, second_message)?;
        Ok(from_slice(payload.expose_secret())?)
    }
}

impl Request {
    /// A request for `key_id` stamped with `now`, in Unix nanoseconds, and a fresh nonce.
    pub fn new(key_id: &str, now: NonZeroU64) -> Self {
        let mut nonce = [0u8; NONCE_LENGTH];
        fill(&mut nonce);
        Self {
            key_id: key_id.to_owned(),
            timestamp: now.get() / NANOSECONDS_PER_SECOND,
            nonce: encode_hex(nonce),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::test_keypair;

    /// 2026-10-24 00:00:00.5 UTC.
    const NOW: NonZeroU64 = NonZeroU64::new(1_792_800_000_500_000_000).expect("not zero");

    #[test]
    fn round_trip() {
        let (gnosis_private_key, gnosis_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let request = Request::new("binance_demo_trading", NOW);

        let (initiator, first_message) =
            Initiator::write_request(&client_private_key, &gnosis_public_key, &request)
                .expect("client writes request");
        let responder =
            Responder::read_request(&gnosis_private_key, &client_public_key, &first_message)
                .expect("server authenticates client");
        assert_eq!(responder.parse_request().expect("payload parses"), request);
        assert_eq!(2 * NONCE_LENGTH, request.nonce.len());
        assert_eq!(1_792_800_000, request.timestamp);

        let second_message = responder
            .write_response(&Response::Granted {
                secret: SecretString::from("dummy"),
            })
            .expect("server writes response");
        match initiator
            .read_response(&second_message)
            .expect("client reads response")
        {
            Response::Granted { secret } => assert_eq!(secret.expose_secret(), "dummy"),
            other => panic!("unexpected response {other:?}"),
        }
    }

    #[test]
    fn request_is_not_plaintext() {
        let (_, gnosis_public_key) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let (_, first_message) = Initiator::write_request(
            &client_private_key,
            &gnosis_public_key,
            &Request::new("needle_key_id", NOW),
        )
        .expect("client writes request");
        assert!(!first_message.windows(6).any(|window| window == b"needle"));
    }

    #[test]
    fn unknown_client_is_rejected() {
        let (gnosis_private_key, gnosis_public_key) = test_keypair();
        let (_, client_public_key) = test_keypair();
        let (impostor_private_key, _) = test_keypair();
        let (_, first_message) = Initiator::write_request(
            &impostor_private_key,
            &gnosis_public_key,
            &Request::new("key", NOW),
        )
        .expect("impostor writes request");
        assert!(
            Responder::read_request(&gnosis_private_key, &client_public_key, &first_message)
                .is_err()
        );
    }

    #[test]
    fn response_from_wrong_server_is_rejected() {
        let (_, gnosis_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let (impostor_private_key, _) = test_keypair();
        let (initiator, first_message) = Initiator::write_request(
            &client_private_key,
            &gnosis_public_key,
            &Request::new("key", NOW),
        )
        .expect("client writes request");
        assert!(
            Responder::read_request(&impostor_private_key, &client_public_key, &first_message)
                .is_err()
        );
        assert!(initiator.read_response(b"junk").is_err());
    }

    #[test]
    fn response_debug_hides_secret() {
        let response = Response::Granted {
            secret: SecretString::from("needle"),
        };
        assert!(!format!("{response:?}").contains("needle"));
    }
}
