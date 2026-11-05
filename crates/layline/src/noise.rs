//! End-to-end encryption for every Phanes service and its clients. Cloudflare terminates TLS, so
//! each service encrypts at the application layer with `Noise_IK`: the client knows the service's
//! static X25519 public key in advance and sends its own, encrypted, in message 1; the service
//! learns who is calling from that key and checks it against the clients it knows. Each service
//! uses its own key pair and prologue. Public keys are held as secrets too: they are harmless if
//! leaked, but nobody outside the allow list should learn them.

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use secrecy::{ExposeSecret, ExposeSecretMut, SecretBox, SecretString};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer};
use snow::params::NoiseParams;
use snow::{Builder, Error as NoiseError, HandshakeState, Keypair, TransportState};

pub const NOISE_PARAMETERS: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";
pub const KEY_LENGTH: usize = 32;
pub const MAX_NOISE_MESSAGE_LENGTH: usize = 65535;

/// An X25519 key, private or public; always held in a `SecretBox`.
pub type StaticKey = [u8; KEY_LENGTH];
/// Plaintext may carry API keys, so it is zeroed on drop.
pub type SecretBuffer = SecretBox<Vec<u8>>;

#[derive(Debug, thiserror::Error)]
#[error("invalid key encoding")]
pub struct InvalidKeyError;

pub fn parse_noise_parameters() -> NoiseParams {
    NOISE_PARAMETERS
        .parse()
        .expect("NOISE_PARAMETERS is a valid Noise pattern")
}

pub fn generate_keypair() -> Result<Keypair, NoiseError> {
    Builder::new(parse_noise_parameters()).generate_keypair()
}

pub fn encode_key(key: &[u8]) -> String {
    BASE64.encode(key)
}

/// Decodes a base64 key, private or public, without leaving a plain copy of it behind.
pub fn decode_key(encoded: &SecretString) -> Result<SecretBox<StaticKey>, InvalidKeyError> {
    let bytes = secret_buffer(
        BASE64
            .decode(encoded.expose_secret().trim())
            .map_err(|_| InvalidKeyError)?,
    );
    if bytes.expose_secret().len() != KEY_LENGTH {
        return Err(InvalidKeyError);
    }
    Ok(secret_key_from_bytes(bytes.expose_secret()))
}

/// `bytes` must be `KEY_LENGTH` long.
/// For `#[serde(deserialize_with = "deserialize_key")]`: a base64 key, private or public, written
/// in a config; one that does not decode fails the parse.
pub fn deserialize_key<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<SecretBox<StaticKey>, D::Error> {
    let encoded_key = SecretString::deserialize(deserializer)?;
    decode_key(&encoded_key).map_err(|_| DeserializeError::custom("expected a base64 X25519 key"))
}

fn secret_key_from_bytes(bytes: &[u8]) -> SecretBox<StaticKey> {
    SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(bytes))
}

fn secret_buffer(bytes: Vec<u8>) -> SecretBuffer {
    SecretBox::new(Box::new(bytes))
}

/// The client's half: it must know the service's public key. Every service passes its own
/// prologue, so a handshake for one service fails on another.
pub fn build_initiator(
    prologue: &[u8],
    client_private_key: &SecretBox<StaticKey>,
    service_public_key: &SecretBox<StaticKey>,
) -> Result<HandshakeState, NoiseError> {
    Builder::new(parse_noise_parameters())
        .prologue(prologue)?
        .local_private_key(client_private_key.expose_secret())?
        .remote_public_key(service_public_key.expose_secret())?
        .build_initiator()
}

/// The service's half: the client's public key arrives in message 1.
pub fn build_responder(
    prologue: &[u8],
    service_private_key: &SecretBox<StaticKey>,
) -> Result<HandshakeState, NoiseError> {
    Builder::new(parse_noise_parameters())
        .prologue(prologue)?
        .local_private_key(service_private_key.expose_secret())?
        .build_responder()
}

/// Reads message 1 on a responder: its payload, and the client's public key it carried (never
/// its private key, which Noise only uses locally). Only the holder of that key's private half
/// could have written the message, but whether the key belongs to a known client is for the
/// caller to check.
pub fn read_first_message(
    responder: &mut HandshakeState,
    first_message: &[u8],
) -> Result<(SecretBox<StaticKey>, SecretBuffer), NoiseError> {
    let payload = decrypt(responder, first_message)?;
    let client_public_key = secret_key_from_bytes(
        responder
            .get_remote_static()
            .expect("IK message 1 carries the client's static key"),
    );
    Ok((client_public_key, payload))
}

/// Handshake and transport states share the same read/write API in `snow`, but no trait.
pub trait NoiseState {
    fn write(&mut self, payload: &[u8], message: &mut [u8]) -> Result<usize, NoiseError>;
    fn read(&mut self, message: &[u8], payload: &mut [u8]) -> Result<usize, NoiseError>;
}

impl NoiseState for HandshakeState {
    fn write(&mut self, payload: &[u8], message: &mut [u8]) -> Result<usize, NoiseError> {
        self.write_message(payload, message)
    }

    fn read(&mut self, message: &[u8], payload: &mut [u8]) -> Result<usize, NoiseError> {
        self.read_message(message, payload)
    }
}

impl NoiseState for TransportState {
    fn write(&mut self, payload: &[u8], message: &mut [u8]) -> Result<usize, NoiseError> {
        self.write_message(payload, message)
    }

    fn read(&mut self, message: &[u8], payload: &mut [u8]) -> Result<usize, NoiseError> {
        self.read_message(message, payload)
    }
}

pub fn encrypt<State: NoiseState>(
    state: &mut State,
    plaintext: &[u8],
) -> Result<Vec<u8>, NoiseError> {
    let mut ciphertext = vec![0u8; MAX_NOISE_MESSAGE_LENGTH];
    let ciphertext_length = state.write(plaintext, &mut ciphertext)?;
    ciphertext.truncate(ciphertext_length);
    Ok(ciphertext)
}

pub fn decrypt<State: NoiseState>(
    state: &mut State,
    ciphertext: &[u8],
) -> Result<SecretBuffer, NoiseError> {
    let mut plaintext = secret_buffer(vec![0u8; MAX_NOISE_MESSAGE_LENGTH]);
    let plaintext_length = state.read(ciphertext, plaintext.expose_secret_mut())?;
    plaintext.expose_secret_mut().truncate(plaintext_length);
    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::test_keypair;

    const PROLOGUE: &[u8] = b"test";

    #[test]
    fn handshake_and_transport_round_trip() {
        let (service_private_key, service_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let mut initiator = build_initiator(PROLOGUE, &client_private_key, &service_public_key)
            .expect("initiator builds");
        let mut responder =
            build_responder(PROLOGUE, &service_private_key).expect("responder builds");

        let first_message = encrypt(&mut initiator, b"hello").expect("initiator writes");
        let (sender_public_key, payload) =
            read_first_message(&mut responder, &first_message).expect("responder reads");
        assert_eq!(
            client_public_key.expose_secret(),
            sender_public_key.expose_secret()
        );
        assert_eq!(payload.expose_secret().as_slice(), b"hello");
        let second_message = encrypt(&mut responder, &[]).expect("responder writes");
        decrypt(&mut initiator, &second_message).expect("initiator reads");

        let mut initiator = initiator
            .into_transport_mode()
            .expect("initiator finishes handshake");
        let mut responder = responder
            .into_transport_mode()
            .expect("responder finishes handshake");
        let ciphertext = encrypt(&mut initiator, b"data").expect("initiator encrypts");
        assert!(!ciphertext.windows(4).any(|window| window == b"data"));
        let plaintext = decrypt(&mut responder, &ciphertext).expect("responder decrypts");
        assert_eq!(plaintext.expose_secret().as_slice(), b"data");
    }

    #[test]
    fn different_prologue_fails_the_handshake() {
        let (service_private_key, service_public_key) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let mut initiator = build_initiator(PROLOGUE, &client_private_key, &service_public_key)
            .expect("initiator builds");
        let mut responder =
            build_responder(b"other", &service_private_key).expect("responder builds");
        let first_message = encrypt(&mut initiator, &[]).expect("initiator writes");
        assert!(read_first_message(&mut responder, &first_message).is_err());
    }

    #[test]
    fn message_for_another_service_fails_the_handshake() {
        let (_, service_public_key) = test_keypair();
        let (other_service_private_key, _) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let mut initiator = build_initiator(PROLOGUE, &client_private_key, &service_public_key)
            .expect("initiator builds");
        let mut responder =
            build_responder(PROLOGUE, &other_service_private_key).expect("responder builds");
        let first_message = encrypt(&mut initiator, &[]).expect("initiator writes");
        assert!(read_first_message(&mut responder, &first_message).is_err());
    }

    #[test]
    fn decode_key_validates_length() {
        let (_, key) = test_keypair();
        let encoded = SecretString::from(encode_key(key.expose_secret()));
        let decoded = decode_key(&encoded).expect("valid key decodes");
        assert_eq!(decoded.expose_secret(), key.expose_secret());
        let truncated = SecretString::from(encode_key(&key.expose_secret()[..31]));
        assert!(decode_key(&truncated).is_err());
        assert!(decode_key(&SecretString::from("not base64!")).is_err());
    }
}
