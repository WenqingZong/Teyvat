//! End-to-end encryption shared by every Phanes service. Cloudflare terminates TLS, so each
//! service encrypts at the application layer with `Noise_KK`: both sides know each other's
//! static X25519 public key in advance, and each service uses its own key pair and prologue.

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use secrecy::{ExposeSecret, ExposeSecretMut, SecretBox, SecretString};
use snow::params::NoiseParams;
use snow::{Builder, Error as NoiseError, HandshakeState, Keypair, TransportState};

pub const NOISE_PARAMETERS: &str = "Noise_KK_25519_ChaChaPoly_BLAKE2s";
pub const KEY_LENGTH: usize = 32;
pub const MAX_NOISE_MESSAGE_LENGTH: usize = 65535;

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

pub fn decode_public_key(encoded: &str) -> Result<StaticKey, InvalidKeyError> {
    BASE64
        .decode(encoded.trim())
        .map_err(|_| InvalidKeyError)?
        .try_into()
        .map_err(|_| InvalidKeyError)
}

pub fn decode_private_key(encoded: &SecretString) -> Result<SecretBox<StaticKey>, InvalidKeyError> {
    let bytes = secret_buffer(
        BASE64
            .decode(encoded.expose_secret().trim())
            .map_err(|_| InvalidKeyError)?,
    );
    if bytes.expose_secret().len() != KEY_LENGTH {
        return Err(InvalidKeyError);
    }
    Ok(SecretBox::init_with_mut(|key: &mut StaticKey| {
        key.copy_from_slice(bytes.expose_secret())
    }))
}

fn secret_buffer(bytes: Vec<u8>) -> SecretBuffer {
    SecretBox::new(Box::new(bytes))
}

/// Every service passes its own prologue, so a handshake for one service fails on another.
pub fn build_handshake(
    prologue: &[u8],
    local_private_key: &SecretBox<StaticKey>,
    remote_public_key: &StaticKey,
    is_initiator: bool,
) -> Result<HandshakeState, NoiseError> {
    let builder = Builder::new(parse_noise_parameters())
        .prologue(prologue)?
        .local_private_key(local_private_key.expose_secret())?
        .remote_public_key(remote_public_key)?;
    if is_initiator {
        builder.build_initiator()
    } else {
        builder.build_responder()
    }
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
pub(crate) mod tests {
    use super::*;

    const PROLOGUE: &[u8] = b"test";

    pub(crate) fn test_keypair() -> (SecretBox<StaticKey>, StaticKey) {
        let keypair = generate_keypair().expect("keypair generation succeeds");
        (
            SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(&keypair.private)),
            keypair.public.try_into().expect("public key is 32 bytes"),
        )
    }

    #[test]
    fn handshake_and_transport_round_trip() {
        let (responder_private_key, responder_public_key) = test_keypair();
        let (initiator_private_key, initiator_public_key) = test_keypair();
        let mut initiator = build_handshake(
            PROLOGUE,
            &initiator_private_key,
            &responder_public_key,
            true,
        )
        .expect("initiator builds");
        let mut responder = build_handshake(
            PROLOGUE,
            &responder_private_key,
            &initiator_public_key,
            false,
        )
        .expect("responder builds");

        let first_message = encrypt(&mut initiator, b"hello").expect("initiator writes");
        let payload = decrypt(&mut responder, &first_message).expect("responder reads");
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
        let (responder_private_key, responder_public_key) = test_keypair();
        let (initiator_private_key, initiator_public_key) = test_keypair();
        let mut initiator = build_handshake(
            PROLOGUE,
            &initiator_private_key,
            &responder_public_key,
            true,
        )
        .expect("initiator builds");
        let mut responder = build_handshake(
            b"other",
            &responder_private_key,
            &initiator_public_key,
            false,
        )
        .expect("responder builds");
        let first_message = encrypt(&mut initiator, &[]).expect("initiator writes");
        assert!(decrypt(&mut responder, &first_message).is_err());
    }

    #[test]
    fn decode_public_key_validates_length() {
        let (_, public_key) = test_keypair();
        let decoded = decode_public_key(&encode_key(&public_key)).expect("valid key decodes");
        assert_eq!(decoded, public_key);
        assert!(decode_public_key(&encode_key(&public_key[..31])).is_err());
        assert!(decode_public_key("not base64!").is_err());
    }

    #[test]
    fn decode_private_key_validates_length() {
        let (private_key, _) = test_keypair();
        let encoded = SecretString::from(encode_key(private_key.expose_secret()));
        let decoded = decode_private_key(&encoded).expect("valid key decodes");
        assert_eq!(decoded.expose_secret(), private_key.expose_secret());
        let truncated = SecretString::from(encode_key(&private_key.expose_secret()[..31]));
        assert!(decode_private_key(&truncated).is_err());
    }
}
