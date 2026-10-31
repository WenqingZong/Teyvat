//! Client half of Phanes `src/noise.rs`: `Noise_KK`, both sides know each other's static X25519 public key.

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use secrecy::{ExposeSecret, ExposeSecretMut, SecretBox, SecretString};
use snow::params::NoiseParams;
use snow::{Builder, Error as NoiseError, HandshakeState};

pub(crate) const NOISE_PARAMETERS: &str = "Noise_KK_25519_ChaChaPoly_BLAKE2s";
pub(crate) const KEY_LENGTH: usize = 32;
pub(crate) const MAX_NOISE_MESSAGE_LENGTH: usize = 65535;

pub(crate) type StaticKey = [u8; KEY_LENGTH];
/// Plaintext may carry API keys, so it is zeroed on drop.
pub(crate) type SecretBuffer = SecretBox<Vec<u8>>;

#[derive(Debug, thiserror::Error)]
#[error("invalid key encoding")]
pub(crate) struct InvalidKeyError;

fn parse_noise_parameters() -> NoiseParams {
    NOISE_PARAMETERS
        .parse()
        .expect("NOISE_PARAMETERS is a valid Noise pattern")
}

pub(crate) fn decode_public_key(encoded: &str) -> Result<StaticKey, InvalidKeyError> {
    BASE64
        .decode(encoded.trim())
        .map_err(|_| InvalidKeyError)?
        .try_into()
        .map_err(|_| InvalidKeyError)
}

pub(crate) fn decode_private_key(
    encoded: &SecretString,
) -> Result<SecretBox<StaticKey>, InvalidKeyError> {
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
pub(crate) fn build_handshake(
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

pub(crate) fn encrypt(state: &mut HandshakeState, plaintext: &[u8]) -> Result<Vec<u8>, NoiseError> {
    let mut ciphertext = vec![0u8; MAX_NOISE_MESSAGE_LENGTH];
    let ciphertext_length = state.write_message(plaintext, &mut ciphertext)?;
    ciphertext.truncate(ciphertext_length);
    Ok(ciphertext)
}

pub(crate) fn decrypt(
    state: &mut HandshakeState,
    ciphertext: &[u8],
) -> Result<SecretBuffer, NoiseError> {
    let mut plaintext = secret_buffer(vec![0u8; MAX_NOISE_MESSAGE_LENGTH]);
    let plaintext_length = state.read_message(ciphertext, plaintext.expose_secret_mut())?;
    plaintext.expose_secret_mut().truncate(plaintext_length);
    Ok(plaintext)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn test_keypair() -> (SecretBox<StaticKey>, StaticKey) {
        let keypair = Builder::new(parse_noise_parameters())
            .generate_keypair()
            .expect("keypair generation succeeds");
        (
            SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(&keypair.private)),
            keypair.public.try_into().expect("public key is 32 bytes"),
        )
    }

    pub(crate) fn encode_key(key: &[u8]) -> String {
        BASE64.encode(key)
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
        assert!(decode_private_key(&SecretString::from("not base64!")).is_err());
    }
}
