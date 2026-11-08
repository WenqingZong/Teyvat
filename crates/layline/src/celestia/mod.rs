//! Celestia wire protocol, shared by Phanes (RedmiBook) and its client (Osaka).
//!
//! One WebSocket per reporting process. Binary frames 1 and 2 are the `Noise_IK` handshake:
//! frame 1 carries the client's public key and a postcard-encoded [`Hello`] naming the process,
//! which Phanes checks before it answers with frame 2, whose payload is empty. Every later frame
//! is a transport message carrying a postcard-encoded [`Batch`] (client to server) or [`Ack`]
//! (server to client). Phanes runs the server side; [`client`] is the client every reporting
//! process runs.

pub mod client;

use postcard::{Error as PostcardError, from_bytes, to_stdvec};
use secrecy::{ExposeSecret, SecretBox};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snow::{Error as NoiseError, HandshakeState, TransportState};

use crate::noise::{
    SecretBuffer, StaticKey, build_initiator, build_responder, decrypt, encrypt, read_first_message,
};
use crate::process_metrics::ProcessSample;

/// Mixed into the handshake hash, so Gnosis messages can't open a Celestia session.
pub const CELESTIA_PROLOGUE: &[u8] = b"celestia";
pub const CELESTIA_PATH: &str = "/celestia";

/// The payload of handshake frame 1: which process of the client is connecting, such as
/// `market_data_provider`. The Noise key authenticates the client, a machine, which all its
/// processes share; the name only tells them apart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub process_name: String,
}

/// `sequence` increases across reconnects and restarts, so the server can deduplicate resends.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Batch {
    pub sequence: u64,
    pub samples: Vec<ProcessSample>,
}

/// Every batch up to and including `sequence` has been received.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub sequence: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum CelestiaError {
    #[error("noise: {0}")]
    Noise(#[from] NoiseError),
    #[error("payload: {0}")]
    Payload(#[from] PostcardError),
}

/// Client side: send the first message, then finish with the server's reply.
pub struct Initiator(HandshakeState);

impl Initiator {
    pub fn start(
        client_private_key: &SecretBox<StaticKey>,
        celestia_public_key: &SecretBox<StaticKey>,
        hello: &Hello,
    ) -> Result<(Self, Vec<u8>), CelestiaError> {
        let mut state =
            build_initiator(CELESTIA_PROLOGUE, client_private_key, celestia_public_key)?;
        let first_message = encrypt(&mut state, &to_stdvec(hello)?)?;
        Ok((Self(state), first_message))
    }

    pub fn finish(mut self, second_message: &[u8]) -> Result<Session, CelestiaError> {
        decrypt(&mut self.0, second_message)?;
        Ok(Session(self.0.into_transport_mode()?))
    }
}

/// Server side: read the client's first message, check who it is, then answer.
pub struct Responder {
    state: HandshakeState,
    client_public_key: SecretBox<StaticKey>,
    /// Decoded only by `finish`, so nothing of an unidentified client's payload is used.
    hello_payload: SecretBuffer,
}

/// What `Responder::finish` yields once the caller has identified the client.
pub struct Accepted {
    pub session: Session,
    pub hello: Hello,
    /// Completes the client's handshake.
    pub second_message: Vec<u8>,
}

impl Responder {
    /// Fails unless `first_message` was written for Celestia by the holder of the private half of
    /// the public key it carries. Whether that key belongs to a known client is for the caller to
    /// check, before calling `finish`.
    pub fn read_first_message(
        celestia_private_key: &SecretBox<StaticKey>,
        first_message: &[u8],
    ) -> Result<Self, CelestiaError> {
        let mut state = build_responder(CELESTIA_PROLOGUE, celestia_private_key)?;
        let (client_public_key, hello_payload) = read_first_message(&mut state, first_message)?;
        Ok(Self {
            state,
            client_public_key,
            hello_payload,
        })
    }

    /// The public key the client signed the first message with.
    pub fn client_public_key(&self) -> &SecretBox<StaticKey> {
        &self.client_public_key
    }

    /// Decodes the client's `Hello` and answers it. Fails with `CelestiaError::Payload` on a
    /// first message whose payload is not a `Hello`.
    pub fn finish(mut self) -> Result<Accepted, CelestiaError> {
        let hello = from_bytes(self.hello_payload.expose_secret())?;
        let second_message = encrypt(&mut self.state, &[])?;
        Ok(Accepted {
            session: Session(self.state.into_transport_mode()?),
            hello,
            second_message,
        })
    }
}

/// An established channel. Frames must be decrypted in the order they were sent.
pub struct Session(TransportState);

impl Session {
    pub fn encrypt<Message: Serialize>(
        &mut self,
        message: &Message,
    ) -> Result<Vec<u8>, CelestiaError> {
        Ok(encrypt(&mut self.0, &to_stdvec(message)?)?)
    }

    pub fn decrypt<Message: DeserializeOwned>(
        &mut self,
        ciphertext: &[u8],
    ) -> Result<Message, CelestiaError> {
        let plaintext = decrypt(&mut self.0, ciphertext)?;
        Ok(from_bytes(plaintext.expose_secret())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gnosis::{GnosisId, Initiator as GnosisInitiator, Request};
    use crate::testing::{test_keypair, test_process_sample};
    use crate::time::get_current_timestamp_nanoseconds;

    const PROCESS_NAME: &str = "market_data_provider";

    fn test_hello() -> Hello {
        Hello {
            process_name: PROCESS_NAME.to_owned(),
        }
    }

    fn connected_sessions() -> (Session, Session) {
        let (celestia_private_key, celestia_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let (initiator, first_message) =
            Initiator::start(&client_private_key, &celestia_public_key, &test_hello())
                .expect("client starts");
        let responder = Responder::read_first_message(&celestia_private_key, &first_message)
            .expect("server reads the first message");
        assert_eq!(
            client_public_key.expose_secret(),
            responder.client_public_key().expose_secret()
        );
        let Accepted {
            session: server_session,
            hello,
            second_message,
        } = responder.finish().expect("server answers");
        assert_eq!(hello, test_hello());
        let client_session = initiator
            .finish(&second_message)
            .expect("client finishes handshake");
        (client_session, server_session)
    }

    #[test]
    fn batch_and_ack_round_trip() {
        let (mut client, mut server) = connected_sessions();
        let batch = Batch {
            sequence: 7,
            samples: vec![test_process_sample()],
        };
        let ciphertext = client.encrypt(&batch).expect("client encrypts batch");
        assert_eq!(
            server
                .decrypt::<Batch>(&ciphertext)
                .expect("server decrypts batch"),
            batch
        );

        let ack = Ack { sequence: 7 };
        let ciphertext = server.encrypt(&ack).expect("server encrypts ack");
        assert_eq!(
            client
                .decrypt::<Ack>(&ciphertext)
                .expect("client decrypts ack"),
            ack
        );
    }

    #[test]
    fn message_for_another_server_is_rejected() {
        let (_, celestia_public_key) = test_keypair();
        let (impostor_private_key, _) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let (_, first_message) =
            Initiator::start(&client_private_key, &celestia_public_key, &test_hello())
                .expect("client starts");
        assert!(Responder::read_first_message(&impostor_private_key, &first_message).is_err());
    }

    #[test]
    fn first_message_without_a_hello_fails_to_finish() {
        let (celestia_private_key, celestia_public_key) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let mut state =
            build_initiator(CELESTIA_PROLOGUE, &client_private_key, &celestia_public_key)
                .expect("initiator builds");
        let first_message = encrypt(&mut state, &[]).expect("empty message 1 encrypts");
        let responder = Responder::read_first_message(&celestia_private_key, &first_message)
            .expect("the handshake itself is valid");
        assert!(matches!(responder.finish(), Err(CelestiaError::Payload(_))));
    }

    #[test]
    fn replayed_frame_is_rejected() {
        let (mut client, mut server) = connected_sessions();
        let ciphertext = client
            .encrypt(&Ack { sequence: 1 })
            .expect("client encrypts");
        server
            .decrypt::<Ack>(&ciphertext)
            .expect("first delivery decrypts");
        assert!(server.decrypt::<Ack>(&ciphertext).is_err());
    }

    #[test]
    fn gnosis_handshake_does_not_open_celestia() {
        let (celestia_private_key, celestia_public_key) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let (_, gnosis_message) = GnosisInitiator::write_request(
            &client_private_key,
            &celestia_public_key,
            &Request::new(GnosisId::BinanceDemo, get_current_timestamp_nanoseconds()),
        )
        .expect("gnosis request is written");
        assert!(Responder::read_first_message(&celestia_private_key, &gnosis_message).is_err());
    }
}
