//! Celestia wire protocol, shared by Phanes (RedmiBook) and its client (Osaka).
//!
//! One WebSocket per connection. Binary frames 1 and 2 are the `Noise_KK` handshake with
//! empty payloads; every later frame is a transport message carrying a postcard-encoded
//! [`Batch`] (client to server) or [`Ack`] (server to client).

use std::fmt::{Display, Formatter, Result as FormatResult};
use std::time::Duration;

use postcard::{Error as PostcardError, from_bytes, to_stdvec};
use secrecy::{ExposeSecret, SecretBox};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snow::{Error as NoiseError, HandshakeState, TransportState};

use layline::noise::{StaticKey, build_handshake, decrypt, encrypt};

/// Mixed into the handshake hash, so Gnosis messages can't open a Celestia session.
pub const CELESTIA_PROLOGUE: &[u8] = b"celestia";
pub const CELESTIA_PATH: &str = "/celestia";

const BYTES_PER_MEBIBYTE: f64 = 1024.0 * 1024.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessSample {
    /// Since the Unix epoch.
    pub timestamp: Duration,
    /// 100.0 means one full core.
    pub cpu_percent: f64,
    pub resident_memory_bytes: u64,
    pub network: NetworkRate,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NetworkRate {
    pub received_bytes_per_second: f64,
    pub sent_bytes_per_second: f64,
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

impl Display for ProcessSample {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let milliseconds = self.timestamp.as_millis();
        let cpu_percent = self.cpu_percent;
        let memory_mebibytes = self.resident_memory_bytes as f64 / BYTES_PER_MEBIBYTE;
        let received = self.network.received_bytes_per_second;
        let sent = self.network.sent_bytes_per_second;
        write!(
            formatter,
            "time {milliseconds} ms, cpu {cpu_percent:.1}%, memory {memory_mebibytes:.1} MiB, received {received:.0} B/s, sent {sent:.0} B/s"
        )
    }
}

/// Client side: send the first message, then finish with the server's reply.
pub struct Initiator(HandshakeState);

impl Initiator {
    pub fn start(
        client_private_key: &SecretBox<StaticKey>,
        celestia_public_key: &StaticKey,
    ) -> Result<(Self, Vec<u8>), CelestiaError> {
        let mut state = build_handshake(
            CELESTIA_PROLOGUE,
            client_private_key,
            celestia_public_key,
            true,
        )?;
        let first_message = encrypt(&mut state, &[])?;
        Ok((Self(state), first_message))
    }

    pub fn finish(mut self, second_message: &[u8]) -> Result<Session, CelestiaError> {
        decrypt(&mut self.0, second_message)?;
        Ok(Session(self.0.into_transport_mode()?))
    }
}

/// Server side. Fails unless `first_message` comes from the holder of `client_public_key`.
pub fn respond(
    celestia_private_key: &SecretBox<StaticKey>,
    client_public_key: &StaticKey,
    first_message: &[u8],
) -> Result<(Session, Vec<u8>), CelestiaError> {
    let mut state = build_handshake(
        CELESTIA_PROLOGUE,
        celestia_private_key,
        client_public_key,
        false,
    )?;
    decrypt(&mut state, first_message)?;
    let second_message = encrypt(&mut state, &[])?;
    Ok((Session(state.into_transport_mode()?), second_message))
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
pub(crate) mod tests {
    use super::*;
    use layline::gnosis::{Initiator as GnosisInitiator, Request};
    use layline::testing::test_keypair;
    use layline::time::get_current_timestamp_nanoseconds;

    pub(crate) fn sample() -> ProcessSample {
        ProcessSample {
            timestamp: Duration::from_millis(1_700_000_000_000),
            cpu_percent: 12.5,
            resident_memory_bytes: 64 * 1024 * 1024,
            network: NetworkRate {
                received_bytes_per_second: 1024.0,
                sent_bytes_per_second: 512.0,
            },
        }
    }

    fn connected_sessions() -> (Session, Session) {
        let (celestia_private_key, celestia_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let (initiator, first_message) =
            Initiator::start(&client_private_key, &celestia_public_key).expect("client starts");
        let (server_session, second_message) =
            respond(&celestia_private_key, &client_public_key, &first_message)
                .expect("server authenticates client");
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
            samples: vec![sample()],
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
    fn unknown_client_is_rejected() {
        let (celestia_private_key, celestia_public_key) = test_keypair();
        let (_, client_public_key) = test_keypair();
        let (impostor_private_key, _) = test_keypair();
        let (_, first_message) =
            Initiator::start(&impostor_private_key, &celestia_public_key).expect("impostor starts");
        assert!(respond(&celestia_private_key, &client_public_key, &first_message).is_err());
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
        let (client_private_key, client_public_key) = test_keypair();
        let (_, gnosis_message) = GnosisInitiator::write_request(
            &client_private_key,
            &celestia_public_key,
            &Request::new("key", get_current_timestamp_nanoseconds()),
        )
        .expect("gnosis request is written");
        assert!(respond(&celestia_private_key, &client_public_key, &gnosis_message).is_err());
    }

    #[test]
    fn displays_sample() {
        assert!(
            sample()
                .to_string()
                .ends_with("cpu 12.5%, memory 64.0 MiB, received 1024 B/s, sent 512 B/s")
        );
    }
}
