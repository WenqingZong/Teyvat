//! Bytes received and sent on each TCP socket of this process, from netlink `sock_diag`.
//!
//! The kernel lists every TCP socket of the network namespace with its `tcp_info`; the ones whose
//! inode is among this process's file descriptors are this process's. A socket opened and closed
//! between two readings is never seen, so its bytes are missed.

use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail, ensure};
use netlink_packet_core::{
    NLM_F_DUMP, NLM_F_REQUEST, NetlinkHeader, NetlinkMessage, NetlinkPayload,
};
use netlink_packet_sock_diag::SockDiagMessage;
use netlink_packet_sock_diag::constants::{AF_INET, AF_INET6, IPPROTO_TCP};
use netlink_packet_sock_diag::inet::nlas::Nla;
use netlink_packet_sock_diag::inet::{ExtensionFlags, InetRequest, SocketId, StateFlags};
use netlink_sys::protocols::NETLINK_SOCK_DIAG;
use netlink_sys::{Socket, SocketAddr};
/// Offset of `tcpi_bytes_acked` in `struct tcp_info` (`linux/tcp.h`, Linux 4.2 and later; the
/// kernel requirement is in `deploy/README.md`).
const BYTES_ACKED_OFFSET: usize = 120;
/// Offset of `tcpi_bytes_received`, right after `tcpi_bytes_acked`.
const BYTES_RECEIVED_OFFSET: usize = 128;
const COUNTER_LENGTH: usize = size_of::<u64>();
/// Room for a whole multipart chunk of the dump; the kernel sends at most 32 KiB at a time.
const RECEIVE_BUFFER_LENGTH: usize = 64 * 1024;
/// Port 0 addresses the kernel.
const KERNEL_PORT: u32 = 0;
const NO_MULTICAST_GROUPS: u32 = 0;

/// Cumulative counters of one socket, in bytes and `u64` as the kernel keeps them: a long-lived
/// connection passes 4 GiB within hours. Only the rates derived from them leave the process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpByteCount {
    pub received_bytes: u64,
    /// Bytes the peer acknowledged, so retransmissions are not counted twice. The kernel counts
    /// acknowledged sequence numbers, so the SYN and the FIN add one each.
    pub sent_bytes: u64,
}

/// The counters of every IPv4 and IPv6 TCP socket whose inode is in `socket_inodes`, by inode.
pub fn read_tcp_byte_counts(socket_inodes: &HashSet<u32>) -> Result<HashMap<u32, TcpByteCount>> {
    let mut socket = Socket::new(NETLINK_SOCK_DIAG)?;
    socket.bind_auto()?;
    socket.connect(&SocketAddr::new(KERNEL_PORT, NO_MULTICAST_GROUPS))?;
    let mut receive_buffer = vec![0; RECEIVE_BUFFER_LENGTH];
    let mut byte_counts = HashMap::new();
    for family in [AF_INET, AF_INET6] {
        send_dump_request(&socket, family)?;
        receive_dump(
            &socket,
            &mut receive_buffer,
            socket_inodes,
            &mut byte_counts,
        )?;
    }
    Ok(byte_counts)
}

fn send_dump_request(socket: &Socket, family: u8) -> Result<()> {
    let mut header = NetlinkHeader::default();
    header.flags = NLM_F_REQUEST | NLM_F_DUMP;
    let mut request = NetlinkMessage::new(
        header,
        SockDiagMessage::InetRequest(InetRequest {
            family,
            protocol: IPPROTO_TCP,
            extensions: ExtensionFlags::INFO,
            states: StateFlags::all(),
            socket_id: SocketId::new_v4(),
        })
        .into(),
    );
    request.finalize();
    let mut request_bytes = vec![0; request.buffer_len()];
    request.serialize(&mut request_bytes);
    socket.send(&request_bytes, 0)?;
    Ok(())
}

/// Reads the reply to one dump request up to its `Done` message.
fn receive_dump(
    socket: &Socket,
    receive_buffer: &mut [u8],
    socket_inodes: &HashSet<u32>,
    byte_counts: &mut HashMap<u32, TcpByteCount>,
) -> Result<()> {
    loop {
        let received_length = socket.recv(&mut &mut receive_buffer[..], 0)?;
        let mut offset = 0;
        while offset < received_length {
            let message = NetlinkMessage::<SockDiagMessage>::deserialize(
                &receive_buffer[offset..received_length],
            )?;
            let message_length = message.header.length as usize;
            ensure!(
                message_length > 0,
                "sock_diag sent an empty netlink message"
            );
            offset += message_length;
            match message.payload {
                NetlinkPayload::InnerMessage(SockDiagMessage::InetResponse(response)) => {
                    let inode = response.header.inode;
                    if !socket_inodes.contains(&inode) {
                        continue;
                    }
                    for attribute in &response.nlas {
                        if let Nla::TcpInfo(tcp_info) = attribute {
                            byte_counts.insert(inode, parse_tcp_info(tcp_info)?);
                        }
                    }
                }
                NetlinkPayload::Done(_) => return Ok(()),
                NetlinkPayload::Error(error) => {
                    let error = error.to_io();
                    bail!("sock_diag refused the dump: {error}");
                }
                NetlinkPayload::Overrun(_) => bail!("sock_diag dropped part of the dump"),
                // Noop, and kinds no dump of TCP sockets carries.
                _ => {}
            }
        }
    }
}

fn parse_tcp_info(tcp_info: &[u8]) -> Result<TcpByteCount> {
    match (
        read_counter(tcp_info, BYTES_RECEIVED_OFFSET),
        read_counter(tcp_info, BYTES_ACKED_OFFSET),
    ) {
        (Some(received_bytes), Some(sent_bytes)) => Ok(TcpByteCount {
            received_bytes,
            sent_bytes,
        }),
        _ => {
            let length = tcp_info.len();
            bail!("tcp_info of {length} bytes has no byte counters; the kernel predates Linux 4.2")
        }
    }
}

/// Netlink attributes are in host byte order.
fn read_counter(tcp_info: &[u8], offset: usize) -> Option<u64> {
    let counter_bytes = tcp_info.get(offset..offset + COUNTER_LENGTH)?;
    Some(u64::from_ne_bytes(counter_bytes.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use std::fs::read_link;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;

    use super::*;
    use crate::process_metrics::proc_files_parser::parse_socket_inode;
    use crate::process_metrics::read_open_file_descriptors;

    const PAYLOAD_LENGTH: usize = 5000;

    #[test]
    fn tcp_info_yields_both_counters() {
        let mut tcp_info = vec![0; BYTES_RECEIVED_OFFSET + COUNTER_LENGTH];
        tcp_info[BYTES_ACKED_OFFSET..BYTES_ACKED_OFFSET + COUNTER_LENGTH]
            .copy_from_slice(&700_u64.to_ne_bytes());
        tcp_info[BYTES_RECEIVED_OFFSET..].copy_from_slice(&900_u64.to_ne_bytes());
        assert_eq!(
            TcpByteCount {
                received_bytes: 900,
                sent_bytes: 700,
            },
            parse_tcp_info(&tcp_info).expect("tcp_info is long enough")
        );
    }

    #[test]
    fn short_tcp_info_is_refused() {
        let error = parse_tcp_info(&[0; BYTES_RECEIVED_OFFSET])
            .expect_err("tcp_info without bytes_received is refused");
        assert!(error.to_string().contains("128 bytes"), "{error}");
    }

    /// Sends bytes over a loopback connection and finds them on both ends' sockets.
    #[test]
    fn loopback_connection_counts_bytes_on_both_ends() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binding a loopback port works");
        let mut client =
            TcpStream::connect(listener.local_addr().expect("the listener has an address"))
                .expect("connecting to the listener works");
        let (mut server, _) = listener.accept().expect("the listener accepts");
        client
            .write_all(&[1; PAYLOAD_LENGTH])
            .expect("writing to the connection works");
        let mut received = vec![0; PAYLOAD_LENGTH];
        server
            .read_exact(&mut received)
            .expect("reading from the connection works");
        // The reply carries the server's ack of the whole payload, so the client counts it as sent
        // once the reply arrives, whatever the delayed ack timer would do.
        server.write_all(&[2]).expect("replying works");
        client
            .read_exact(&mut [0])
            .expect("reading the reply works");

        let socket_inodes = read_open_file_descriptors()
            .expect("listing own file descriptors works")
            .socket_inodes;
        let byte_counts = read_tcp_byte_counts(&socket_inodes).expect("sock_diag answers");
        let inode_of = |stream: &TcpStream| {
            let file_descriptor = stream.as_raw_fd();
            let link_target = read_link(format!("/proc/self/fd/{file_descriptor}"))
                .expect("the socket's file descriptor exists");
            parse_socket_inode(&link_target.to_string_lossy()).expect("the link is a socket")
        };
        let client_count = byte_counts[&inode_of(&client)];
        let server_count = byte_counts[&inode_of(&server)];
        assert_eq!(PAYLOAD_LENGTH as u64, server_count.received_bytes);
        let syn_sequence_number = 1;
        assert_eq!(
            PAYLOAD_LENGTH as u64 + syn_sequence_number,
            client_count.sent_bytes
        );
    }

    #[test]
    fn sockets_of_other_processes_are_left_out() {
        let byte_counts = read_tcp_byte_counts(&HashSet::new()).expect("sock_diag answers");
        assert!(byte_counts.is_empty());
    }
}
