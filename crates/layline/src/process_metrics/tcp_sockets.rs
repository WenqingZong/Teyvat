//! Bytes received and sent on each TCP socket of this process, from netlink `sock_diag`.
//!
//! The kernel lists every TCP socket of the network namespace with its `tcp_info`; the ones whose
//! inode is among this process's file descriptors are this process's. A socket opened and closed
//! between two readings is never seen, so its bytes are missed.
//!
//! Netlink is the only source of these counters, so the reading half of this module is Linux-only;
//! [`TcpByteCount`] exists on every platform because [`super::ProcessSample`] carries it.

/// Cumulative counters of one socket, in bytes and `u64` as the kernel keeps them: a long-lived
/// connection passes 4 GiB within hours. Only the rates derived from them leave the process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpByteCount {
    pub received_bytes: u64,
    /// Bytes the peer acknowledged, so retransmissions are not counted twice. The kernel counts
    /// acknowledged sequence numbers, so the SYN and the FIN add one each.
    pub sent_bytes: u64,
}

#[cfg(target_os = "linux")]
pub(super) mod linux {
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

    use super::TcpByteCount;

    /// Room for a whole multipart chunk of the dump; the kernel sends at most 32 KiB at a time.
    const RECEIVE_BUFFER_LENGTH: usize = 64 * 1024;
    /// Port 0 addresses the kernel.
    const KERNEL_PORT: u32 = 0;
    const NO_MULTICAST_GROUPS: u32 = 0;

    /// The counters of every IPv4 and IPv6 TCP socket whose inode is in `socket_inodes`, by inode.
    pub(crate) fn read_tcp_byte_counts(
        socket_inodes: &HashSet<u32>,
    ) -> Result<HashMap<u32, TcpByteCount>> {
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
                                byte_counts.insert(
                                    inode,
                                    TcpByteCount {
                                        received_bytes: tcp_info.bytes_received,
                                        sent_bytes: tcp_info.bytes_acked,
                                    },
                                );
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
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::collections::HashSet;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;

    use super::linux::read_tcp_byte_counts;
    use super::*;

    const PAYLOAD_LENGTH: usize = 5000;

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

        let socket_inodes = HashSet::from([socket_inode(&client), socket_inode(&server)]);
        let byte_counts = read_tcp_byte_counts(&socket_inodes).expect("sock_diag answers");
        let client_count = byte_counts[&socket_inode(&client)];
        let server_count = byte_counts[&socket_inode(&server)];
        assert_eq!(PAYLOAD_LENGTH as u64, server_count.received_bytes);
        let syn_sequence_number = 1;
        assert_eq!(
            PAYLOAD_LENGTH as u64 + syn_sequence_number,
            client_count.sent_bytes
        );
    }

    /// The socket inode of `stream`, through `procfs`, so the test does not parse `/proc` itself.
    fn socket_inode(stream: &TcpStream) -> u32 {
        let process = procfs::process::Process::myself().expect("the process is readable");
        let file_descriptor = process
            .fd_from_fd(stream.as_raw_fd())
            .expect("the socket's file descriptor exists");
        match file_descriptor.target {
            procfs::process::FDTarget::Socket(inode) => {
                u32::try_from(inode).expect("the inode fits u32")
            }
            target => panic!("the descriptor is not a socket: {target:?}"),
        }
    }

    #[test]
    fn sockets_of_other_processes_are_left_out() {
        let byte_counts = read_tcp_byte_counts(&HashSet::new()).expect("sock_diag answers");
        assert!(byte_counts.is_empty());
    }
}
