use core::str;
use std::{
    io::{self, BufReader, Read, Write},
    net::{Shutdown, TcpStream},
};

use anyhow::Context;
use log::info;

#[cfg(test)]
use {
    proptest::{collection::vec, prelude::*},
    proptest_derive::Arbitrary,
    std::io::Cursor,
};

use crate::server::TcpServerProblem;


/// Cipher operations.
/// 
/// The operation "00: End of cipher spec" is not represented here, the
/// zero byte is manually added wherever needed.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(Arbitrary))]
enum Op {
    /// 01: reversebits: Reverse the order of bits in the byte, so the least-significant bit becomes the most-significant bit, the 2nd-least-significant becomes the 2nd-most-significant, and so on.
    Reversebits,
    /// 02 N: xor(N): XOR the byte by the value N. Note that 0 is a valid value for N.
    Xor(u8),
    /// 03: xorpos: XOR the byte by its position in the stream, starting from 0.
    Xorpos,
    /// 04 N: add(N): Add N to the byte, modulo 256. Note that 0 is a valid value for N, and addition wraps, so that 255+1=0, 255+2=1, and so on.
    Add(u8),
    /// 05: addpos: Add the position in the stream to the byte, modulo 256, starting from 0. Addition wraps, so that 255+1=0, 255+2=1, and so on.
    Addpos,
}

impl Op {
    pub fn apply(&self, mut input: u8, pos: usize) -> u8 {
        match self {
            Op::Reversebits => {
                let mut out = 0;
                for _ in 0..8 {
                    out = (out << 1) | (input & 1);
                    input >>= 1;
                }
                out
            }
            Op::Xor(n) => input ^ n,
            Op::Xorpos => input ^ pos as u8,
            Op::Add(n) => input.wrapping_add(*n),
            Op::Addpos => input.wrapping_add(pos as u8),
        }
    }

    pub fn unapply(&self, input: u8, pos: usize) -> u8 {
        match self {
            Op::Add(n) => input.wrapping_sub(*n),
            Op::Addpos => input.wrapping_sub(pos as u8),
            _ => self.apply(input, pos),
        }
    }

    pub fn apply_list(list: &[Self], input: u8, pos: usize) -> u8 {
        list.iter().fold(input, |input, op| op.apply(input, pos))
    }

    pub fn unapply_list(list: &[Self], input: u8, pos: usize) -> u8 {
        list.iter()
            .rev()
            .fold(input, |input, op| op.unapply(input, pos))
    }

    pub fn compact_list(list: &[Self]) -> Vec<Self> {
        let mut output = vec![];

        for &op in list {
            match (output.last(), op) {
                (Some(Op::Add(a)), Op::Add(b)) => {
                    *output.last_mut().unwrap() = Op::Add(a.wrapping_add(b));
                }
                (Some(Op::Xor(a)), Op::Xor(b)) => {
                    *output.last_mut().unwrap() = Op::Xor(a ^ b);
                }
                (Some(Op::Reversebits), Op::Reversebits) => {
                    output.pop();
                }
                (_, op) => {
                    output.push(op);
                }
            }

            if matches!(output.last(), Some(Op::Xor(0) | Op::Add(0))) {
                output.pop();
            }
        }

        output
    }

    pub fn is_identity_list_exhaustive(list: &[Self]) -> bool {
        for input in 0..=255 {
            for pos in 0..=255 {
                let output = Op::apply_list(list, input, pos);
                if output != input {
                    return false;
                }
            }
        }

        true
    }
    pub fn is_identity_list_fast(list: &[Self]) -> bool {
        for input in 0..=255 {
            for pos in [0, 1] {
                let output = Op::apply_list(list, input, pos);
                if output != input {
                    return false;
                }
            }
        }

        true
    }

    #[cfg(test)]
    pub fn encode_list(list: &[Self]) -> Vec<u8> {
        let mut output = vec![];

        for op in list {
            match op {
                Op::Reversebits => output.push(1),
                Op::Xor(n) => {
                    output.push(2);
                    output.push(*n);
                }
                Op::Xorpos => output.push(3),
                Op::Add(n) => {
                    output.push(4);
                    output.push(*n);
                }
                Op::Addpos => output.push(5),
            };
        }

        output.push(0);
        output
    }
}

fn parse_ops(mut stream: impl io::Read) -> io::Result<Vec<Op>> {
    let mut ops = vec![];

    fn read_byte(mut stream: impl io::Read) -> io::Result<u8> {
        let mut buf = [0; 1];
        stream.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    loop {
        let op = match read_byte(&mut stream)? {
            0 => break,
            1 => Op::Reversebits,
            2 => Op::Xor(read_byte(&mut stream)?),
            3 => Op::Xorpos,
            4 => Op::Add(read_byte(&mut stream)?),
            5 => Op::Addpos,
            other => panic!("Invalid opcode {}", other),
        };

        ops.push(op);
    }
    Ok(ops)
}

pub struct ISLServer;

impl TcpServerProblem for ISLServer {
    type SharedState = ();

    fn handle_connection(
        mut stream: TcpStream,
        client_id: i32,
        _state: Self::SharedState,
    ) -> anyhow::Result<()> {
        info!("Connected to client {client_id}");
        let mut reader = BufReader::new(stream.try_clone().context("cannot clone TcpStream")?);
        let ops = Op::compact_list(&parse_ops(&mut reader).context("cannot read cipher spec")?);

        if Op::is_identity_list_fast(&ops) {
            stream.shutdown(Shutdown::Both)?;
            return Ok(());
        }

        let mut input_pos = 0;
        let mut output_pos = 0;

        let mut line_buf = Vec::with_capacity(5000);

        loop {
            let mut byte_buf = vec![0];
            if let Err(e) = reader.read_exact(&mut byte_buf) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    // client disconnected
                    return Ok(());
                } else {
                    return Err(e).context("could not read next encrypted byte");
                }
            };

            let decoded = Op::unapply_list(&ops, byte_buf[0], input_pos);
            input_pos = input_pos.wrapping_add(1);

            if decoded != b'\n' {
                line_buf.push(decoded);
            } else {
                let line = str::from_utf8(&line_buf)?;
                info!("Client {client_id} sent line {line}");

                let (_, max_order) = line
                    .split(',')
                    .map(|s: &str| -> anyhow::Result<(i32, &str)> {
                        let Some((number, _)) = s.split_once('x') else {
                            anyhow::bail!("Line {s} does not contain a number");
                        };

                        Ok((number.parse()?, s))
                    })
                    .reduce(|acc, current| {
                        let (max_count, max_order) = acc?;
                        let (current_count, current_order) = current?;

                        if max_count < current_count {
                            Ok((current_count, current_order))
                        } else {
                            Ok((max_count, max_order))
                        }
                    })
                    .context("empty line received")?.context("cannot parse line")?;

                info!("Client {client_id} max order: {max_order}");

                let mut out = vec![];
                for b in max_order.bytes().chain(std::iter::once(b'\n')) {
                    out.push(Op::apply_list(&ops, b, output_pos));
                    output_pos = output_pos.wrapping_add(1);
                }

                stream.write_all(&out)?;
                stream.flush()?;

                line_buf.clear();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use io::Write;

    use crate::server::run_tcp_server;

    use super::*;

    #[test]
    fn example_session() {
        let port = 9918;
        run_tcp_server::<ISLServer>(port, ());
        let mut conn = std::net::TcpStream::connect(("localhost", port)).unwrap();

        conn.write_all(&[0x02, 0x7b, 0x05, 0x01, 0x00]).unwrap(); // xor(123),addpos,reversebits

        conn.write_all(&[
            0xf2, 0x20, 0xba, 0x44, 0x18, 0x84, 0xba, 0xaa, 0xd0, 0x26, 0x44, 0xa4, 0xa8, 0x7e,
        ])
        .unwrap(); // 4x dog,5x car\n
        let mut buf = vec![0; 7];
        conn.read_exact(&mut buf).unwrap();
        assert_eq!(buf, &[0x72, 0x20, 0xba, 0xd8, 0x78, 0x70, 0xee]); // 5x car\n

        conn.write_all(&[
            0x6a, 0x48, 0xd6, 0x58, 0x34, 0x44, 0xd6, 0x7a, 0x98, 0x4e, 0x0c, 0xcc, 0x94, 0x31,
        ])
        .unwrap(); // 3x rat,2x cat\n
        conn.read_exact(&mut buf).unwrap();
        assert_eq!(buf, &[0xf2, 0xd0, 0x26, 0xc8, 0xa4, 0xd8, 0x7e]); // 3x rat\n
    }

    #[test]
    fn reject_identity_cipher() {
        let port = 9928;
        run_tcp_server::<ISLServer>(port, ());

        let encoded = Op::encode_list(&[Op::Xor(1), Op::Xor(1)]);

        let mut conn = std::net::TcpStream::connect(("localhost", port)).unwrap();
        conn.write_all(&encoded).unwrap();
        conn.write_all(b"0x foo, 1x bar\n").unwrap();
        assert_eq!(conn.read_to_end(&mut vec![]).unwrap(), 0);
    }

    #[test]
    fn reverse() {
        assert_eq!(Op::Reversebits.apply(0b01000111, 0), 0b11100010);
    }

    #[test]
    fn compact_list() {
        assert_eq!(Op::compact_list(&[]), vec![]);
        assert_eq!(Op::compact_list(&[Op::Add(0)]), vec![]);
        assert_eq!(Op::compact_list(&[Op::Add(1)]), vec![Op::Add(1)]);
        assert_eq!(
            Op::compact_list(&[Op::Add(1), Op::Add(2)]),
            vec![Op::Add(3)]
        );
    }

    #[test]
    fn tricky_compact_idempotent() {
        let compacted = Op::compact_list(&[Op::Xor(1), Op::Add(0), Op::Xor(1)]);
        assert_eq!(compacted, vec![]);
    }

    #[test]
    fn parse_example_cipherspec() {
        let ops = parse_ops(&mut io::Cursor::new(vec![2, 1, 1, 0])).unwrap();
        assert_eq!(ops, vec![Op::Xor(1), Op::Reversebits]);
    }
}

#[cfg(test)]
proptest! {
    #[test]
    fn unapply_inverts_apply(c: u8, op: Op, pos in 0usize..1000) {
        let out = op.unapply(op.apply(c, pos), pos);
        prop_assert_eq!(c, out);
    }

    #[test]
    fn apply_inverts_unapply(c: u8, op: Op, pos in 0usize..256) {
        let out = op.apply(op.unapply(c, pos), pos);
        prop_assert_eq!(c, out);
    }

    #[test]
    fn compacted_list_is_equivalent(list in vec(Op::arbitrary(), 0..80), input: u8, position in 0usize..256) {
        let compacted = Op::compact_list(&list);

        prop_assert!(compacted.len() <= list.len());

        let output_compacted = Op::apply_list(&list, input, position as usize);
        let output = Op::apply_list(&compacted, input, position as usize);
        prop_assert_eq!(output, output_compacted);
    }

    #[test]
    fn compacted_list_is_idempotent(list in vec(Op::arbitrary(), 0..80)) {
        let compacted = Op::compact_list(&list);
        let doubly_compacted = Op::compact_list(&compacted);
        prop_assert_eq!(doubly_compacted, compacted);
    }

    #[test]
    fn fast_identity_check_matches_exhaustive(list in vec(Op::arbitrary(), 0..80)) {
        let compacted = Op::compact_list(&list);
        let is_identity_fast = Op::is_identity_list_fast(&compacted);
        let is_identity_exhaustive = Op::is_identity_list_exhaustive(&compacted);
        prop_assert_eq!(is_identity_fast, is_identity_exhaustive);
    }

    #[test]
    fn parse_reverses_encode(list in vec(Op::arbitrary(), 0..80)) {
        let encoded = Op::encode_list(&list);
        let mut reader = Cursor::new(&encoded);
        let decoded = parse_ops(&mut reader).unwrap();
        prop_assert_eq!(list, decoded);
    }
}
