use std::{borrow::Cow, io::Result};

use log::debug;
use regex::Regex;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    select,
};

use crate::shutdown::ShutdownToken;

const TONY: &str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";
const SERVER: &str = "chat.protohackers.com:16963";
const REGEX: &str = "7[[:alnum:]]{25,34}";

fn replace<'a>(input: &'a str, regex: &Regex) -> Cow<'a, str> {
    let mut out = Cow::Borrowed(input);
    let mut last_end = 0;

    let b_input = input.as_bytes();
    let b_len = b_input.len();

    for loc in regex.find_iter(input) {
        if last_end == 0 {
            out = Cow::Owned(String::with_capacity(input.len()));
        }

        let mut replace = true;

        if loc.start() > 0 && b_input[loc.start() - 1] != b' ' {
            replace = false;
        }
        if loc.end() < b_len && b_input[loc.end()] != b' ' && b_input[loc.end()] != b'\n' {
            replace = false;
        }

        if let Cow::Owned(o) = &mut out {
            o.push_str(&input[last_end..loc.start()]);
            if replace {
                o.push_str(TONY);
            } else {
                o.push_str(loc.as_str());
            }
        }
        last_end = loc.end();
    }

    if let Cow::Owned(o) = &mut out {
        o.push_str(&input[last_end..]);
    }

    out
}

pub async fn handle_connection(
    mut client: TcpStream,
    mut shutdown_token: ShutdownToken,
    client_id: i32,
) -> Result<()> {
    let mut server = TcpStream::connect(SERVER).await?;

    let (read_client, mut write_client) = client.split();
    let (read_server, mut write_server) = server.split();

    let mut read_client = BufReader::new(read_client);
    let mut read_server = BufReader::new(read_server);

    let client_to_server = async move {
        let bogus_regex = Regex::new(REGEX).unwrap();
        let mut line = String::with_capacity(256);
        let mut first = true;
        loop {
            line.clear();
            if read_client.read_line(&mut line).await? == 0 {
                break;
            }
            debug!("-> *    {client_id} : {}", line.trim());

            let replaced = if first {
                Cow::from(&line)
            } else {
                replace(&line, &bogus_regex)
            };
            debug!("   * -> {client_id} : {}", replaced.trim());

            write_server.write_all(replaced.as_bytes()).await?;
            first = false;
        }
        debug!("Client {client_id} EOF");
        Ok::<_, std::io::Error>(())
    };

    let server_to_client = async move {
        let bogus_regex = Regex::new(REGEX).unwrap();
        let mut line = String::with_capacity(256);
        let mut first = true;
        loop {
            line.clear();
            if read_server.read_line(&mut line).await? == 0 {
                break;
            }
            debug!("   * <- {client_id} : {}", line.trim());

            let replaced = if !first && !line.starts_with('*') {
                replace(&line, &bogus_regex)
            } else {
                Cow::from(&line)
            };

            debug!("<- *    {client_id} : {replaced}");
            write_client.write_all(replaced.as_bytes()).await?;
            first = false;
        }
        debug!("Server {client_id} EOF");
        Ok::<_, std::io::Error>(())
    };

    select! {
        _ = client_to_server => {},
        _ = server_to_client => {},
        _ = shutdown_token.wait_for_shutdown() => {},
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_noop() {
        let re = Regex::new(REGEX).unwrap();
        let example = "This is a product ID, not a Boguscoin: 7MGT7MnCSIjk6FrESkNX0JNrVfme-tBBmVMCQuqRq5kAwoU8FnMLnwAH-1234 j klj";

        assert_eq!(replace(&example, &re).to_owned(), example.to_owned());
    }

    #[test]
    fn test_replace_non7() {
        let re = Regex::new(REGEX).unwrap();
        let example = "This doesn't start with a 7: kOeQqJCClNzT0ngMt3uzEaolSOn";

        assert_eq!(replace(&example, &re).to_owned(), example.to_owned());
    }

    #[test]
    fn test_replace_too_long() {
        let re = Regex::new(REGEX).unwrap();
        let example = "This is too long: 7b73C0KQT4faKrKn0CDmCu8US4HRXcQbpWlQ";

        assert_eq!(replace(&example, &re).to_owned(), example.to_owned());
    }

    #[test]
    fn test_replace_noop_tony() {
        let re = Regex::new(REGEX).unwrap();
        let example = "What do you mean? I sent it to 7YWHMfk9JZe0LM0g1ZauHuiSxhI";

        assert_eq!(replace(&example, &re).to_owned(), example.to_owned());
    }

    #[test]
    fn test_replace_end() {
        let re = Regex::new(REGEX).unwrap();
        let example =
            "Please send the payment of 750 Boguscoins to 7r2kAfQ6IWcLJ3nIRVEKXoaiUNnje5LlxF";

        assert_eq!(
            replace(&example, &re).to_owned(),
            "Please send the payment of 750 Boguscoins to 7YWHMfk9JZe0LM0g1ZauHuiSxhI".to_owned()
        );
    }

    #[test]
    fn test_replace_end_newline() {
        let re = Regex::new(REGEX).unwrap();
        let example =
            "Please send the payment of 750 Boguscoins to 7r2kAfQ6IWcLJ3nIRVEKXoaiUNnje5LlxF\n";

        assert_eq!(
            replace(&example, &re).to_owned(),
            "Please send the payment of 750 Boguscoins to 7YWHMfk9JZe0LM0g1ZauHuiSxhI\n".to_owned()
        );
    }

    #[test]
    fn test_replace_start() {
        let re = Regex::new(REGEX).unwrap();
        let example = "7r2kAfQ6IWcLJ3nIRVEKXoaiUNnje5LlxF lkjl lkja sdf";

        assert_eq!(
            replace(&example, &re).to_owned(),
            "7YWHMfk9JZe0LM0g1ZauHuiSxhI lkjl lkja sdf".to_owned()
        );
    }

    #[test]
    fn test_replace_inline() {
        let re = Regex::new(REGEX).unwrap();
        let example = "ee 7r2kAfQ6IWcLJ3nIRVEKXoaiUNnje5LlxF lkjl lkja sdf";

        assert_eq!(
            replace(&example, &re).to_owned(),
            "ee 7YWHMfk9JZe0LM0g1ZauHuiSxhI lkjl lkja sdf".to_owned()
        );
    }

    #[test]
    fn test_replace_triple() {
        let re = Regex::new(REGEX).unwrap();
        let example = "Please pay the ticket price of 15 Boguscoins to one of these addresses: 7mvByLWxB2tC82nUTjFzhC6Th7SxNv 7nxMReTPgrDHqH5fd0BeSTy5B2tzpoP 7VGKUUeC5rZTz51UG1E2cvCEYJc0p";
        let sol = "Please pay the ticket price of 15 Boguscoins to one of these addresses: 7YWHMfk9JZe0LM0g1ZauHuiSxhI 7YWHMfk9JZe0LM0g1ZauHuiSxhI 7YWHMfk9JZe0LM0g1ZauHuiSxhI";

        assert_eq!(replace(&example, &re).to_owned(), sol.to_owned());
    }
}
