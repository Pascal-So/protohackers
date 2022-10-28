use is_prime::is_prime;
use log::debug;
use regex::Regex;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    select,
};

use crate::shutdown::ShutdownToken;

lazy_static::lazy_static! {
    static ref REGEX: Regex = Regex::new(r"^(\d+)(.0+)?$").unwrap();
}

fn get_number(val: &Value) -> Option<String> {
    let num = match val {
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }?;

    // At this point we only know that we have a valid JSON number.
    // We further want to ensure that it is a non-negative integer
    // because we can't pass floats or negative numbers to the
    // `is_prime` function. Anything that is not a non-negative
    // integer is just swapped out for "0" because that's an 
    // accepted input for `is_prime` and is also not a prime.

    if let Some(capt) = REGEX.captures(&num) {
        Some(capt[1].to_string())
    } else {
        Some("0".to_string())
    }
}

fn handle_line(line: &str, client_id: i32) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;

    let method = value.get("method").and_then(Value::as_str)?;
    if method != "isPrime" {
        return None;
    }

    let number = value.get("number").and_then(get_number)?;
    debug!("client {client_id} checking number {number}");

    Some(
        json!({
            "method": "isPrime",
            "prime": is_prime(&number),
        })
        .to_string(),
    )
}

pub async fn handle_connection(
    mut stream: TcpStream,
    mut shutdown_token: ShutdownToken,
    client_id: i32,
) -> std::io::Result<()> {
    let (read, mut write) = stream.split();
    let mut lines = BufReader::new(read).lines();

    loop {
        let line = select! {
            line = lines.next_line() => match line? {
                Some(line) => line,
                None => break
            },
            _ = shutdown_token.wait_for_shutdown() => break
        };

        debug!("--> {client_id}: {line}");
        let response = handle_line(&line, client_id).unwrap_or(String::from(r#"error"#));
        debug!("<-- {client_id}: {response}");

        write.write_all(response.as_bytes()).await?;
        write.write_all("\n".as_bytes()).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIME_TRUE: &str = r#"{"method":"isPrime","prime":true}"#;
    const PRIME_FALSE: &str = r#"{"method":"isPrime","prime":false}"#;

    #[test]
    fn test_examples() {
        let example1 = r#"{"method":"isPrime","number":123}"#.to_string();
        assert_eq!(handle_line(&example1, 0), Some(PRIME_FALSE.to_string()));

        let example2 = r#"{"method":"isPrime","number":13}"#.to_string();
        assert_eq!(handle_line(&example2, 0), Some(PRIME_TRUE.to_string()));

        let example3 = r#"{"method":"isPrime","number":13.0}"#.to_string();
        assert_eq!(handle_line(&example3, 0), Some(PRIME_TRUE.to_string()));

        let example4 = r#"{"method":"isPrime","number":13.1}"#.to_string();
        assert_eq!(handle_line(&example4, 0), Some(PRIME_FALSE.to_string()));

        let example5 = r#"{"method":"isPrime","number":asdf}"#.to_string();
        assert_eq!(handle_line(&example5, 0), None);

        let example6 = r#"{"method":"isPrime"}"#.to_string();
        assert_eq!(handle_line(&example6, 0), None);

        let example7 = r#"{"method":"asdf"}"#.to_string();
        assert_eq!(handle_line(&example7, 0), None);
    }
}
