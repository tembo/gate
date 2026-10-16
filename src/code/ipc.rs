//! Messages between gate and its code workers, over the worker's stdin and stdout.
//!
//! Each message is a 4-byte big-endian length, then that many bytes of JSON. Lengths over
//! `MAX_MESSAGE_BYTES` are rejected before anything is allocated, in both directions.

use std::io::Write;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt};

/// The largest message either side sends or accepts
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// What the parent sends. `R` is a tool call's result: the parent sends rmcp's
/// `CallToolResult` as is, and the worker reads it as plain JSON.
#[derive(Debug, Serialize, Deserialize)]
pub enum ToWorker<R = Value> {
    Start { code: String, timeout_ms: u64 },
    CallResult { id: u64, outcome: Result<R, String> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FromWorker {
    Log(String),
    Call {
        id: u64,
        server_id: String,
        tool_name: String,
        arguments: Value,
    },
    Complete {
        result: Value,
        warnings: Vec<String>,
    },
    Failed {
        error: String,
    },
}

/// Serializes a message with its length prefix. Stops as soon as the message is too large,
/// rather than serializing all of it first.
pub fn encode(message: &impl Serialize) -> Result<Vec<u8>, String> {
    let mut writer = LimitedWriter(vec![0; 4]);
    if let Err(error) = serde_json::to_writer(&mut writer, message) {
        return Err(match error.io_error_kind() {
            Some(std::io::ErrorKind::FileTooLarge) => {
                format!("the message exceeds the {MAX_MESSAGE_BYTES}-byte limit")
            }
            _ => format!("can't encode message: {error}"),
        });
    }
    let mut bytes = writer.0;
    let length = u32::try_from(bytes.len() - 4).expect("limited to MAX_MESSAGE_BYTES");
    bytes[..4].copy_from_slice(&length.to_be_bytes());
    Ok(bytes)
}

/// Collects a message's JSON, after room for its length, failing once it's too large
struct LimitedWriter(Vec<u8>);

impl Write for LimitedWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.0.len() - 4 + data.len() > MAX_MESSAGE_BYTES {
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        self.0.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Reads the next message, or `None` if the stream ended cleanly between messages
pub async fn read<T: DeserializeOwned>(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<Option<T>, String> {
    let mut length = [0; 4];
    let first = reader
        .read(&mut length[..1])
        .await
        .map_err(|error| format!("can't read message: {error}"))?;
    if first == 0 {
        return Ok(None);
    }
    let mid_message = |error: std::io::Error| match error.kind() {
        std::io::ErrorKind::UnexpectedEof => {
            "the stream ended in the middle of a message".to_owned()
        }
        _ => format!("can't read message: {error}"),
    };
    reader
        .read_exact(&mut length[1..])
        .await
        .map_err(mid_message)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_MESSAGE_BYTES {
        return Err(too_large(length));
    }
    let mut json = vec![0; length];
    reader.read_exact(&mut json).await.map_err(mid_message)?;
    serde_json::from_slice(&json)
        .map(Some)
        .map_err(|error| format!("malformed message: {error}"))
}

fn too_large(length: usize) -> String {
    format!("a message of {length} bytes is over the {MAX_MESSAGE_BYTES}-byte limit")
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_bytes(bytes: &[u8]) -> Result<Option<Value>, String> {
        read(&mut &bytes[..]).await
    }

    #[tokio::test]
    async fn round_trips_messages() {
        let message = FromWorker::Log("café 🦀".into());
        let bytes = encode(&message).unwrap();
        let mut reader = &bytes[..];
        let read: Option<FromWorker> = read(&mut reader).await.unwrap();
        assert!(matches!(read, Some(FromWorker::Log(text)) if text == "café 🦀"));
        // Then a clean end
        assert!(
            super::read::<FromWorker>(&mut reader)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn rejects_bad_messages() {
        let cases: &[(&[u8], &str)] = &[
            (&[0xff, 0xff, 0xff, 0xff], "over the 4194304-byte limit"),
            (&[0, 0], "ended in the middle of a message"),
            (&[0, 0, 0, 5, b'{'], "ended in the middle of a message"),
            (&[0, 0, 0, 3, b'{', b'x', b'}'], "malformed message"),
        ];
        for &(bytes, expected) in cases {
            let error = read_bytes(bytes).await.unwrap_err();
            assert!(error.contains(expected), "{bytes:?}: {error}");
        }
    }

    #[test]
    fn refuses_to_encode_large_messages() {
        let message = FromWorker::Log("x".repeat(MAX_MESSAGE_BYTES));
        assert_eq!(
            encode(&message).unwrap_err(),
            "the message exceeds the 4194304-byte limit"
        );
        // Right at the limit is fine: the JSON is the string plus its two quotes, and
        // `{"Log":}`
        let fits = FromWorker::Log("x".repeat(MAX_MESSAGE_BYTES - 10));
        assert_eq!(encode(&fits).unwrap().len(), 4 + MAX_MESSAGE_BYTES);
    }
}
