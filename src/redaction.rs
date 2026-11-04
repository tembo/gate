//! Byte-oriented streaming redaction, including secrets split across reads or lines.
use std::collections::BTreeMap;
#[derive(Clone, Default)]
pub struct Redactor {
    secrets: Vec<Vec<u8>>,
}
impl Redactor {
    pub fn new(values: &BTreeMap<String, String>) -> Self {
        let mut secrets: Vec<_> = values
            .values()
            .filter(|s| s.len() >= 8)
            .map(|s| s.as_bytes().to_vec())
            .collect();
        secrets.sort_by_key(|v| std::cmp::Reverse(v.len()));
        secrets.dedup();
        Self { secrets }
    }
    pub fn text(&self, text: &str) -> String {
        let mut buffer = text.as_bytes().to_vec();
        String::from_utf8_lossy(&self.take(&mut buffer, true)).into_owned()
    }
    pub fn take(&self, buffer: &mut Vec<u8>, eof: bool) -> Vec<u8> {
        let hold = if eof {
            0
        } else {
            self.secrets.first().map_or(0, |s| s.len() - 1)
        };
        let limit = buffer.len().saturating_sub(hold);
        let mut out = Vec::new();
        let mut cursor = 0;
        while cursor < limit {
            if let Some(secret) = self
                .secrets
                .iter()
                .find(|s| buffer[cursor..].starts_with(s))
            {
                out.extend_from_slice(b"<redacted>");
                cursor += secret.len();
            } else {
                out.push(buffer[cursor]);
                cursor += 1;
            }
        }
        buffer.drain(..cursor);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redacts_every_split_and_overlapping_prefixes() {
        let redactor = Redactor::new(&BTreeMap::from([
            ("a".into(), "abcdefgh".into()),
            ("b".into(), "abcdefghijkl".into()),
        ]));
        let text = b"before abcdefghijkl after abcdefgh!";
        for split in 0..=text.len() {
            let mut pending = text[..split].to_vec();
            let mut output = redactor.take(&mut pending, false);
            pending.extend_from_slice(&text[split..]);
            output.extend(redactor.take(&mut pending, true));
            assert_eq!(
                String::from_utf8(output).unwrap(),
                "before <redacted> after <redacted>!"
            );
        }
        assert_eq!(
            Redactor::new(&BTreeMap::from([("a".into(), "short".into())])).text("short"),
            "short"
        );
    }
}
