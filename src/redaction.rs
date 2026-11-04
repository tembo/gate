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
        let mut out = Vec::new();
        let mut cursor = 0;
        while cursor < buffer.len() {
            let rest = &buffer[cursor..];
            // Wait only if the remaining bytes could become a longer secret.
            // This also preserves longest-match behavior for overlapping values.
            if !eof
                && self
                    .secrets
                    .iter()
                    .any(|s| rest.len() < s.len() && s.starts_with(rest))
            {
                break;
            }
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
    fn forwards_unrelated_lines_and_holds_only_possible_secret_prefixes() {
        let redactor = Redactor::new(&BTreeMap::from([(
            "token".into(),
            "secret-token-value".into(),
        )]));
        let mut pending = b"ready\nnext line\nsec".to_vec();
        assert_eq!(redactor.take(&mut pending, false), b"ready\nnext line\n");
        assert_eq!(pending, b"sec");
        pending.extend_from_slice(b"ret-token-value\ndone\n");
        assert_eq!(redactor.take(&mut pending, false), b"<redacted>\ndone\n");
        assert!(pending.is_empty());
    }

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
