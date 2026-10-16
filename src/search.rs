use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    sync::Arc,
};

use rmcp::model::{JsonObject, Tool};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::ServerId;

/// BM25's term frequency saturation and length normalization, as the original gateway used
const K: f64 = 1.2;
const B: f64 = 0.75;
/// How much more a tool scores when the query names its server, and matched something else
const SERVER_PRIOR: f64 = 1.25;
/// The most text taken from a tool's input schema
const SCHEMA_TEXT_BYTES: usize = 16_384;

/// The fields a tool is searched by, and how much each counts: names and titles count three
/// times as much as descriptions and parameters
const FIELD_BOOSTS: [f64; 6] = [
    3.0, // server ID
    3.0, // tool name
    3.0, // tool title
    1.0, // server description
    1.0, // tool description
    1.0, // input schema text
];

/// A server, as captured once for a batch of searches
#[derive(Clone)]
pub struct CatalogServer {
    pub id: ServerId,
    pub description: Option<String>,
    pub state: ServerState,
}

#[derive(Clone)]
pub enum ServerState {
    Ready(Arc<[Tool]>),
    Connecting,
    Failed(Arc<str>),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResults<'a> {
    pub matches: Vec<Match<'a>>,
    pub excluded_servers: Vec<Excluded<'a>>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Match<'a> {
    pub server_id: &'a str,
    pub tool_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'a str>,
}

/// A selected server that isn't ready, so none of its tools could match
#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Excluded<'a> {
    pub server_id: &'a str,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<&'a str>,
}

/// An index of every ready server's tools. It's immutable, so every search in a batch sees
/// the same servers and tools.
pub struct Index {
    servers: Vec<CatalogServer>,
    /// In catalog order: by server, then in each server's own order
    documents: Vec<Document>,
    fields: [FieldIndex; FIELD_BOOSTS.len()],
}

struct Document {
    server: usize,
    tool: usize,
    /// The tool's name and `server/tool`, normalized for exact matching
    identities: [String; 2],
}

#[derive(Default)]
struct FieldIndex {
    /// The documents each term appears in, and how many times
    postings: HashMap<String, Vec<(usize, u32)>>,
    /// Each document's number of unique terms, which is how MiniSearch measures length
    lengths: Vec<u32>,
    average_length: f64,
}

impl FieldIndex {
    fn add(&mut self, document: usize, text: &str) {
        let mut counts: HashMap<String, u32> = HashMap::new();
        for term in tokenize(text) {
            *counts.entry(term).or_default() += 1;
        }
        self.lengths.push(counts.len() as u32);
        for (term, count) in counts {
            self.postings
                .entry(term)
                .or_default()
                .push((document, count));
        }
    }
}

/// The index of the current catalog, kept until the catalog changes
#[derive(Default)]
pub struct IndexCache {
    index: Mutex<Option<Arc<Index>>>,
}

impl IndexCache {
    /// The index of `catalog()`'s servers, reusing the last one if nothing has changed. The
    /// catalog is captured while holding the lock, so searches that arrive during a rebuild
    /// wait for it and reuse it, rather than each building their own.
    pub async fn get(
        &self,
        catalog: impl FnOnce() -> Vec<CatalogServer>,
    ) -> Result<Arc<Index>, String> {
        let mut cached = self.index.lock().await;
        let catalog = catalog();
        if let Some(index) = &*cached
            && index.is_for(&catalog)
        {
            return Ok(index.clone());
        }
        // Big catalogs take a while to index, so it happens off the async threads
        let index = tokio::task::spawn_blocking(move || Index::new(catalog))
            .await
            .map_err(|error| format!("can't build the search index: {error}"))?;
        let index = Arc::new(index);
        *cached = Some(index.clone());
        Ok(index)
    }
}

impl Index {
    /// Whether this index was built from the same catalog: the same servers, in the same
    /// states, with the same tool lists. Lists are compared by identity, which is cheap and
    /// safe: every connection gets a new list, and this index keeps its own alive, so a new
    /// list can't reuse its address.
    pub fn is_for(&self, catalog: &[CatalogServer]) -> bool {
        self.servers.len() == catalog.len()
            && self.servers.iter().zip(catalog).all(|(indexed, current)| {
                indexed.id == current.id
                    && match (&indexed.state, &current.state) {
                        (ServerState::Ready(indexed), ServerState::Ready(current)) => {
                            Arc::ptr_eq(indexed, current)
                        }
                        (ServerState::Connecting, ServerState::Connecting) => true,
                        (ServerState::Failed(indexed), ServerState::Failed(current)) => {
                            Arc::ptr_eq(indexed, current)
                        }
                        _ => false,
                    }
            })
    }

    pub fn new(servers: Vec<CatalogServer>) -> Self {
        let mut documents = Vec::new();
        let mut fields: [FieldIndex; FIELD_BOOSTS.len()] = Default::default();
        for (server_index, server) in servers.iter().enumerate() {
            let ServerState::Ready(tools) = &server.state else {
                continue;
            };
            for (tool_index, tool) in tools.iter().enumerate() {
                let texts = [
                    server.id.as_str(),
                    &tool.name,
                    tool.title.as_deref().unwrap_or_default(),
                    server.description.as_deref().unwrap_or_default(),
                    tool.description.as_deref().unwrap_or_default(),
                    &schema_text(&tool.input_schema),
                ];
                for (field, text) in fields.iter_mut().zip(texts) {
                    field.add(documents.len(), text);
                }
                documents.push(Document {
                    server: server_index,
                    tool: tool_index,
                    identities: [
                        normalize(&tool.name),
                        normalize(&format!("{}/{}", server.id, tool.name)),
                    ],
                });
            }
        }
        for field in &mut fields {
            let total: u32 = field.lengths.iter().sum();
            field.average_length = f64::from(total) / documents.len().max(1) as f64;
        }
        Self {
            servers,
            documents,
            fields,
        }
    }

    /// Searches the selected servers, or all of them if `server_ids` is `None`. An empty
    /// query lists their tools in catalog order.
    pub fn search(
        &self,
        query: &str,
        server_ids: Option<&[String]>,
        max_results: usize,
    ) -> Result<SearchResults<'_>, String> {
        let selected = self.select(server_ids)?;
        let mut is_selected = vec![false; self.servers.len()];
        for &server in &selected {
            is_selected[server] = true;
        }

        let query = query.trim();
        let ranked: Vec<usize> = if query.is_empty() {
            (0..self.documents.len())
                .filter(|&document| is_selected[self.documents[document].server])
                .collect()
        } else {
            self.rank(query, &is_selected)
                .into_iter()
                .map(|(document, _)| document)
                .collect()
        };

        let matches = ranked
            .into_iter()
            .take(max_results)
            .map(|document| self.to_match(document))
            .collect();
        let excluded_servers = selected
            .into_iter()
            .filter_map(|server| {
                let server = &self.servers[server];
                let (status, error) = match &server.state {
                    ServerState::Ready(_) => return None,
                    ServerState::Connecting => ("connecting", None),
                    ServerState::Failed(error) => ("failed", Some(error.as_ref())),
                };
                Some(Excluded {
                    server_id: &server.id,
                    status,
                    error,
                })
            })
            .collect();
        Ok(SearchResults {
            matches,
            excluded_servers,
        })
    }

    /// The selected servers' indices, in the order given and without duplicates
    fn select(&self, server_ids: Option<&[String]>) -> Result<Vec<usize>, String> {
        let Some(server_ids) = server_ids else {
            return Ok((0..self.servers.len()).collect());
        };
        let mut selected = Vec::new();
        for id in server_ids {
            let server = self
                .servers
                .iter()
                .position(|server| &server.id == id)
                .ok_or_else(|| format!("unknown server `{id}`"))?;
            if !selected.contains(&server) {
                selected.push(server);
            }
        }
        Ok(selected)
    }

    /// Scores the selected servers' tools against the query, best first, with exact name
    /// matches ahead of everything else
    fn rank(&self, query: &str, is_selected: &[bool]) -> Vec<(usize, f64)> {
        let mut terms = tokenize(query);
        let mut seen = HashSet::new();
        terms.retain(|term| seen.insert(term.clone()));

        #[derive(Default)]
        struct Hit {
            score: f64,
            matched_terms: HashSet<usize>,
        }
        let mut hits: HashMap<usize, Hit> = HashMap::new();
        let document_count = self.documents.len() as f64;
        for (term_index, term) in terms.iter().enumerate() {
            for (field, boost) in self.fields.iter().zip(FIELD_BOOSTS) {
                let Some(postings) = field.postings.get(term) else {
                    continue;
                };
                let matching = postings.len() as f64;
                let idf = (1.0 + (document_count - matching + 0.5) / (matching + 0.5)).ln();
                for &(document, frequency) in postings {
                    if !is_selected[self.documents[document].server] {
                        continue;
                    }
                    let frequency = f64::from(frequency);
                    let length = f64::from(field.lengths[document]) / field.average_length;
                    let saturation =
                        frequency * (K + 1.0) / (frequency + K * (1.0 - B + B * length));
                    let hit = hits.entry(document).or_default();
                    hit.score += boost * idf * saturation;
                    hit.matched_terms.insert(term_index);
                }
            }
        }

        // Exact names are found even when no term matched
        let identity = normalize(query);
        if !identity.is_empty() {
            for (index, document) in self.documents.iter().enumerate() {
                if is_selected[document.server] && document.identities.contains(&identity) {
                    hits.entry(index).or_default();
                }
            }
        }

        let mut ranked: Vec<(usize, bool, f64)> = hits
            .into_iter()
            .map(|(index, hit)| {
                let document = &self.documents[index];
                // Tools matching more of the query's terms rank higher
                let mut score = hit.score * hit.matched_terms.len().max(1) as f64;
                let server_terms = tokenize(&self.servers[document.server].id);
                let names_server = !server_terms.is_empty()
                    && server_terms.iter().all(|term| terms.contains(term));
                let matches_other_term = hit
                    .matched_terms
                    .iter()
                    .any(|&term| !server_terms.contains(&terms[term]));
                if names_server && matches_other_term {
                    score *= SERVER_PRIOR;
                }
                let exact = document.identities.contains(&identity);
                (index, exact, score)
            })
            .collect();
        // Ties keep catalog order, so results are stable
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.total_cmp(&a.2)).then(a.0.cmp(&b.0)));
        ranked
            .into_iter()
            .map(|(document, _, score)| (document, score))
            .collect()
    }

    fn to_match(&self, document: usize) -> Match<'_> {
        let document = &self.documents[document];
        let server = &self.servers[document.server];
        let ServerState::Ready(tools) = &server.state else {
            unreachable!("only ready servers' tools are indexed")
        };
        let tool = &tools[document.tool];
        Match {
            server_id: &server.id,
            tool_name: &tool.name,
            title: tool.title.as_deref(),
            description: tool.description.as_deref(),
        }
    }
}

/// Splits text into lowercase terms, at anything that isn't a letter or digit, and at
/// camelCase and acronym boundaries: `getHTTPServer` gives `get`, `http` and `server`
fn tokenize(text: &str) -> Vec<String> {
    let mut terms = Vec::new();
    for word in text.split(|c: char| !c.is_alphanumeric()) {
        let chars: Vec<char> = word.chars().collect();
        let mut start = 0;
        for i in 1..chars.len() {
            let (previous, current) = (chars[i - 1], chars[i]);
            let next_is_lowercase = chars.get(i + 1).is_some_and(|c| c.is_lowercase());
            let boundary = current.is_uppercase()
                && (previous.is_lowercase()
                    || previous.is_numeric()
                    || (previous.is_uppercase() && next_is_lowercase));
            if boundary {
                terms.push(
                    chars[start..i]
                        .iter()
                        .flat_map(|c| c.to_lowercase())
                        .collect(),
                );
                start = i;
            }
        }
        if start < chars.len() {
            terms.push(
                chars[start..]
                    .iter()
                    .flat_map(|c| c.to_lowercase())
                    .collect(),
            );
        }
    }
    terms
}

/// Lowercases a name and drops everything but letters and digits, so `get_issue`, `getIssue`
/// and `getissue` are the same
fn normalize(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// The searchable text of a tool's input schema: its descriptions and parameter names, at any
/// depth, like the original gateway
fn schema_text(schema: &JsonObject) -> String {
    let mut text = String::new();
    let _ = collect_schema_text(schema, &mut text);
    text
}

fn collect_schema_text(object: &JsonObject, text: &mut String) -> ControlFlow<()> {
    for (key, value) in object {
        match (key.as_str(), value) {
            ("description", Value::String(description)) => push_capped(text, description)?,
            ("properties", Value::Object(properties)) => {
                for name in properties.keys() {
                    push_capped(text, name)?;
                }
            }
            _ => {}
        }
        collect_schema_values(value, text)?;
    }
    ControlFlow::Continue(())
}

fn collect_schema_values(value: &Value, text: &mut String) -> ControlFlow<()> {
    match value {
        Value::Object(object) => collect_schema_text(object, text),
        Value::Array(values) => values
            .iter()
            .try_for_each(|value| collect_schema_values(value, text)),
        _ => ControlFlow::Continue(()),
    }
}

/// Appends a word to the schema text, stopping once it's full
fn push_capped(text: &mut String, word: &str) -> ControlFlow<()> {
    if !text.is_empty() {
        text.push(' ');
    }
    let room = SCHEMA_TEXT_BYTES.saturating_sub(text.len());
    if word.len() <= room {
        text.push_str(word);
        ControlFlow::Continue(())
    } else {
        text.push_str(&word[..word.floor_char_boundary(room)]);
        ControlFlow::Break(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/search_catalog.json");

    /// The fixture's servers, all ready
    fn catalog() -> Vec<CatalogServer> {
        let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
        fixture["servers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|server| CatalogServer {
                id: server["id"].as_str().unwrap().into(),
                description: server["description"].as_str().map(Into::into),
                state: ServerState::Ready(
                    serde_json::from_value::<Vec<Tool>>(server["tools"].clone())
                        .unwrap()
                        .into(),
                ),
            })
            .collect()
    }

    /// `server/tool` for each match
    fn search(index: &Index, query: &str, server_ids: Option<&[&str]>, max: usize) -> Vec<String> {
        let server_ids: Option<Vec<String>> =
            server_ids.map(|ids| ids.iter().map(|&id| id.into()).collect());
        index
            .search(query, server_ids.as_deref(), max)
            .unwrap()
            .matches
            .iter()
            .map(|found| format!("{}/{}", found.server_id, found.tool_name))
            .collect()
    }

    /// Every fixture query ranks tools in the same order as the original gateway did, using
    /// MiniSearch 7.2.0 with its settings, except where noted. Scores differ slightly, since
    /// MiniSearch has quirks we don't copy, like counting an empty field as one term long.
    #[test]
    fn ranks_like_the_original_gateway() {
        let index = Index::new(catalog());
        // A query, the servers it searches, the most matches, and the expected matches
        type Case<'a> = (&'a str, Option<&'a [&'a str]>, usize, &'a [&'a str]);
        let cases: &[Case] = &[
            (
                "get_issue",
                None,
                20,
                &[
                    "github/get_issue",
                    "linear/get_issue",
                    "github/create_issue",
                    "linear/create_issue",
                    "linear/update_issue",
                ],
            ),
            (
                "getIssue",
                None,
                20,
                &[
                    "github/get_issue",
                    "linear/get_issue",
                    "github/create_issue",
                    "linear/create_issue",
                    "linear/update_issue",
                ],
            ),
            // The original found nothing, since `getissue` is a single term
            (
                "getissue",
                None,
                20,
                &["github/get_issue", "linear/get_issue"],
            ),
            (
                "github/get_issue",
                None,
                20,
                &[
                    "github/get_issue",
                    "github/create_issue",
                    "linear/get_issue",
                    "linear/create_issue",
                    "linear/update_issue",
                    "github/search",
                    "github/list_projects",
                    "github/create_pull_request",
                ],
            ),
            // Naming a server favors its tools
            (
                "linear issue",
                None,
                20,
                &[
                    "linear/get_issue",
                    "linear/create_issue",
                    "linear/update_issue",
                    "github/get_issue",
                    "github/create_issue",
                    "linear/search",
                    "linear/list_projects",
                ],
            ),
            // Matching both terms beats matching only the common one
            (
                "create issue",
                None,
                20,
                &[
                    "github/create_issue",
                    "linear/create_issue",
                    "github/get_issue",
                    "github/create_pull_request",
                    "linear/get_issue",
                    "linear/update_issue",
                ],
            ),
            // Only in `move_file`'s input schema
            ("destination", None, 20, &["filesystem/move_file"]),
            (
                "search",
                None,
                20,
                &["github/search", "linear/search", "filesystem/search_files"],
            ),
            // Filters before truncating: GitHub's `search` scores higher, but isn't selected
            ("search", Some(&["linear"]), 1, &["linear/search"]),
            (
                "pull request",
                None,
                20,
                &[
                    "github/create_pull_request",
                    "github/search",
                    "github/get_issue",
                    "github/create_issue",
                    "github/list_projects",
                ],
            ),
            (
                "rename file",
                None,
                20,
                &["filesystem/move_file", "filesystem/read_file"],
            ),
            (
                "projects",
                None,
                20,
                &[
                    "linear/list_projects",
                    "github/list_projects",
                    "linear/search",
                    "linear/get_issue",
                    "linear/create_issue",
                    "linear/update_issue",
                ],
            ),
            (
                "issue",
                None,
                3,
                &[
                    "github/get_issue",
                    "github/create_issue",
                    "linear/get_issue",
                ],
            ),
        ];
        for &(query, server_ids, max, expected) in cases {
            assert_eq!(search(&index, query, server_ids, max), expected, "{query}");
        }
    }

    #[tokio::test]
    async fn reuses_the_index_until_the_catalog_changes() {
        let cache = IndexCache::default();
        let servers = catalog();
        let first = cache.get(|| servers.clone()).await.unwrap();
        let again = cache.get(|| servers.clone()).await.unwrap();
        assert!(Arc::ptr_eq(&first, &again));

        // Searches arriving while it's built wait for that build, rather than starting their own
        let empty = IndexCache::default();
        let indexes =
            futures::future::join_all((0..8).map(|_| empty.get(|| servers.clone()))).await;
        let built = indexes[0].as_ref().unwrap();
        assert!(
            indexes
                .iter()
                .all(|index| Arc::ptr_eq(built, index.as_ref().unwrap()))
        );

        // Concurrent searches share it too
        let indexes =
            futures::future::join_all((0..8).map(|_| cache.get(|| servers.clone()))).await;
        for index in indexes {
            assert!(Arc::ptr_eq(&first, &index.unwrap()));
        }

        // A reconnect gives a server a new tool list, even with the same tools
        let mut reconnected = servers.clone();
        let ServerState::Ready(tools) = &reconnected[2].state else {
            unreachable!()
        };
        reconnected[2].state = ServerState::Ready(tools.to_vec().into());
        let after_reconnect = cache.get(|| reconnected.clone()).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &after_reconnect));

        // So does a server going down, which changes what's excluded
        let mut down = reconnected.clone();
        down[2].state = ServerState::Failed("refused".into());
        let after_failure = cache.get(|| down.clone()).await.unwrap();
        assert!(!Arc::ptr_eq(&after_reconnect, &after_failure));
        let results = after_failure.search("issue", None, 20).unwrap();
        assert_eq!(results.excluded_servers[0].server_id, "linear");
        assert!(
            results
                .matches
                .iter()
                .all(|found| found.server_id != "linear")
        );
        // Staying down with the same error keeps the index
        let still_down = cache.get(|| down.clone()).await.unwrap();
        assert!(Arc::ptr_eq(&after_failure, &still_down));
    }

    #[test]
    fn empty_queries_list_tools_in_catalog_order() {
        let index = Index::new(catalog());
        let all = search(&index, "  ", None, 50);
        assert_eq!(all.len(), 13);
        assert_eq!(
            all[..4],
            [
                "filesystem/read_file",
                "filesystem/move_file",
                "filesystem/search_files",
                "github/search",
            ]
        );
        assert_eq!(
            search(&index, "", Some(&["linear"]), 2),
            ["linear/search", "linear/get_issue"]
        );
    }

    #[test]
    fn exact_names_come_first_even_when_truncating() {
        let index = Index::new(catalog());
        assert_eq!(
            search(&index, "github/get_issue", None, 1),
            ["github/get_issue"]
        );
        assert_eq!(
            search(&index, "linear/getissue", None, 1),
            ["linear/get_issue"]
        );
    }

    #[test]
    fn ties_keep_catalog_order() {
        let tool: Tool = serde_json::from_value(serde_json::json!({
            "name": "same",
            "description": "Identical on both servers",
            "inputSchema": { "type": "object" },
        }))
        .unwrap();
        let server = |id: &str| CatalogServer {
            id: id.into(),
            description: None,
            state: ServerState::Ready(vec![tool.clone()].into()),
        };
        let index = Index::new(vec![server("a"), server("b")]);
        assert_eq!(search(&index, "identical", None, 20), ["a/same", "b/same"]);
    }

    #[test]
    fn selects_servers() {
        let index = Index::new(catalog());
        assert_eq!(search(&index, "issue", Some(&[]), 20), Vec::<String>::new());
        assert_eq!(
            search(&index, "search", Some(&["linear", "linear"]), 20),
            ["linear/search"]
        );
        let unknown = index.search("issue", Some(&["jira".into()]), 20);
        assert_eq!(unknown.err().unwrap(), "unknown server `jira`");
    }

    #[test]
    fn reports_servers_that_are_not_ready() {
        let mut servers = catalog();
        servers[1].state = ServerState::Connecting;
        servers[2].state = ServerState::Failed("refused".into());
        let index = Index::new(servers);

        let results = index.search("issue", None, 20).unwrap();
        assert_eq!(results.matches, []);
        assert_eq!(
            results.excluded_servers,
            [
                Excluded {
                    server_id: "github",
                    status: "connecting",
                    error: None,
                },
                Excluded {
                    server_id: "linear",
                    status: "failed",
                    error: Some("refused"),
                },
            ]
        );
        // Only selected servers are reported
        let results = index
            .search("rename", Some(&["filesystem".into()]), 20)
            .unwrap();
        assert_eq!(results.excluded_servers, []);
        assert_eq!(results.matches.len(), 1);
    }

    #[test]
    fn indexes_the_catalog_it_was_given() {
        let before = Index::new(catalog());
        let mut servers = catalog();
        let ServerState::Ready(tools) = &servers[2].state else {
            unreachable!()
        };
        let mut tools = tools.to_vec();
        tools[0].name = "triage".into();
        servers[2].state = ServerState::Ready(tools.into());
        let after = Index::new(servers);

        assert_eq!(search(&before, "triage", None, 1), Vec::<String>::new());
        assert_eq!(search(&after, "triage", None, 1), ["linear/triage"]);
        let linear_before = search(&before, "", Some(&["linear"]), 1);
        let linear_after = search(&after, "", Some(&["linear"]), 1);
        assert_eq!(
            (linear_before, linear_after),
            (vec!["linear/search".into()], vec!["linear/triage".into()])
        );
    }

    #[test]
    fn tokenizes_names_and_prose() {
        let cases: &[(&str, &[&str])] = &[
            ("get_issue", &["get", "issue"]),
            ("getHTTPServer", &["get", "http", "server"]),
            ("v2Api", &["v2", "api"]),
            ("ENG-123, file's", &["eng", "123", "file", "s"]),
            ("Ünïcode Wörds", &["ünïcode", "wörds"]),
            ("", &[]),
        ];
        for &(text, expected) in cases {
            assert_eq!(tokenize(text), expected, "{text}");
        }
        assert_eq!(normalize("GitHub/get_Issue"), "githubgetissue");
    }

    #[test]
    fn collects_schema_descriptions_and_parameter_names() {
        let schema = serde_json::json!({
            "type": "object",
            "description": "Top",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": { "status": { "description": "Nested" } },
                },
                "tags": { "type": "array", "items": { "description": "Item" } },
            },
        });
        let text = schema_text(schema.as_object().unwrap());
        let mut words: Vec<_> = text.split(' ').collect();
        words.sort();
        assert_eq!(words, ["Item", "Nested", "Top", "filter", "status", "tags"]);
    }

    #[test]
    fn caps_schema_text_at_a_character_boundary() {
        // Each `é` is two bytes, so the cap falls in the middle of one
        let description = "é".repeat(SCHEMA_TEXT_BYTES);
        let schema = serde_json::json!({ "description": "a", "properties": { "b": { "description": description } } });
        let text = schema_text(schema.as_object().unwrap());
        assert!(text.len() <= SCHEMA_TEXT_BYTES, "{}", text.len());
        assert!(text.len() > SCHEMA_TEXT_BYTES - 4, "{}", text.len());
    }
}
