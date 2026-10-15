mod config;
mod registry;
mod search;

use std::{path::PathBuf, sync::Arc, time::Duration};

use clap::Parser;
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, JsonObject},
    tool, tool_handler, tool_router,
    transport::stdio,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    registry::{Registry, RetryDelays, Status},
    search::Index,
};

const MAX_BATCH: usize = 20;
/// The most matches one search returns, and how many it returns by default
const MAX_SEARCH_RESULTS: usize = 50;
const DEFAULT_SEARCH_RESULTS: usize = 20;

#[derive(Parser)]
#[command(about = "WIP MCP gateway")]
struct Args {
    /// Path to the config file, in the `mcpServers` format
    #[arg(long, short)]
    config: PathBuf,

    /// How long a tool call may take, in seconds
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    call_timeout: u64,

    /// Milliseconds to wait before the second and third connection attempts. Hidden, since
    /// it's for tests that can't wait for the defaults.
    #[arg(long, hide = true, default_value = "2000,10000", value_parser = parse_retry_delays)]
    retry_delays: RetryDelays,
}

fn parse_retry_delays(text: &str) -> Result<RetryDelays, String> {
    let delays: Vec<u64> = text
        .split(',')
        .map(|delay| {
            delay
                .trim()
                .parse()
                .map_err(|_| format!("invalid delay `{delay}`"))
        })
        .collect::<Result<_, _>>()?;
    let [first, second] = delays[..] else {
        return Err("expected two delays in milliseconds, like `2000,10000`".into());
    };
    Ok([Duration::from_millis(first), Duration::from_millis(second)])
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ToolRef {
    /// ID of the MCP server
    server_id: String,
    /// Name of the tool on that server
    tool_name: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct Search {
    /// Words to look for in server and tool names, titles, descriptions and parameters. Leave
    /// out to list tools in order.
    query: Option<String>,
    /// Only search these servers. Leave out to search all of them.
    server_ids: Option<Vec<String>>,
    /// The most matches to return, from 1 to 50. Defaults to 20.
    #[schemars(range(min = 1, max = MAX_SEARCH_RESULTS))]
    max_results: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
struct SearchMcpTools {
    /// Searches to run, in a single batch
    #[schemars(length(min = 1, max = MAX_BATCH))]
    searches: Vec<Search>,
}

#[derive(Deserialize, JsonSchema)]
struct GetToolDetails {
    /// Tools to fetch details for, in a single batch
    #[schemars(length(min = 1, max = MAX_BATCH))]
    tools: Vec<ToolRef>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CallTool {
    /// ID of the MCP server
    server_id: String,
    /// Name of the tool on that server
    tool_name: String,
    /// Arguments to pass to the tool
    #[serde(default)]
    arguments: JsonObject,
}

#[derive(Clone)]
struct Gate {
    config: Arc<Config>,
    registry: Arc<Registry>,
}

#[tool_router]
impl Gate {
    #[tool(description = "List the configured MCP servers")]
    async fn list_mcp_servers(&self) -> String {
        let servers: Vec<_> = self
            .config
            .servers
            .iter()
            .map(|(id, server)| {
                let mut entry = json!({ "serverId": id, "transport": server.transport.kind() });
                if let Some(description) = &server.description {
                    entry["description"] = description.as_str().into();
                }
                match self.registry.status(id) {
                    // Errors are only shown once every attempt has failed
                    Some(Status::Attempting(_) | Status::Pending) | None => {
                        entry["status"] = "connecting".into();
                    }
                    Some(Status::Ready(connection)) => {
                        entry["status"] = "ready".into();
                        entry["toolCount"] = connection.tools.len().into();
                    }
                    Some(Status::Failed(error)) => {
                        entry["status"] = "failed".into();
                        entry["error"] = error.as_ref().into();
                    }
                }
                entry
            })
            .collect();
        json!({ "servers": servers }).to_string()
    }

    #[tool(description = "Search the tools of the connected MCP servers")]
    async fn search_mcp_tools(
        &self,
        Parameters(SearchMcpTools { searches }): Parameters<SearchMcpTools>,
    ) -> Result<String, String> {
        if !(1..=MAX_BATCH).contains(&searches.len()) {
            return Err(format!(
                "`searches` must have between 1 and {MAX_BATCH} items"
            ));
        }
        if searches.iter().any(|search| {
            search
                .max_results
                .is_some_and(|max| !(1..=MAX_SEARCH_RESULTS).contains(&max))
        }) {
            return Err(format!(
                "`maxResults` must be between 1 and {MAX_SEARCH_RESULTS}"
            ));
        }
        // One index for the whole batch, so every search sees the same servers and tools
        let index = Index::new(self.registry.catalog());
        let results: Vec<_> = searches
            .iter()
            .map(|search| {
                let query = search.query.as_deref().unwrap_or_default();
                let max_results = search.max_results.unwrap_or(DEFAULT_SEARCH_RESULTS);
                match index.search(query, search.server_ids.as_deref(), max_results) {
                    Ok(results) => json!(results),
                    Err(error) => json!({ "error": error }),
                }
            })
            .collect();
        Ok(json!({ "results": results }).to_string())
    }

    #[tool(description = "Get the details of one or more tools from MCP servers")]
    async fn get_mcp_tool_details(
        &self,
        Parameters(GetToolDetails { tools }): Parameters<GetToolDetails>,
        ct: CancellationToken,
    ) -> Result<String, String> {
        if !(1..=MAX_BATCH).contains(&tools.len()) {
            return Err(format!("`tools` must have between 1 and {MAX_BATCH} items"));
        }
        let lookups: Vec<_> = tools
            .iter()
            .map(|tool| (tool.server_id.as_str(), tool.tool_name.as_str()))
            .collect();
        let found = self.registry.tools(&lookups, &ct).await;
        let results: Vec<_> = tools
            .into_iter()
            .zip(found)
            .map(
                |(
                    ToolRef {
                        server_id,
                        tool_name,
                    },
                    found,
                )| match found {
                    Ok(mut tool) => {
                        tool.icons = None;
                        json!({ "serverId": server_id, "tool": tool })
                    }
                    Err(error) => {
                        json!({ "serverId": server_id, "toolName": tool_name, "error": error })
                    }
                },
            )
            .collect();
        Ok(json!({ "results": results }).to_string())
    }

    #[tool(description = "Call a tool on an MCP server")]
    async fn call_mcp_tool(
        &self,
        Parameters(CallTool {
            server_id,
            tool_name,
            arguments,
        }): Parameters<CallTool>,
        ct: CancellationToken,
    ) -> Result<CallToolResult, String> {
        // The server's result, including its own `isError`, is passed through as is
        self.registry
            .call(&server_id, &tool_name, arguments, &ct)
            .await
    }
}

#[tool_handler]
impl ServerHandler for Gate {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Arc::new(Config::load(&args.config)?);
    let registry = Arc::new(Registry::start(
        &config,
        Duration::from_secs(args.call_timeout),
        args.retry_delays,
    ));
    Gate { config, registry }
        .serve(stdio())
        .await?
        .waiting()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_retry_delays() {
        assert_eq!(
            parse_retry_delays("2000, 10000"),
            Ok([Duration::from_secs(2), Duration::from_secs(10)])
        );
        for invalid in ["", "2000", "1,2,3", "2000,soon", "-1,2"] {
            assert!(parse_retry_delays(invalid).is_err(), "{invalid}");
        }
    }
}
