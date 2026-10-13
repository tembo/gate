mod config;
mod registry;

use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use rmcp::{
    ServerHandler, ServiceExt, handler::server::wrapper::Parameters, tool, tool_handler,
    tool_router, transport::stdio,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::{
    config::Config,
    registry::{Registry, Status},
};

const MAX_BATCH: usize = 20;

#[derive(Parser)]
#[command(about = "WIP MCP gateway")]
struct Args {
    /// Path to the config file, in the `mcpServers` format
    #[arg(long, short)]
    config: PathBuf,
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
struct GetToolDetails {
    /// Tools to fetch details for, in a single batch
    #[schemars(length(min = 1, max = MAX_BATCH))]
    tools: Vec<ToolRef>,
}

#[allow(dead_code)]
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CallTool {
    /// ID of the MCP server
    server_id: String,
    /// Name of the tool on that server
    tool_name: String,
    /// Arguments to pass to the tool
    arguments: Option<serde_json::Map<String, serde_json::Value>>,
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
                    Some(Status::Connecting) | None => entry["status"] = "connecting".into(),
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

    #[tool(description = "Get the details of one or more tools from MCP servers")]
    async fn get_mcp_tool_details(
        &self,
        Parameters(GetToolDetails { tools }): Parameters<GetToolDetails>,
    ) -> Result<String, String> {
        if !(1..=MAX_BATCH).contains(&tools.len()) {
            return Err(format!("`tools` must have between 1 and {MAX_BATCH} items"));
        }
        let mut results = Vec::with_capacity(tools.len());
        // Servers connect in the background, so waiting on each in turn only takes as long as
        // the slowest one
        for ToolRef {
            server_id,
            tool_name,
        } in tools
        {
            results.push(match self.registry.tool(&server_id, &tool_name).await {
                Ok(mut tool) => {
                    tool.icons = None;
                    json!({ "serverId": server_id, "tool": tool })
                }
                Err(error) => {
                    json!({ "serverId": server_id, "toolName": tool_name, "error": error })
                }
            });
        }
        Ok(json!({ "results": results }).to_string())
    }

    #[tool(description = "Call a tool on an MCP server")]
    async fn call_mcp_tool(&self, Parameters(_): Parameters<CallTool>) -> String {
        String::new()
    }
}

#[tool_handler]
impl ServerHandler for Gate {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Arc::new(Config::load(&args.config)?);
    let registry = Arc::new(Registry::start(&config));
    Gate { config, registry }
        .serve(stdio())
        .await?
        .waiting()
        .await?;
    Ok(())
}
