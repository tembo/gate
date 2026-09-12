mod config;

use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use rmcp::{
    ServerHandler, ServiceExt, handler::server::wrapper::Parameters, tool, tool_handler,
    tool_router, transport::stdio,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::config::Config;

#[derive(Parser)]
#[command(about = "WIP MCP gateway")]
struct Args {
    /// Path to the config file, in the `mcpServers` format
    #[arg(long, short)]
    config: PathBuf,
}

#[allow(dead_code)]
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ToolRef {
    /// ID of the MCP server
    server_id: String,
    /// Name of the tool on that server
    tool_name: String,
}

#[allow(dead_code)]
#[derive(Deserialize, JsonSchema)]
struct GetToolDetails {
    /// Tools to fetch details for, in a single batch
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
                entry
            })
            .collect();
        format!("{:#}", json!({ "servers": servers }))
    }

    #[tool(description = "Get the details of one or more tools from MCP servers")]
    async fn get_mcp_tool_details(&self, Parameters(_): Parameters<GetToolDetails>) -> String {
        String::new()
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
    Gate { config }.serve(stdio()).await?.waiting().await?;
    Ok(())
}
