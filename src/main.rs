use rmcp::{
    ServerHandler, ServiceExt, handler::server::wrapper::Parameters, tool, tool_handler,
    tool_router, transport::stdio,
};
use schemars::JsonSchema;
use serde::Deserialize;

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
struct Gate;

#[tool_router]
impl Gate {
    #[tool(description = "List the available MCP servers")]
    async fn list_mcp_servers(&self) -> String {
        String::new()
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Gate.serve(stdio()).await?.waiting().await?;
    Ok(())
}
