#!/usr/bin/env python3
"""Smoke-tests a built gate binary over stdio: python3 scripts/smoke-test.py path/to/gate

Starts gate with no servers, lists its tools, and runs a TypeScript script through
execute_mcp_code, which starts gate again as a code worker. Uses only the standard library.
"""

import json
import os
import subprocess
import sys
import tempfile
import threading

EXPECTED_TOOLS = {
    "call_mcp_tool",
    "execute_mcp_code",
    "get_mcp_tool_details",
    "list_mcp_servers",
    "search_mcp_tools",
}


def main() -> None:
    binary = sys.argv[1]
    with tempfile.TemporaryDirectory() as directory:
        config = os.path.join(directory, "gate.json")
        with open(config, "w") as file:
            json.dump({"mcpServers": {}}, file)
        gate = subprocess.Popen(
            [binary, "--config", config],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        # Fails the test instead of hanging if gate stops answering
        timer = threading.Timer(60, gate.kill)
        timer.start()
        try:
            run(gate)
        finally:
            timer.cancel()
            gate.stdin.close()
            gate.wait(timeout=10)
    print("smoke test passed")


def run(gate: subprocess.Popen) -> None:
    next_id = 0

    def request(method: str, params: dict) -> dict:
        nonlocal next_id
        next_id += 1
        send({"jsonrpc": "2.0", "id": next_id, "method": method, "params": params})
        while True:
            line = gate.stdout.readline()
            if not line:
                sys.exit(f"gate exited before answering {method}")
            message = json.loads(line)
            if message.get("id") == next_id:
                if "error" in message:
                    sys.exit(f"{method} failed: {message['error']}")
                return message["result"]

    def send(message: dict) -> None:
        gate.stdin.write(json.dumps(message) + "\n")
        gate.stdin.flush()

    request(
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "smoke-test", "version": "0"},
        },
    )
    send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    tools = {tool["name"] for tool in request("tools/list", {})["tools"]}
    if tools != EXPECTED_TOOLS:
        sys.exit(f"unexpected tools: {sorted(tools)}")

    result = request(
        "tools/call",
        {
            "name": "execute_mcp_code",
            "arguments": {"code": "const n: number = 41;\nreturn n + 1;"},
        },
    )
    text = result["content"][0]["text"]
    if result.get("isError") or json.loads(text).get("result") != 42:
        sys.exit(f"execute_mcp_code returned {text}")


if __name__ == "__main__":
    main()
