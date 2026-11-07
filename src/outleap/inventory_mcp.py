"""Small external MCP server for Firestorm's read-only inventory surface."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from typing import Any, Awaitable, Callable, Dict, Optional

from .api_wrappers import LLInventoryAPI
from .client import LEAPClient
from .protocol import LEAPProtocol


TOOLS = {
    "inventory.get_ui_state": {
        "description": "Return bounded active inventory panel state.",
        "inputSchema": {
            "type": "object",
            "properties": {"max_rows": {"type": "integer", "minimum": 1, "maximum": 100}},
        },
    },
    "inventory.expand_folder": {
        "description": "Expand a folder in the active inventory panel.",
        "inputSchema": {"type": "object", "required": ["folder_id"]},
    },
    "inventory.collapse_folder": {
        "description": "Collapse a folder in the active inventory panel.",
        "inputSchema": {"type": "object", "required": ["folder_id"]},
    },
    "inventory.select_object": {
        "description": "Select an object in the active inventory panel.",
        "inputSchema": {"type": "object", "required": ["object_id"]},
    },
    "inventory.set_filter": {
        "description": "Set the active inventory panel filter.",
        "inputSchema": {"type": "object", "required": ["filter"]},
    },
    "inventory.set_sort": {
        "description": "Set the active inventory panel sort order.",
        "inputSchema": {"type": "object", "required": ["sort_order"]},
    },
    "inventory.set_view_mode": {
        "description": "Set the active inventory panel view mode.",
        "inputSchema": {"type": "object", "required": ["view_mode"]},
    },
    "inventory.get_context_actions": {
        "description": "Return context action availability for an inventory object.",
        "inputSchema": {"type": "object", "required": ["object_id"]},
    },
    "inventory.resolve_link": {
        "description": "Resolve an inventory link without changing viewer state.",
        "inputSchema": {"type": "object", "required": ["object_id"]},
    },
}


def _uuid(value: Any) -> uuid.UUID:
    return uuid.UUID(str(value))


class InventoryMCPServer:
    def __init__(self, leap_host: str, leap_port: int):
        self._leap_host = leap_host
        self._leap_port = leap_port
        self._leap_ready = asyncio.Event()
        self._inventory: Optional[LLInventoryAPI] = None
        self._leap_client: Optional[LEAPClient] = None

    async def start(self) -> None:
        await asyncio.gather(self._serve_leap(), self._serve_mcp())

    async def _serve_leap(self) -> None:
        async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            if self._leap_client is not None:
                writer.close()
                await writer.wait_closed()
                return
            self._leap_client = LEAPClient(LEAPProtocol(reader, writer))
            await self._leap_client.connect()
            self._inventory = LLInventoryAPI(self._leap_client)
            self._leap_ready.set()

        server = await asyncio.start_server(accept, self._leap_host, self._leap_port)
        async with server:
            await server.serve_forever()

    async def _serve_mcp(self) -> None:
        while True:
            request = await asyncio.to_thread(_read_frame, sys.stdin.buffer)
            if request is None:
                return
            response = await self._handle(request)
            if response is not None:
                _write_frame(sys.stdout.buffer, response)

    async def _handle(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = request.get("method")
        request_id = request.get("id")
        if method == "notifications/initialized":
            return None
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "firestorm-inventory", "version": "0.1.0"},
                },
            }
        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": [
                {"name": name, **details} for name, details in TOOLS.items()
            ]}}
        if method == "tools/call":
            try:
                result = await self._call_tool(request["params"]["name"], request["params"].get("arguments", {}))
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(result, default=str)}],
                        "structuredContent": result,
                    },
                }
            except Exception as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "isError": True,
                        "content": [{"type": "text", "text": str(exc)}],
                    },
                }
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"Unknown method: {method}"},
        }

    async def _call_tool(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        await self._leap_ready.wait()
        if self._inventory is None:
            raise RuntimeError("LEAP is unavailable")
        calls: dict[str, Callable[[], Awaitable[dict[str, Any]]]] = {
            "inventory.get_ui_state": lambda: self._inventory.get_ui_state(args.get("max_rows", 100)),
            "inventory.expand_folder": lambda: self._inventory.expand_folder(_uuid(args["folder_id"])),
            "inventory.collapse_folder": lambda: self._inventory.collapse_folder(_uuid(args["folder_id"])),
            "inventory.select_object": lambda: self._inventory.select_object(_uuid(args["object_id"])),
            "inventory.set_filter": lambda: self._inventory.set_filter(args["filter"]),
            "inventory.set_sort": lambda: self._inventory.set_sort(args["sort_order"]),
            "inventory.set_view_mode": lambda: self._inventory.set_view_mode(args["view_mode"]),
            "inventory.get_context_actions": lambda: self._inventory.get_context_actions(_uuid(args["object_id"])),
            "inventory.resolve_link": lambda: self._inventory.resolve_link(_uuid(args["object_id"])),
        }
        if name not in calls:
            raise ValueError(f"Unknown inventory tool: {name}")
        return await calls[name]()


def _read_frame(stream: Any) -> Optional[Dict[str, Any]]:
    headers: Dict[str, str] = {}
    while True:
        line = stream.readline()
        if not line:
            return None
        decoded = line.decode("ascii").strip()
        if not decoded:
            break
        key, value = decoded.split(":", 1)
        headers[key.lower()] = value.strip()
    length = int(headers["content-length"])
    return json.loads(stream.read(length))


def _write_frame(stream: Any, message: Dict[str, Any]) -> None:
    payload = json.dumps(message, separators=(",", ":"), default=str).encode("utf-8")
    stream.write(f"Content-Length: {len(payload)}\r\n\r\n".encode("ascii"))
    stream.write(payload)
    stream.flush()


def main() -> None:
    host = os.environ.get("FIRESTORM_LEAP_HOST", "127.0.0.1")
    port = int(os.environ.get("FIRESTORM_LEAP_PORT", "9063"))
    asyncio.run(InventoryMCPServer(host, port).start())


if __name__ == "__main__":
    main()
