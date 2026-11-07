import asyncio
import io
import unittest

from outleap.inventory_mcp import InventoryMCPServer, _read_frame, _write_frame


class TestInventoryMCP(unittest.IsolatedAsyncioTestCase):
    async def test_initialize_and_tools(self):
        server = InventoryMCPServer("127.0.0.1", 9063)
        initialize = await server._handle({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
        tools = await server._handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})

        self.assertEqual("firestorm-inventory", initialize["result"]["serverInfo"]["name"])
        self.assertEqual(9, len(tools["result"]["tools"]))

    def test_frame_round_trip(self):
        stream = io.BytesIO()
        _write_frame(stream, {"jsonrpc": "2.0", "id": 1, "result": {"ok": True}})
        stream.seek(0)
        self.assertEqual(
            {"jsonrpc": "2.0", "id": 1, "result": {"ok": True}},
            _read_frame(stream),
        )

    def test_notification_has_no_response(self):
        async def run():
            server = InventoryMCPServer("127.0.0.1", 9063)
            return await server._handle({"jsonrpc": "2.0", "method": "notifications/initialized"})

        self.assertIsNone(asyncio.run(run()))
