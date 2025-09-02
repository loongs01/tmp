from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import random

app = FastAPI()


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, connection_id: str):
        await websocket.accept()
        self.active_connections[connection_id] = websocket

    def disconnect(self, connection_id: str):
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]

    async def send_message(self, connection_id: str, message: str):
        if connection_id in self.active_connections:
            await self.active_connections[connection_id].send_text(message)


manager = ConnectionManager()


@app.websocket("/test")
async def websocket_endpoint(websocket: WebSocket, connection_id: str):
    await manager.connect(websocket, connection_id)
    try:
        while True:
            data = await websocket.receive_text()
            # data = await websocket.receive_bytes()
            # 立即响应
            await websocket.send_bytes(f"消息已接收: {data}")

            # 启动后台任务处理数据
            asyncio.create_task(process_websocket_data(connection_id, data))
    except WebSocketDisconnect:
        manager.disconnect(connection_id)


async def process_websocket_data(connection_id: str, data: str):
    """异步处理WebSocket数据"""
    try:
        # 模拟耗时处理
        await asyncio.sleep(random.uniform(1, 3))
        result = f"处理结果: {data.upper()}"

        # 发送处理结果
        await manager.send_message(connection_id, result)
    except Exception as e:
        await manager.send_message(connection_id, f"处理出错: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="192.168.18.154", port=8756)
