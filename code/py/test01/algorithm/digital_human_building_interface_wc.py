from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import random
import json

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


@app.websocket("/building")
async def websocket_endpoint(websocket: WebSocket, connection_id: str):
    await manager.connect(websocket, connection_id)
    try:
        while True:
            data = await websocket.receive_json()
            # 提取 userId 和 text
            user_id = data.get("userId")
            text = data.get("text")

            # 模拟处理数据
            asyncio.create_task(process_websocket_data(connection_id, user_id, text))

    except WebSocketDisconnect:
        manager.disconnect(connection_id)


async def process_websocket_data(connection_id: str, user_id: int, text: str):
    """异步处理WebSocket数据"""
    try:
        # 模拟耗时处理
        await asyncio.sleep(random.uniform(1, 3))
        output_path = f"/home/liufeng/res/output_{user_id}.avi"
        result = {
            "userId": user_id,
            "responseMessage": output_path
        }

        # 发送处理结果
        await manager.send_message(connection_id, json.dumps(result))
    except Exception as e:
        error_result = {
            "userId": user_id,
            "error": f"处理出错: {str(e)}"
        }
        await manager.send_message(connection_id, json.dumps(error_result))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="192.168.18.154", port=8756)
