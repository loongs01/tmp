from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import random

app = FastAPI()


class VoiceRequest(BaseModel):
    userId: int
    bucketName: str
    objectName: str


class VoiceResponse(BaseModel):
    userId: int
    text: str
    code: int


@app.post("/voice-to-text", response_model=VoiceResponse)
async def voice_to_text(request: VoiceRequest):
    try:
        # 模拟语音转文字处理（实际调用语音识别 API）
        processed_text = f"语音文件 {request.objectName} 的转写文本"

        # 模拟处理成功
        return VoiceResponse(
            userId=request.userId,
            text=processed_text,
            code=200
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"语音处理失败: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="192.168.18.154", port=8756)
