import requests
import json


def stream_response(prompt, model_name="deepseek-r1:8b"):
    url = "http://192.168.18.154:11434/api/generate"
    headers = {"Content-Type": "application/json"}
    data = {
        "model": model_name,
        "prompt": prompt,
        "stream": True  # 启用流式响应
    }

    with requests.post(url, headers=headers, json=data, stream=True) as response:
        if response.status_code != 200:
            raise Exception(f"请求失败: {response.text}")

        print("实时生成结果:")
        for line in response.iter_lines():
            if line:
                chunk = json.loads(line.decode('utf-8'))
                print(chunk.get("response", ""), end="", flush=True)


if __name__ == "__main__":
    prompt = "今天日期？"
    stream_response(prompt)
