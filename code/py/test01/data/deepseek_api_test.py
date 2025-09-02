import requests
import json
import time

# DeepSeek API 配置
DEEPSEEK_API_KEY = "sk-8836e19e374e458ba9e1e1cd0052a2b0"
DEEPSEEK_ENDPOINT = "https://api.deepseek.com/v1/chat/completions"
MODEL_NAME = "deepseek-chat"


def chat_with_deepseek(prompt, max_tokens=2000, temperature=0.7):
    """与 DeepSeek 进行交互"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature
    }

    try:
        response = requests.post(DEEPSEEK_ENDPOINT, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except requests.exceptions.RequestException as e:
        return f"请求失败: {str(e)}"
    except (KeyError, IndexError) as e:
        return f"解析响应失败: {str(e)}"


def main():
    print("DeepSeek 聊天助手 (输入 'exit' 退出)")
    print("=" * 40)

    while True:
        user_input = input("\n[你]: ")

        if user_input.lower() in ["exit", "quit"]:
            print("再见！")
            break

        if not user_input.strip():
            print("请输入有效内容")
            continue

        print("\n[DeepSeek]: ", end="", flush=True)

        start_time = time.time()
        response = chat_with_deepseek(user_input)
        elapsed_time = time.time() - start_time

        print(response)
        print(f"\n[响应时间: {elapsed_time:.2f}秒]")


if __name__ == "__main__":
    main()