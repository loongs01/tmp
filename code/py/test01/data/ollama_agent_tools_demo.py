import os
import json
import time
import glob
import typing as t
import datetime as dt
import requests
import ast
import re

BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://192.168.18.154:11434")
CHAT_URL = f"{BASE_URL}/api/chat"
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "deepseek-r1:8b")
TOOL_MODE = os.environ.get("OLLAMA_TOOL_MODE", "auto").lower()  # auto | native | emulated


def _get_env_int(name: str, default_value: int) -> int:
    try:
        return int(os.environ.get(name, str(default_value)))
    except Exception:
        return default_value


DEFAULT_TIMEOUT_S = _get_env_int("OLLAMA_TIMEOUT_SECONDS", 300)
DEFAULT_MAX_RETRIES = _get_env_int("OLLAMA_MAX_RETRIES", 2)
DEFAULT_BACKOFF_S = float(os.environ.get("OLLAMA_RETRY_BACKOFF_SECONDS", "2.0"))
DEFAULT_NUM_PREDICT = _get_env_int("OLLAMA_NUM_PREDICT", 256)
DEFAULT_MAX_TOOL_ROUNDS = _get_env_int("OLLAMA_MAX_TOOL_ROUNDS", 12)


class CalculatorError(Exception):
    pass


def _evaluate_math_expression(expression: str) -> t.Union[int, float]:
    """Safely evaluate a simple arithmetic expression using Python AST.

    Supported operators: +, -, *, /, //, %, **, parentheses, unary +/-.
    Disallows names, calls, attributes, and other unsafe constructs.
    """

    def _eval(node: ast.AST) -> t.Union[int, float]:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return node.value
            raise CalculatorError("Only numeric constants are allowed")
        if isinstance(node, ast.Num):  # for older Python versions
            return node.n
        if isinstance(node, ast.BinOp):
            left = _eval(node.left)
            right = _eval(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.FloorDiv):
                return left // right
            if isinstance(node.op, ast.Mod):
                return left % right
            if isinstance(node.op, ast.Pow):
                return left ** right
            raise CalculatorError("Unsupported binary operator")
        if isinstance(node, ast.UnaryOp):
            operand = _eval(node.operand)
            if isinstance(node.op, ast.UAdd):
                return +operand
            if isinstance(node.op, ast.USub):
                return -operand
            raise CalculatorError("Unsupported unary operator")
        if isinstance(node, (ast.Call, ast.Attribute, ast.Subscript, ast.Name)):
            raise CalculatorError("Names, calls, attributes, or indexing are not allowed")
        raise CalculatorError(f"Unsupported expression: {type(node).__name__}")

    try:
        parsed = ast.parse(expression, mode="eval")
        return _eval(parsed)
    except CalculatorError:
        raise
    except Exception as exc:
        raise CalculatorError(str(exc))


# -------------------------------
# Tool implementations
# -------------------------------


def tool_get_time(timezone: str = "local") -> dict:
    now = dt.datetime.now()
    return {
        "ok": True,
        "timezone": timezone,
        "iso": now.isoformat(timespec="seconds"),
        "readable": now.strftime("%Y-%m-%d %H:%M:%S"),
    }


def tool_calculator(expression: str) -> dict:
    value = _evaluate_math_expression(expression)
    if isinstance(value, float) and value == 0:
        value = 0.0
    return {"ok": True, "expression": expression, "result": value}


def tool_list_dir(path: str) -> dict:
    try:
        if any(ch in path for ch in ["*", "?", "["]):
            entries = sorted(glob.glob(path))
        else:
            entries = sorted(os.listdir(path))
        return {"ok": True, "path": path, "entries": entries}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "path": path}


def tool_read_file(path: str, max_chars: int = 2000, encoding: str = "utf-8") -> dict:
    try:
        with open(path, "r", encoding=encoding) as f:
            content = f.read(max_chars)
        size = os.path.getsize(path)
        return {
            "ok": True,
            "path": path,
            "size_bytes": size,
            "content_preview": content,
            "truncated": size > len(content),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "path": path}


def tool_write_file(path: str, content: str, overwrite: bool = True, encoding: str = "utf-8") -> dict:
    try:
        parent = os.path.dirname(path) or "."
        os.makedirs(parent, exist_ok=True)
        if not overwrite and os.path.exists(path):
            return {"ok": False, "error": "File exists and overwrite is False", "path": path}
        with open(path, "w", encoding=encoding) as f:
            f.write(content)
        size = os.path.getsize(path)
        return {"ok": True, "path": path, "size_bytes": size}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "path": path}


# -------------------------------
# Tool registry and schemas
# -------------------------------


TOOLS_REGISTRY: dict[str, t.Callable[..., dict]] = {
    "get_current_time": tool_get_time,
    "calculator": tool_calculator,
    "list_directory": tool_list_dir,
    "read_file": tool_read_file,
    "write_file": tool_write_file,
}


def _build_tools_schema() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": "get_current_time",
                "description": "Get the current local time in human-readable and ISO formats.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "timezone": {
                            "type": "string",
                            "description": "Timezone hint. Only 'local' is supported in this demo.",
                            "default": "local",
                        }
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "calculator",
                "description": "Evaluate a simple arithmetic expression like '12*(3+4) - 5/2'.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "expression": {
                            "type": "string",
                            "description": "Arithmetic expression using + - * / // % ** and parentheses.",
                        }
                    },
                    "required": ["expression"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_directory",
                "description": "List entries for a directory or glob pattern.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Directory path or glob pattern (e.g., 'data/*.txt').",
                        }
                    },
                    "required": ["path"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a text file and return a preview.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File path to read."},
                        "max_chars": {
                            "type": "integer",
                            "description": "Max number of characters to read for preview.",
                            "default": 2000,
                        },
                        "encoding": {
                            "type": "string",
                            "description": "Text encoding (default utf-8).",
                            "default": "utf-8",
                        },
                    },
                    "required": ["path"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "write_file",
                "description": "Write text content to a file.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Destination file path."},
                        "content": {"type": "string", "description": "Text content to write."},
                        "overwrite": {
                            "type": "boolean",
                            "description": "Whether to overwrite an existing file.",
                            "default": True,
                        },
                        "encoding": {
                            "type": "string",
                            "description": "Text encoding (default utf-8).",
                            "default": "utf-8",
                        },
                    },
                    "required": ["path", "content"],
                },
            },
        },
    ]


# -------------------------------
# Ollama chat helpers
# -------------------------------


def _http_chat(
        messages: list[dict],
        model: str = DEFAULT_MODEL,
        tools: t.Optional[list[dict]] = None,
        stream: bool = False,
        options: t.Optional[dict] = None,
) -> dict:
    payload = {
        "model": model,
        "messages": messages,
        "stream": stream,
    }
    if tools:
        payload["tools"] = tools
    # Always pass some safe defaults to help reduce latency; allow user overrides
    merged_options = {
        "num_predict": DEFAULT_NUM_PREDICT,
    }
    if options:
        merged_options.update(options)
    payload["options"] = merged_options
    headers = {"Content-Type": "application/json"}
    last_err: Exception | None = None
    for attempt in range(1, DEFAULT_MAX_RETRIES + 2):  # e.g. 2 retries => 3 total attempts
        try:
            resp = requests.post(CHAT_URL, headers=headers, json=payload, timeout=DEFAULT_TIMEOUT_S)
            if resp.status_code != 200:
                raise RuntimeError(f"Chat request failed: {resp.status_code} {resp.text}")
            return resp.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            last_err = exc
            if attempt <= DEFAULT_MAX_RETRIES:
                time.sleep(DEFAULT_BACKOFF_S * attempt)
                continue
            break
    assert last_err is not None
    raise last_err


def run_agent_native(
        user_prompt: str,
        model: str = DEFAULT_MODEL,
        max_tool_rounds: int = 4,
        temperature: float = 0.2,
) -> str:
    tools = _build_tools_schema()
    messages: list[dict] = [
        {
            "role": "system",
            "content": (
                "你是一个可以调用工具的助手。\n"
                "- 当需要外部信息、计算、文件操作时，请优先调用合适的工具。\n"
                "- 工具返回的是 JSON，请提炼关键信息给用户。\n"
                "- 如果无法完成，直接说明原因，不要编造。\n"
            ),
        },
        {"role": "user", "content": user_prompt},
    ]

    final_text: str = ""
    for _ in range(max_tool_rounds):
        response = _http_chat(
            messages,
            model=model,
            tools=tools,
            stream=False,
            options={"temperature": temperature},
        )

        message = response.get("message", {})
        role = message.get("role", "assistant")
        content = message.get("content", "")
        tool_calls = message.get("tool_calls") or []

        messages.append({"role": role, "content": content, "tool_calls": tool_calls})

        if tool_calls:
            for call in tool_calls:
                fn = (call or {}).get("function", {})
                name = fn.get("name")
                args_raw = fn.get("arguments")
                call_id = call.get("id") or name
                try:
                    args = json.loads(args_raw) if isinstance(args_raw, str) else (args_raw or {})
                except json.JSONDecodeError:
                    args = {"__error": f"Invalid JSON arguments: {args_raw!r}"}

                if name not in TOOLS_REGISTRY:
                    tool_result = {"ok": False, "error": f"Unknown tool: {name}"}
                else:
                    try:
                        tool_result = TOOLS_REGISTRY[name](**args)
                    except TypeError as exc:
                        tool_result = {"ok": False, "error": f"Bad arguments: {str(exc)}", "args": args}
                    except Exception as exc:
                        tool_result = {"ok": False, "error": str(exc)}

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "name": name,
                        "content": json.dumps(tool_result, ensure_ascii=False),
                    }
                )

            continue

        final_text = content
        break

    return final_text or content


def _extract_json_objects(text: str) -> list[dict]:
    """Try to extract JSON object(s) from LLM output.

    Strategies: direct parse; fenced ```json blocks; balanced braces scanning.
    """
    results: list[dict] = []
    s = text.strip()
    s = re.sub(r"<think>[\s\S]*?</think>", "", s, flags=re.IGNORECASE)

    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return [obj]
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
    except Exception:
        pass

    for m in re.finditer(r"```(?:json)?\s*([\s\S]*?)\s*```", s, flags=re.IGNORECASE):
        block = m.group(1)
        try:
            obj = json.loads(block)
            if isinstance(obj, dict):
                results.append(obj)
        except Exception:
            continue
    if results:
        return results

    stack = []
    start = -1
    for i, ch in enumerate(s):
        if ch == '{':
            if not stack:
                start = i
            stack.append('{')
        elif ch == '}':
            if stack:
                stack.pop()
                if not stack and start != -1:
                    candidate = s[start:i + 1]
                    try:
                        obj = json.loads(candidate)
                        if isinstance(obj, dict):
                            results.append(obj)
                    except Exception:
                        pass
                    start = -1
    return results


def run_agent_emulated(
        user_prompt: str,
        model: str = DEFAULT_MODEL,
        max_tool_rounds: int = DEFAULT_MAX_TOOL_ROUNDS,
        temperature: float = 0.1,
) -> str:
    tool_docs = {
        "get_current_time": {"args": {"timezone": "string (default 'local')"}},
        "calculator": {"args": {"expression": "string"}},
        "list_directory": {"args": {"path": "string"}},
        "read_file": {
            "args": {"path": "string", "max_chars": "int (default 2000)", "encoding": "string (default utf-8)"}},
        "write_file": {"args": {"path": "string", "content": "string", "overwrite": "bool (default true)",
                                "encoding": "string (default utf-8)"}},
    }

    system_instruction = (
        "你将通过严格的 JSON 与我交互以调用工具：\n"
        "- 当需要使用工具时，只输出一个 JSON：{\"tool\": \"<name>\", \"arguments\": { ... }}。\n"
        "- 当你已经完成所有步骤并得出结论时，只输出一个 JSON：{\"final\": \"<具体回答>\"}。\n"
        "- 不能使用占位符（如 <回答>、<result>、...），必须填入真实完整内容。\n"
        "- 不要输出额外文字、不要 Markdown、不要解释。只输出一个 JSON 对象。\n"
        "示例1（调用计算器）：{\"tool\":\"calculator\",\"arguments\":{\"expression\":\"1+2\"}}\n"
        "示例2（最终回答）：{\"final\":\"现在时间是 2025-01-01 12:00:00，计算结果为 3。\"}\n"
        f"可用工具及参数：{json.dumps(tool_docs, ensure_ascii=False)}\n"
    )

    messages: list[dict] = [
        {"role": "system", "content": system_instruction},
        {"role": "user", "content": user_prompt},
    ]

    for round_idx in range(max_tool_rounds):
        response = _http_chat(
            messages,
            model=model,
            tools=None,
            stream=False,
            options={"temperature": temperature},
        )
        message = response.get("message", {})
        content = message.get("content", "")
        messages.append({"role": "assistant", "content": content})

        objs = _extract_json_objects(content)
        if not objs:
            messages.append({"role": "user", "content": "仅输出一个 JSON 对象，遵循上述格式。"})
            continue

        obj = objs[-1]
        if "final" in obj:
            final_text = str(obj.get("final") or "").strip()
            # Reject placeholder-like finals and ask for concrete content
            if final_text in {"<回答>", "<result>", "<answer>", "...", "…", "<final>"} or re.search(r"<[^>]+>",
                                                                                                    final_text):
                messages.append({
                    "role": "user",
                    "content": "不要输出占位符，请给出具体且完整的最终回答。仅输出一个 {\"final\":\"...\"} 的 JSON。",
                })
                continue
            return final_text

        if "tool" in obj and isinstance(obj.get("arguments"), dict):
            name = t.cast(str, obj.get("tool"))
            args = t.cast(dict, obj.get("arguments") or {})
            if name not in TOOLS_REGISTRY:
                tool_result = {"ok": False, "error": f"Unknown tool: {name}"}
            else:
                try:
                    tool_result = TOOLS_REGISTRY[name](**args)
                except TypeError as exc:
                    tool_result = {"ok": False, "error": f"Bad arguments: {str(exc)}", "args": args}
                except Exception as exc:
                    tool_result = {"ok": False, "error": str(exc)}

            messages.append(
                {
                    "role": "user",
                    "content": json.dumps({
                        "tool_result": {
                            "round": round_idx + 1,
                            "name": name,
                            "result": tool_result,
                        }
                    }, ensure_ascii=False),
                }
            )
            continue

        messages.append({"role": "user", "content": "格式错误：仅输出 {\"tool\":...} 或 {\"final\":...} 其中之一。"})

    return "对不起，未能在限定轮次内完成。"


def run_agent(
        user_prompt: str,
        model: str = DEFAULT_MODEL,
        max_tool_rounds: int = DEFAULT_MAX_TOOL_ROUNDS,
        temperature: float = 0.2,
) -> str:
    mode = TOOL_MODE if TOOL_MODE in ("auto", "native", "emulated") else "auto"

    if mode == "emulated":
        return run_agent_emulated(user_prompt, model=model, max_tool_rounds=max_tool_rounds, temperature=temperature)

    if mode == "native":
        return run_agent_native(user_prompt, model=model, max_tool_rounds=max_tool_rounds, temperature=temperature)

    try:
        return run_agent_native(user_prompt, model=model, max_tool_rounds=max_tool_rounds, temperature=temperature)
    except RuntimeError as err:
        msg = str(err)
        if "does not support tools" in msg or "400" in msg:
            return run_agent_emulated(user_prompt, model=model, max_tool_rounds=max_tool_rounds,
                                      temperature=temperature)
        raise


if __name__ == "__main__":
    demo_prompt = (
        "请完成以下任务：\n"
        "1) 告诉我现在的本地时间；\n"
        "2) 计算 123 * (45 - 6) / 3 的结果；\n"
        "3) 在 data 目录下创建 demo_agent_output.txt，写入一句话 'hello from ollama agent'；\n"
        "4) 然后列出 data 目录下的 .py 文件；\n"
        "5) 最后读取刚写入的文件预览并返回大小（字节）。"
    )
    try:
        answer = run_agent(demo_prompt, model=DEFAULT_MODEL)
        print("\n=== Agent Answer ===\n")
        print(answer)
    except Exception as e:
        print(f"Agent run failed: {e}")
