#!/usr/bin/env python3
import os

from openai import OpenAI


def main() -> None:
    base_url = os.getenv("QWEN35_BASE_URL", "http://127.0.0.1:8000/v1")
    api_key = os.getenv("QWEN35_API_KEY", "EMPTY")
    model = os.getenv("QWEN35_MODEL", "Qwen3.5-27B-Q4_K_M.gguf")

    client = OpenAI(base_url=base_url, api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "你是一个简洁的中文助手。"},
            {"role": "user", "content": "用一句话介绍你自己。"},
        ],
        temperature=0.7,
        max_tokens=128,
    )
    print(resp.choices[0].message.content)


if __name__ == "__main__":
    main()
