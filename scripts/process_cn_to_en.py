import argparse
import logging
import os
from typing import Dict, Optional, Tuple
from datetime import datetime

import requests
from ollama import Client  # type: ignore
from .fm_utils import translate_body_cjk_to_en


def setup_logger() -> None:
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(os.path.join("logs", "process_cn_to_en.log"), encoding="utf-8"),
        ],
    )


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_md(path: str) -> Tuple[Dict[str, str], str]:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    return {}, content


 


 


def translate_to_english_with_ollama(
    text: str,
    base_url: str,
    model: str,
    wait: float = 10.0,
) -> str:
    if not text:
        return ""
    try:
        import time
        _start_dt = datetime.now()
        logging.info(f"Ollama 调用开始: {_start_dt.strftime('%Y-%m-%d %H:%M:%S')} 模式=http 模型={model} 最大等待={wait}s")
        _t0 = time.perf_counter()
        prompt = (
            f"请将以下 Markdown 文本中的中文翻译为自然流畅的英文，保持原有的 Markdown 格式、链接与引用标识；"
            f"不要添加任何说明或多余内容；不要输出任何 'thinking' 或思考过程，仅输出最终的英文文本。\n\n{text}"
        )
        resp = requests.post(
            base_url.rstrip("/") + "/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
            },
            timeout=wait if wait and wait > 0 else 60,
        )
        if resp.status_code == 404:
            logging.error(f"Ollama 模型未找到：{model}。请先拉取或更换模型。")
        resp.raise_for_status()
        _t1 = time.perf_counter()
        _end_dt = datetime.now()
        logging.info(f"Ollama 调用结束: {_end_dt.strftime('%Y-%m-%d %H:%M:%S')} 模式=http 模型={model} 状态={resp.status_code} 耗时={_t1 - _t0:.2f}s")
        data = resp.json()
        return (data.get("response") or "").strip()
    except Exception as ex:
        logging.error(f"Ollama HTTP 调用失败：{ex}")
    return text


 


def process_file(path: str, out_dir: str, base_url: str, model: str, wait: float) -> Optional[str]:
    fm, body = read_md(path)
    lines = body.splitlines(keepends=True)
    chunks = ["".join(lines[i : i + 5]) for i in range(0, len(lines), 5)]
    logging.info(
        f"拆分 {os.path.basename(path)}: 总行数={len(lines)} 分块数={len(chunks)} 每块最多5行"
    )

    translated_parts = []
    for idx, chunk in enumerate(chunks, start=1):
        chunk_line_count = len(chunk.splitlines())
        logging.info(
            f"开始翻译分块 {idx}/{len(chunks)}: 行数={chunk_line_count} 字符数={len(chunk)}"
        )
        part = translate_to_english_with_ollama(chunk, base_url, model, wait)
        translated_parts.append(part)
        logging.info(
            f"完成翻译分块 {idx}/{len(chunks)}: 输出字符数={len(part)}"
        )

    en_body = "".join(translated_parts)
    # 统一中文过滤：如仍有中文，则使用 ArgosTranslate 做段内翻译与清理
    try:
        en_body, _replaced = translate_body_cjk_to_en(en_body, cancelled=False)
    except Exception:
        pass
    logging.info(
        f"合并 {os.path.basename(path)}: 段数={len(chunks)} 合并后字符数={len(en_body)}"
    )

    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, os.path.basename(path))
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(en_body)
    logging.info(f"写入英文 Markdown（无前言）: {out_path}")
    return out_path


def main() -> None:
    setup_logger()
    parser = argparse.ArgumentParser(description="读取中文 Markdown 并完成剩余逻辑，输出英文 Markdown 到 en 目录")
    parser.add_argument("--input-dir", default=os.environ.get("CN_OUTPUT_DIR", "results"), help="中文 Markdown 输入目录")
    parser.add_argument("--output-dir", default=os.environ.get("EN_OUTPUT_DIR", "en"), help="英文 Markdown 输出目录")
    parser.add_argument("--ollama-base", default=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434"))
    parser.add_argument("--ollama-model", default=os.environ.get("OLLAMA_MODEL", "qwen3:4b"))
    parser.add_argument("--ollama-wait", type=float, default=float(os.environ.get("OLLAMA_WAIT", 10)))
    args = parser.parse_args()

    ensure_dir(args.output_dir)
    files = [f for f in os.listdir(args.input_dir) if f.endswith(".md")]
    if not files:
        logging.warning("输入目录没有 Markdown 文件")
        return

    for name in files:
        path = os.path.join(args.input_dir, name)
        try:
            process_file(path, args.output_dir, args.ollama_base, args.ollama_model, args.ollama_wait)
        except Exception as e:
            logging.error(f"处理 {name} 失败：{e}")


if __name__ == "__main__":
    main()

