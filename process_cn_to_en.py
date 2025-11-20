import argparse
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from ollama import Client  # type: ignore


def setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_md(path: str) -> Tuple[Dict[str, Any], str]:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    fm: Dict[str, Any] = {}
    body = content
    if content.startswith("---\n"):
        end = content.find("\n---\n", 4)
        if end != -1:
            yaml = content[4:end]
            body = content[end + 5 :]
            fm = parse_simple_yaml(yaml)
    return fm, body


def parse_simple_yaml(yaml: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for line in yaml.splitlines():
        if not line.strip() or line.strip().startswith("#"):
            continue
        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            result[key] = val
    return result


def build_yaml(fm: Dict[str, Any]) -> str:
    lines = ["---"]
    for k, v in fm.items():
        if isinstance(v, list):
            lines.append(f"{k}:")
            for item in v:
                lines.append(f"  - {item}")
        else:
            s = str(v).replace("\"", "\\\"")
            lines.append(f'{k}: "{s}"')
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def slugify(text: str) -> str:
    text = re.sub(r"[^\w\-\s]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "-", text.strip())
    return text.lower() or "index"


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
        time.sleep(max(wait, 0))
        client = Client(host=base_url)
        prompt = f"Translate the following text into natural English. Keep formatting.\n\n{text}"
        resp = client.generate(model=model, prompt=prompt)
        out = resp.get("response") if isinstance(resp, dict) else None
        if out:
            return out.strip()
    except Exception as e:
        logging.warning(f"Ollama 官方库调用失败，回退到 HTTP：{e}")
        try:
            resp = requests.post(
                base_url.rstrip("/") + "/api/generate",
                json={"model": model, "prompt": f"Translate to English:\n\n{text}"},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            return (data.get("response") or "").strip()
        except Exception as ex:
            logging.error(f"Ollama HTTP 回退失败：{ex}")
    return text


def extract_keywords_with_ollama(text: str, base_url: str, model: str, wait: float = 10.0) -> List[str]:
    if not text:
        return []
    prompt = (
        "Extract 5–10 concise keywords (single words or short phrases) from the text. "
        "Return as a comma-separated list only.\n\n" + text
    )
    try:
        import time
        time.sleep(max(wait, 0))
        client = Client(host=base_url)
        resp = client.generate(model=model, prompt=prompt)
        out = resp.get("response") if isinstance(resp, dict) else None
        if out:
            return [x.strip() for x in out.split(",") if x.strip()]
    except Exception as e:
        logging.warning(f"Ollama 官方库调用失败，回退到 HTTP：{e}")
        try:
            r = requests.post(
                base_url.rstrip("/") + "/api/generate",
                json={"model": model, "prompt": prompt},
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            out = (data.get("response") or "").strip()
            return [x.strip() for x in out.split(",") if x.strip()]
        except Exception as ex:
            logging.error(f"Ollama HTTP 回退失败：{ex}")
    return []


def extract_categories_and_tags_with_ollama(text: str, base_url: str, model: str, wait: float = 10.0) -> Tuple[List[str], List[str]]:
    if not text:
        return [], []
    prompt = (
        "Suggest 1–3 broad categories and 3–8 specific tags that best describe the content. "
        "Return JSON with keys 'categories' and 'tags'. Keep items short.\n\n" + text
    )
    try:
        import time, json
        time.sleep(max(wait, 0))
        client = Client(host=base_url)
        resp = client.generate(model=model, prompt=prompt)
        out = resp.get("response") if isinstance(resp, dict) else None
        if out:
            j = json.loads(out)
            cats = [x.strip() for x in j.get("categories", []) if x.strip()]
            tags = [x.strip() for x in j.get("tags", []) if x.strip()]
            return cats, tags
    except Exception as e:
        logging.warning(f"Ollama 官方库调用失败，回退到 HTTP：{e}")
        try:
            import json
            r = requests.post(
                base_url.rstrip("/") + "/api/generate",
                json={"model": model, "prompt": prompt},
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            out = (data.get("response") or "").strip()
            j = json.loads(out)
            cats = [x.strip() for x in j.get("categories", []) if x.strip()]
            tags = [x.strip() for x in j.get("tags", []) if x.strip()]
            return cats, tags
        except Exception as ex:
            logging.error(f"Ollama HTTP 回退失败：{ex}")
    return [], []


def process_file(path: str, out_dir: str, base_url: str, model: str, wait: float) -> Optional[str]:
    fm, body = read_md(path)
    cn_title = fm.get("title") or os.path.splitext(os.path.basename(path))[0]
    cn_desc = fm.get("description") or ""

    en_title = translate_to_english_with_ollama(cn_title, base_url, model, wait)
    en_desc = translate_to_english_with_ollama(cn_desc, base_url, model, wait) if cn_desc else ""
    en_body = translate_to_english_with_ollama(body, base_url, model, wait)

    keywords = extract_keywords_with_ollama(en_body, base_url, model, wait)
    categories, tags = extract_categories_and_tags_with_ollama(en_body, base_url, model, wait)

    dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    slug = slugify(en_title)
    url = f"/docs/{slug}/"

    out_fm: Dict[str, Any] = {
        "title": en_title,
        "description": en_desc or "",
        "summary": en_desc or "",
        "type": "docs",
        "draft": True,
        "url": url,
        "keywords": keywords,
        "categories": categories,
        "tags": tags,
        "lastmod": dt,
    }

    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"{slug}.md")
    yaml = build_yaml(out_fm)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(yaml)
        f.write(en_body)
    logging.info(f"写入英文 Markdown: {out_path}")
    return out_path


def main() -> None:
    setup_logger()
    parser = argparse.ArgumentParser(description="读取中文 Markdown 并完成剩余逻辑，输出英文 Markdown 到 en 目录")
    parser.add_argument("--input-dir", default=os.environ.get("CN_OUTPUT_DIR", "results"), help="中文 Markdown 输入目录")
    parser.add_argument("--output-dir", default=os.environ.get("EN_OUTPUT_DIR", "en"), help="英文 Markdown 输出目录")
    parser.add_argument("--ollama-base", default=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434"))
    parser.add_argument("--ollama-model", default=os.environ.get("OLLAMA_MODEL", "qwen2.5:14b"))
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

