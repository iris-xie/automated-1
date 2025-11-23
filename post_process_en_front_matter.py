import argparse
import logging
import os
import re
import sys
import signal
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from ollama import Client  # type: ignore
from ollama_parser import parse_ollama_response
import json


def setup_logger() -> None:
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(os.path.join("logs", "post_process_en_front_matter.log"), encoding="utf-8"),
        ],
    )


CANCELLED = False


def _on_sigint(signum, frame) -> None:
    global CANCELLED
    CANCELLED = True


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_md(path: str) -> Tuple[Dict[str, Any], str]:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    return {}, content




def build_yaml(fm: Dict[str, Any]) -> str:
    lines = ["---"]
    for k, v in fm.items():
        if isinstance(v, list):
            lines.append(f"{k}:")
            for item in v:
                lines.append(f"  - {item}")
        elif isinstance(v, dict):
            lines.append(f"{k}:")
            for sk, sv in v.items():
                s2 = str(sv).replace('"', '\\"')
                lines.append(f"  {sk}: {s2}")
        else:
            s = str(v).replace('"', '\\"')
            lines.append(f"{k}: {s}")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def scheduled_timestamp_for_index(index: int) -> str:
    base = datetime.now()
    if index < 50:
        dt = base
    else:
        days = ((index - 50) // 3) + 1
        dt = base + timedelta(days=days)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def normalize_category(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9\-_.\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(w.capitalize() for w in s.split(" ")) if s else ""


def normalize_tag(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9\-_.]", "", s)
    s = re.sub(r"-{2,}", "-", s)
    return s


def reconcile_terms(
    proposed: List[str],
    pool: List[str],
    max_size: int,
    ollama_base: str,
    ollama_model: str,
    for_category: bool,
    ollama_wait: float = 0.0,
) -> List[str]:
    selected: List[str] = []
    normalize = normalize_category if for_category else normalize_tag
    for term in proposed:
        norm = normalize(term)
        if not norm:
            continue
        pool_lower = [p.lower() for p in pool]
        if norm.lower() in pool_lower:
            idx = pool_lower.index(norm.lower())
            selected.append(pool[idx])
            continue
        if len(pool) < max_size:
            pool.append(norm)
            selected.append(norm)
        else:
            selected.append(pool[0] if pool else norm)
    return selected


def inline_list(vals: List[str]) -> str:
    if not vals:
        return "[]"
    safe = [f'"{v}"' for v in vals]
    return "[ " + ", ".join(safe) + " ]"


def resolve_ollama_model(base_url: str, preferred: str) -> str:
    candidates: List[str] = []
    env_model = os.environ.get("OLLAMA_MODEL")
    if env_model and env_model.strip():
        candidates.append(env_model.strip())
    if preferred and preferred.strip():
        candidates.append(preferred.strip())
    candidates += [
        "deepseek-r1:7b",
        "qwen2:7b",
        "qwen3:4b",
        "llama3.2:3b",
    ]
    seen: set[str] = set()
    unique_candidates = [c for c in candidates if not (c in seen or seen.add(c))]
    try:
        r = requests.get(base_url.rstrip("/") + "/api/tags", timeout=10)
        r.raise_for_status()
        data = r.json()
        models = data.get("models") if isinstance(data, dict) else None
        names = []
        if isinstance(models, list):
            for m in models:
                name = m.get("name") if isinstance(m, dict) else None
                if isinstance(name, str):
                    names.append(name)
        for cand in unique_candidates:
            if cand in names:
                return cand
    except Exception:
        pass
    return unique_candidates[0] if unique_candidates else preferred


def analyze_content_with_ollama(text: str, base_url: str, model: str, wait: float = 10.0) -> Dict[str, Any]:
    if not text:
        return {"title": "", "description": "", "keywords": [], "categories": [], "tags": []}
    prompt = (
        "请根据以下英文内容返回一个 JSON 对象，包含五个字段："
        "title（不超过60字符的英文标题）、"
        "description（符合 Google SEO 标准的英文 meta 描述，不超过160字符，包含主要关键词，简洁自然，无引号，无换行）、"
        "keywords（最多20个英文关键词的数组）、"
        "categories（1–3个宽泛分类的数组）、tags（3–8个具体标签的数组）。"
        "不要输出任何解释或思考过程，仅输出 JSON。\n\n" + text
    )
    try:
        import json, time
        _start_dt = datetime.now()
        logging.info(f"Ollama 调用开始: {_start_dt.strftime('%Y-%m-%d %H:%M:%S')} 模式=http 模型={model} 最大等待={wait}s")
        _t0 = time.perf_counter()
        r = requests.post(
            base_url.rstrip("/") + "/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=(5, wait if wait and wait > 0 else 60),
        )
        if r.status_code == 404:
            logging.error(f"Ollama 模型未找到：{model}")
        r.raise_for_status()
        logging.info(f"Ollama 原始响应: {r.text}")
        _t1 = time.perf_counter()
        _end_dt = datetime.now()
        logging.info(f"Ollama 调用结束: {_end_dt.strftime('%Y-%m-%d %H:%M:%S')} 模式=http 模型={model} 状态={r.status_code} 耗时={_t1 - _t0:.2f}s")
        res = parse_ollama_response(r.text)
        logging.info(f"Ollama 处理后响应: {json.dumps(res, ensure_ascii=False)}")
        return res
    except KeyboardInterrupt:
        raise
    except Exception:
        return {"title": "", "description": "", "keywords": [], "categories": [], "tags": []}


def generate_title_with_ollama(text: str, base_url: str, model: str, wait: float = 10.0) -> str:
    res = analyze_content_with_ollama(text, base_url, model, wait)
    s = str(res.get("title", "")).strip()
    return re.sub(r"[\r\n]+", " ", s).strip()

def process_file(path: str, base_url: str, model: str, wait: float, idx: int, prev_url: Optional[str], cat_pool: List[str], tag_pool: List[str]) -> Optional[str]:
    if CANCELLED:
        raise KeyboardInterrupt
    fm, body = read_md(path)
    try:
        import re
        m = re.match(r"^---\r?\n([\s\S]*?)\r?\n---\r?\n", body)
        if m:
            old_yaml = m.group(1)
            body = body[m.end():]
            logging.info(f"检测到并移除旧前言: 文件={os.path.basename(path)} 字符数={len(old_yaml)}")
    except Exception:
        pass
    # Step 1: Use ArgosTranslate to translate remaining Chinese to English, pad with spaces
    try:
        import re
        from datetime import datetime
        _start_dt = datetime.now()
        logging.info(f"ArgosTranslate 开始: {_start_dt.strftime('%Y-%m-%d %H:%M:%S')} 文件={os.path.basename(path)}")
        cjk_pattern = re.compile(r"[\u3400-\u4DBF\u4E00-\u9FFF\u3000-\u303F\uFE30-\uFE4F\uF900-\uFAFF\uFF00-\uFFEF\U00020000-\U0002A6DF\U0002A700-\U0002B81F\U0002B820-\U0002CEAF\U0002F800-\U0002FA1F]+")
        from argostranslate import translate as argos_translate  # type: ignore
        installed = argos_translate.get_installed_languages()
        zh = next((l for l in installed if getattr(l, 'code', '') == 'zh'), None)
        en = next((l for l in installed if getattr(l, 'code', '') == 'en'), None)
        replaced = 0
        if zh and en:
            tr = zh.get_translation(en)
            def repl(m: re.Match) -> str:
                nonlocal replaced
                if CANCELLED:
                    raise KeyboardInterrupt
                src = m.group(0)
                try:
                    out = tr.translate(src)
                except Exception:
                    out = ""
                replaced += 1
                out = (out or "").strip()
                return f" {out} " if out else " "
            body = cjk_pattern.sub(repl, body)
            def is_cjk(ch: str) -> bool:
                cp = ord(ch)
                return (
                    0x3400 <= cp <= 0x4DBF or
                    0x4E00 <= cp <= 0x9FFF or
                    0x20000 <= cp <= 0x2A6DF or
                    0x2A700 <= cp <= 0x2B81F or
                    0x2B820 <= cp <= 0x2CEAF or
                    0xF900 <= cp <= 0xFAFF or
                    0x2F800 <= cp <= 0x2FA1F or
                    0x3000 <= cp <= 0x303F or
                    0xFE30 <= cp <= 0xFE4F or
                    0xFF00 <= cp <= 0xFFEF
                )
            if any(is_cjk(ch) for ch in body):
                out_chars = []
                i = 0
                n = len(body)
                while i < n:
                    if CANCELLED:
                        raise KeyboardInterrupt
                    if is_cjk(body[i]):
                        j = i
                        while j < n and is_cjk(body[j]):
                            j += 1
                        src = body[i:j]
                        try:
                            trans = tr.translate(src).strip()
                        except Exception:
                            trans = ""
                        replaced += 1
                        out_chars.append(f" {trans} " if trans else " ")
                        i = j
                    else:
                        out_chars.append(body[i])
                        i += 1
                body = "".join(out_chars)
            body = cjk_pattern.sub("", body)
        else:
            body = cjk_pattern.sub(" ", body)
            body = cjk_pattern.sub("", body)
        _end_dt = datetime.now()
        logging.info(f"ArgosTranslate 结束: {_end_dt.strftime('%Y-%m-%d %H:%M:%S')} 文件={os.path.basename(path)} 替换段数={replaced}")
        logging.info(f"ArgosTranslate 覆盖写入开始: 文件={os.path.basename(path)} 字符数={len(body)}")
        if CANCELLED:
            raise KeyboardInterrupt
        with open(path, "w", encoding="utf-8") as wf:
            wf.write(body)
        logging.info(f"ArgosTranslate 覆盖写入结束: 文件={os.path.basename(path)}")
    except Exception as e:
        logging.warning(f"ArgosTranslate 处理失败，跳过：{e}")
    if CANCELLED:
        raise KeyboardInterrupt
    analysis = analyze_content_with_ollama(body, base_url, model, wait)
    title = (analysis.get("title") or "").strip() or os.path.splitext(os.path.basename(path))[0]
    description = (analysis.get("description") or "").strip()
    url = fm.get("url") or ""
    publish_date = scheduled_timestamp_for_index(idx)
    lastmod = publish_date
    ar_cats = analysis.get("categories", [])
    ar_tags = analysis.get("tags", [])
    ar_kws = analysis.get("keywords", [])
    cats = reconcile_terms(ar_cats, cat_pool, 70, base_url, model, True, ollama_wait=wait)
    tags = reconcile_terms(ar_tags, tag_pool, 300, base_url, model, False, ollama_wait=wait)
    keywords = ar_kws
    out_fm: Dict[str, Any] = {
        "publishDate": f"\"{publish_date}\"",
        "lastmod": f"\"{lastmod}\"",
        "title": title,
        "description": description,
        "summary": description,
        "url": url,
        "categories": cats,
        "tags": tags,
        "keywords": inline_list(keywords),
        "type": "docs",
        "prev": prev_url or "",
        "sidebar": {"open": True},
    }
    yaml = build_yaml(out_fm)
    with open(path, "w", encoding="utf-8") as f:
        f.write(yaml)
        f.write(body)
    logging.info(f"更新英文 Markdown 前言: {path}")
    return url or None


def main() -> None:
    setup_logger()
    signal.signal(signal.SIGINT, _on_sigint)
    parser = argparse.ArgumentParser(description="为英文 Markdown 添加前言、分类、标签和关键词")
    parser.add_argument("--input-dir", default=os.environ.get("EN_OUTPUT_DIR", "en"))
    parser.add_argument("--ollama-base", default=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434"))
    parser.add_argument("--ollama-model", default=os.environ.get("OLLAMA_MODEL", "deepseek-r1:7b"))
    parser.add_argument("--ollama-wait", type=float, default=float(os.environ.get("OLLAMA_WAIT", 3000)))
    args = parser.parse_args()

    files = [os.path.join(args.input_dir, f) for f in os.listdir(args.input_dir) if f.endswith(".md")]
    if not files:
        logging.info("没有英文 Markdown 文件")
        return

    model = resolve_ollama_model(args.ollama_base, args.ollama_model)
    if model != args.ollama_model:
        logging.info(f"使用可用模型: {model}")

    prev_url: Optional[str] = None
    cat_pool: List[str] = []
    tag_pool: List[str] = []
    idx = 0
    for path in files:
        try:
            prev_url = process_file(path, args.ollama_base, model, args.ollama_wait, idx, prev_url, cat_pool, tag_pool) or prev_url
            idx += 1
        except Exception as e:
            logging.error(f"处理 {os.path.basename(path)} 失败: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.")
        sys.exit(130)
def _parse_ollama_text(raw: str) -> Dict[str, Any]:
    try:
        import json
        return json.loads(raw)
    except Exception:
        import json
        parts = [p for p in raw.splitlines() if p.strip()]
        for p in reversed(parts):
            try:
                return json.loads(p)
            except Exception:
                continue
    return {}


def _strip_code_fences(s: str) -> str:
    try:
        m = re.search(r"```[a-zA-Z0-9]*\s*([\s\S]*?)\s*```", s)
        if m:
            return m.group(1)
        return s
    except Exception:
        return s