#!/usr/bin/env python3
"""
Use a local Firecrawl service to recursively crawl a website until completion
and write Markdown files under a configurable output directory (default `results/`)
with YAML front matter.

Front matter format example:
---
 title: Abertay University (University of Abertay Dundee)
 type: docs
 prev: colleges/Aberystwyth-University-(Prifysgol-Aberystwyth)
 date: "2025-02-21 18:01:17"
 sidebar:
    open: true
---

Usage:
  python crawl_with_firecrawl.py --start-url https://example.com \
    [--firecrawl-base http://localhost:3002] [--max-pages 0] [--delay 0.2]

Notes:
  - Assumes a local Firecrawl server is running and reachable.
  - Restricts crawling to the start URL's domain by default.
  - Creates files inside `results/<domain>/<path>.md` mirroring URL paths.
"""

import argparse
import asyncio
import html as html_module
import json
import logging
import os
import re
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from urllib.parse import urljoin, urldefrag, urlparse

import httpx  # type: ignore
import html2text  # type: ignore
import requests
from markdownify import markdownify as md  # type: ignore
from firecrawl import AsyncFirecrawl  # type: ignore
from ollama import Client  # type: ignore

def setup_logger():
    """Configure logging to write to ./logs/crawl.log and console.
    Creates a sibling 'logs' directory next to this script if missing.
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        base_dir = os.getcwd()
    logs_dir = os.path.join(base_dir, "logs")
    try:
        os.makedirs(logs_dir, exist_ok=True)
    except Exception:
        # If directory creation fails, continue with console-only logging
        logs_dir = base_dir

    log_file = os.path.join(logs_dir, "crawl.log")

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid duplicate handlers if setup_logger is called multiple times
    root.handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    # File handler
    try:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    except Exception as e:
        # Fall back silently to console-only if file handler fails
        logging.warning(f"Failed to attach file logger at {log_file}: {e}")


def load_env_file(path: str = ".env") -> None:
    """Load key=value pairs from a .env file into os.environ if present.
    - Ignores blank lines and lines starting with '#'
    - Supports values wrapped in single or double quotes
    - Does not override existing environment variables
    """
    try:
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip()
                if not key:
                    continue
                if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                    val = val[1:-1]
                # Only set if not already provided by real environment
                if key not in os.environ:
                    os.environ[key] = val
    except Exception as e:
        logging.warning(f"Failed to load .env from {path}: {e}")


def find_cli_arg(argv: list[str], name: str) -> str | None:
    """Find a CLI argument by name and return its value.
    Supports forms: `--name value` and `--name=value`.
    Returns None if not present or value missing.
    """
    try:
        for i, a in enumerate(argv):
            if a == name and i + 1 < len(argv):
                return argv[i + 1]
            if a.startswith(name + "="):
                return a.split("=", 1)[1]
    except Exception:
        pass
    return None


def normalize_url(u: str) -> str:
    # Remove fragments; normalize scheme/host casing
    u = urldefrag(u)[0]
    parsed = urlparse(u)
    # strip default ports
    netloc = parsed.hostname or ""
    if parsed.port:
        if not ((parsed.scheme == "http" and parsed.port == 80) or (parsed.scheme == "https" and parsed.port == 443)):
            netloc = f"{netloc}:{parsed.port}"
    path = parsed.path or "/"
    # remove duplicated slashes
    path = re.sub(r"/+", "/", path)
    return f"{parsed.scheme}://{netloc}{path}"


def in_same_domain(url: str, root_netloc: str) -> bool:
    parsed = urlparse(url)
    return (parsed.hostname or "") == root_netloc


def path_to_file_parts(url: str, base_dir: str) -> tuple[str, str]:
    """Return (dir_path_under_output_dir, file_name) derived from URL path.
    Mirrors the URL path inside `<output_dir>/<domain>/...` and ensures `.md`.
    """
    p = urlparse(url)
    domain = p.hostname or "unknown-domain"
    # keep query-independent path
    path = p.path or "/"
    if path.endswith("/"):
        path = path + "index"
    # sanitize segments for filesystem safety
    safe_path = re.sub(r"[^A-Za-z0-9_\-/().]", "-", path)
    # split into directory and filename
    dir_part, _, file_part = safe_path.rpartition("/")
    dir_under_results = os.path.join(base_dir, domain, dir_part) if dir_part else os.path.join(base_dir, domain)
    filename = f"{file_part}.md"
    return dir_under_results, filename


def extract_title_from_markdown(md: str, fallback: str) -> str:
    # Try first ATX header line
    for line in md.splitlines():
        s = line.strip()
        if s.startswith("# ") or s.startswith("## "):
            return s.lstrip("# ").strip()
    return fallback


def current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def scheduled_timestamp_for_index(index: int) -> str:
    """Compute scheduled timestamp based on page index:
    - For first 50 pages (index 0..49): current date
    - After 50, every 3 pages increments one day: 50-52 => +1 day, 53-55 => +2 days, ...
    """
    base = datetime.now()
    if index < 50:
        dt = base
    else:
        days = ((index - 50) // 3) + 1
        dt = base + timedelta(days=days)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def make_url_from_title(title: str, max_bytes: int = 30) -> str:
    """Create a URL filename from English title:
    - lowercase
    - replace spaces with '-'
    - remove punctuation (keep a-z, 0-9, '-')
    - ensure total length (including .html) <= max_bytes (UTF-8 bytes)
    """
    if not title:
        base = "index"
    else:
        base = title.lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9-]", "", base)
        base = re.sub(r"-{2,}", "-", base).strip("-")
        if not base:
            base = "index"

    suffix = ".html"
    out_chars: list[str] = []
    total = 0
    for ch in base:
        b = len(ch.encode("utf-8"))
        # reserve space for suffix
        if total + b + len(suffix.encode("utf-8")) > max_bytes:
            break
        out_chars.append(ch)
        total += b
    final = "".join(out_chars) + suffix
    # If even suffix alone exceeds (extremely small max), fall back
    if len(final.encode("utf-8")) > max_bytes:
        return "index.html"[:max_bytes]
    return final


def make_unique_url_from_title(title: str, used: set[str], max_bytes: int = 30) -> str:
    """Generate a unique URL (filename) from title.
    First create a base URL within max_bytes, then append -N before .html if duplicated.
    The uniqueness suffix may exceed the 30-byte limit as required.
    """
    base_candidate = make_url_from_title(title, max_bytes)
    if base_candidate not in used:
        used.add(base_candidate)
        return base_candidate
    base = base_candidate[:-5] if base_candidate.endswith(".html") else base_candidate
    i = 2
    while True:
        candidate = f"{base}-{i}.html"
        if candidate not in used:
            used.add(candidate)
            return candidate
        i += 1


def write_markdown_file(dir_path: str, filename: str, title: str, description: str, summary: str, url_str: str, prev_url: str | None, md_body: str, categories: list[str], tags: list[str], keywords: list[str], publish_date: str, lastmod: str):
    os.makedirs(dir_path, exist_ok=True)
    # Format inline YAML list for keywords
    def _inline_list(vals: list[str]) -> str:
        if not vals:
            return "[]"
        safe = [f'"{v}"' for v in vals]
        return "[ " + ", ".join(safe) + " ]"
    front_matter_lines = [
        "---",
        f" publishDate: \"{publish_date}\"",
        f" lastmod: \"{lastmod}\"",
        f" title: {title}",
        f" description: {description}",
        f" summary: {summary}",
        f" url: {url_str}",
        " categories:" if categories else " categories: []",
    ]
    if categories:
        for c in categories:
            front_matter_lines.append(f"  - {c}")
    front_matter_lines += [
        " tags:" if tags else " tags: []",
    ]
    if tags:
        for t in tags:
            front_matter_lines.append(f"  - {t}")
    # Add keywords inline list (English SEO keywords)
    front_matter_lines += [
        f" keywords: {_inline_list(keywords)}",
    ]
    front_matter_lines += [
        " type: docs",
        f" prev: {prev_url if prev_url else ''}",
        " sidebar:",
        "    open: true",
        " ---",
        "",
    ]
    full_path = os.path.join(dir_path, filename)
    with open(full_path, "w", encoding="utf-8") as f:
        f.write("\n".join(front_matter_lines))
        # Ensure body separated by a blank line
        if md_body and not md_body.startswith("\n"):
            f.write(md_body if md_body.startswith("\n") else ("\n" + md_body))
    return full_path


def pull_links_from_markdown(md: str, base_url: str) -> list[str]:
    urls = []
    # Markdown link pattern [text](url)
    for m in re.finditer(r"\[[^\]]+\]\(([^\)\s]+)\)", md):
        href = m.group(1)
        if href.startswith("http://") or href.startswith("https://"):
            urls.append(href)
        else:
            urls.append(urljoin(base_url, href))
    return urls


def to_posix_path(path: str) -> str:
    """Convert file path to POSIX style (forward slashes)."""
    return path.replace("\\", "/")

def load_manifest(manifest_path: str) -> dict:
    """Load manifest.json if it exists, otherwise return empty dict."""
    try:
        if os.path.exists(manifest_path):
            with open(manifest_path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logging.warning(f"Failed to load manifest at {manifest_path}: {e}")
    return {}


def convert_images_to_reference(md_text: str) -> str:
    """Convert inline image syntax to reference-style images and append references.
    Example: ![alt](url "title") -> ![alt][img-1] with a footnote [img-1]: url "title"
    """
    refs = []
    counter = 1

    def repl(m):
        nonlocal counter
        alt = m.group(1) or ""
        url = m.group(2)
        title = m.group(3)
        ref_id = f"img-{counter}"
        counter += 1
        refs.append((ref_id, url, title))
        return f"![{alt}][{ref_id}]"

    # Only transform inline pattern; keep existing reference-style untouched
    new_md = re.sub(r"!\[([^\]]*)\]\(([^)\s]+)(?:\s+\"([^\"]*)\")?\)", repl, md_text)
    if refs:
        new_md = new_md.rstrip() + "\n\n"
        for ref_id, url, title in refs:
            if title:
                new_md += f"[{ref_id}]: {url} \"{title}\"\n"
            else:
                new_md += f"[{ref_id}]: {url}\n"
    return new_md


def html_to_markdown(html: str) -> str:
    """Convert HTML to Markdown while preserving links and using reference-style images.
    Tries `html2text`, then `markdownify`, and finally a robust fallback.
    """
    if not html:
        return ""
    # Try html2text
    try:
        h = html2text.HTML2Text()
        h.ignore_links = False  # always keep links
        h.ignore_images = False
        h.inline_links = True   # keep original link inline so URL is visible in context
        h.body_width = 0
        md_text = h.handle(html)
        return convert_images_to_reference(md_text)
    except Exception:
        pass
    # Try markdownify
    try:
        md_text = md(html, heading_style="ATX")
        return convert_images_to_reference(md_text)
    except Exception:
        pass
    # Fallback: transform <a> and <img> first, then strip other tags, preserving references.
    img_refs = []
    img_counter = 1

    def img_repl(m):
        nonlocal img_counter
        src = m.group(1)
        alt = m.group(2) or ""
        title = m.group(3)
        ref_id = f"img-{img_counter}"
        img_counter += 1
        img_refs.append((ref_id, src, title))
        return f"![{alt}][{ref_id}]"

    def a_repl(m):
        href = m.group(1)
        text = m.group(2).strip()
        text = text if text else href
        return f"[{text}]({href})"

    working = html
    # Remove script/style content early
    working = re.sub(r"(?is)<(script|style)[^>]*>.*?</\\1>", "", working)
    # Replace images with reference-style placeholders
    working = re.sub(r"(?is)<img[^>]*src=\"([^\"]+)\"[^>]*(?:alt=\"([^\"]*)\")?[^>]*(?:title=\"([^\"]*)\")?[^>]*>", img_repl, working)
    # Replace anchors with inline markdown links
    working = re.sub(r"(?is)<a[^>]*href=\"([^\"]+)\"[^>]*>(.*?)</a>", a_repl, working)
    # Basic line breaks
    working = re.sub(r"(?is)<br\\s*/?>", "\n", working)
    working = re.sub(r"(?is)</p\\s*>", "\n\n", working)
    # strip remaining tags
    working = re.sub(r"(?is)<[^>]+>", "", working)
    md_text = html_module.unescape(working).strip()
    if img_refs:
        md_text = md_text.rstrip() + "\n\n"
        for ref_id, src, title in img_refs:
            if title:
                md_text += f"[{ref_id}]: {src} \"{title}\"\n"
            else:
                md_text += f"[{ref_id}]: {src}\n"
    return md_text

def translate_to_english_with_ollama(ollama_base: str, model: str, text: str, wait: float = 0.0) -> str:
    """Translate Markdown text to English using a local Ollama model.
    Preserves Markdown structure, link URLs, and reference-style image identifiers.
    Returns translated text, or original on failure.
    """
    if not text or not text.strip():
        return text
    # 可选等待，避免频繁调用或模型加载抖动
    if wait and wait > 0:
        try:
            time.sleep(wait)
        except Exception:
            pass
    # 优先使用官方 Ollama Python 库
    try:
        client = Client(host=ollama_base)
        resp = client.generate(
            model=model,
            prompt=(
                "You are a professional translator. Translate the following Markdown to English only. "
                "Preserve Markdown formatting, keep link URLs unchanged, and DO NOT alter reference identifiers like [img-1]. "
                "Do not include any Chinese characters in the output. Output ONLY the translated English Markdown without extra commentary.\n\n"
                + text
            ),
            stream=False,
            options={"temperature": 0.25, "num_ctx": 512},
        )
        translated = resp.get("response") if isinstance(resp, dict) else None
        if isinstance(translated, str) and translated.strip():
            translated = re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", translated)
            return translated
    except ImportError:
        # 回退到 HTTP API
        pass
    except Exception as e:
        logging.error(f"Ollama translation (client) failed: {e}")
    endpoint = ollama_base.rstrip("/") + "/api/generate"
    prompt = (
        "You are a professional translator. Translate the following Markdown to English only. "
        "Preserve Markdown formatting, keep link URLs unchanged, and DO NOT alter reference identifiers like [img-1]. "
        "Do not include any Chinese characters in the output. Output ONLY the translated English Markdown without extra commentary.\n\n"
        + text
    )
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "max_tokens": 70,
        "temperature": 0.25,
        "num_ctx": 512,
    }
    try:
        resp = requests.post(endpoint, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        translated = data.get("response")
        if isinstance(translated, str) and translated.strip():
            # Enforce English-only by stripping CJK characters if any remain
            translated = re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", translated)
            return translated
    except requests.RequestException as e:
        logging.error(f"Ollama translation failed: {e}")
    # Fallback: return original text if translation fails
    return text


def extract_categories_and_tags_with_ollama(ollama_base: str, model: str, title: str, body: str, wait: float = 0.0) -> tuple[list[str], list[str]]:
    """Use local Ollama to extract English categories and tags from title/body.
    Expects the model to return a JSON object: {"categories": [...], "tags": [...]}.
    Returns (categories, tags). Falls back to simple heuristics on failure.
    """
    categories: list[str] = []
    tags: list[str] = []
    if not (title or body):
        return categories, tags

    # 可选等待
    if wait and wait > 0:
        try:
            time.sleep(wait)
        except Exception:
            pass
    # 优先使用官方 Ollama Python 库
    try:
        client = Client(host=ollama_base)
        resp = client.generate(
            model=model,
            prompt=(
                "You are a taxonomy assistant. Based on the following English Markdown title and body, "
                "derive 1-3 broad categories and 4-8 concise tags. "
                "Respond ONLY with a compact JSON object using keys 'categories' and 'tags'. "
                "Ensure all outputs are English and contain no Chinese characters.\n\n"
                f"Title: {title}\n\n"
                f"Body:\n{body}\n"
            ),
            stream=False,
            options={"temperature": 0.25, "num_ctx": 512},
        )
        raw = resp.get("response") if isinstance(resp, dict) else None
        if isinstance(raw, str):
            m = re.search(r"\{[\s\S]*\}", raw)
            text = m.group(0) if m else raw
            try:
                obj = json.loads(text)
                cats = obj.get("categories")
                tgs = obj.get("tags")
                categories: list[str] = []
                tags: list[str] = []
                if isinstance(cats, list):
                    categories = [str(x).strip() for x in cats if str(x).strip()]
                if isinstance(tgs, list):
                    tags = [str(x).strip() for x in tgs if str(x).strip()]
                # Enforce English-only by stripping CJK characters
                categories = [re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", c) for c in categories]
                tags = [re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", t) for t in tags]
                return categories, tags
            except Exception:
                pass
    except ImportError:
        pass
    except Exception as e:
        logging.error(f"Ollama taxonomy extraction (client) failed: {e}")
    endpoint = ollama_base.rstrip("/") + "/api/generate"
    prompt = (
        "You are a taxonomy assistant. Based on the following English Markdown title and body, "
        "derive 1-3 broad categories and 4-8 concise tags. "
        "Respond ONLY with a compact JSON object using keys 'categories' and 'tags'. "
        "Ensure all outputs are English and contain no Chinese characters.\n\n"
        f"Title: {title}\n\n"
        f"Body:\n{body}\n"
    )
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "max_tokens": 70,
        "temperature": 0.25,
        "num_ctx": 512,
    }
    try:
        resp = requests.post(endpoint, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        raw = data.get("response")
        if isinstance(raw, str):
            # Try to locate a JSON object in the response
            m = re.search(r"\{[\s\S]*\}", raw)
            text = m.group(0) if m else raw
            try:
                obj = json.loads(text)
                cats = obj.get("categories")
                tgs = obj.get("tags")
                if isinstance(cats, list):
                    categories = [str(x).strip() for x in cats if str(x).strip()]
                if isinstance(tgs, list):
                    tags = [str(x).strip() for x in tgs if str(x).strip()]
            except Exception:
                pass
    except requests.RequestException as e:
        logging.error(f"Ollama taxonomy extraction failed: {e}")

    # Basic fallback if extraction failed
    if not categories:
        # Use first word of title or 'general'
        head = (title or "general").strip().split()
        first = head[0].lower() if head else "general"
        categories = [first.capitalize()]
    if not tags:
        # Generate tags from title words (filtered)
        words = re.findall(r"[a-zA-Z0-9][a-zA-Z0-9\-_.]{1,30}", title.lower())
        tags = list(dict.fromkeys(words))[:5]

    # Enforce English-only by stripping CJK characters
    categories = [re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", c) for c in categories]
    tags = [re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", t) for t in tags]


def extract_keywords_with_ollama(ollama_base: str, model: str, title: str, body: str, max_keywords: int = 70, wait: float = 0.0) -> list[str]:
    """Use local Ollama to extract up to max_keywords English SEO keywords.
    Returns a list of unique, cleaned English keywords.
    """
    if max_keywords <= 0:
        max_keywords = 70
    # 可选等待
    if wait and wait > 0:
        try:
            time.sleep(wait)
        except Exception:
            pass
    # 优先使用官方 Ollama Python 库
    try:
        client = Client(host=ollama_base)
        resp = client.generate(
            model=model,
            prompt=(
                "You are an SEO assistant. From the following English Markdown title and body, "
                f"extract up to {max_keywords} concise English keywords for SEO. "
                "Return ONLY a compact JSON array of strings, no explanations. "
                "Keywords must be English words or phrases, deduplicated, no punctuation except hyphen. "
                "Do not include Chinese characters.\n\n"
                f"Title: {title}\n\n"
                f"Body:\n{body}\n"
            ),
            stream=False,
            options={"temperature": 0.25, "num_ctx": 512},
        )
        raw = resp.get("response") if isinstance(resp, dict) else None
        keywords: list[str] = []
        if isinstance(raw, str):
            m = re.search(r"\[[\s\S]*\]", raw)
            text = m.group(0) if m else raw
            try:
                arr = json.loads(text)
                if isinstance(arr, list):
                    keywords = [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                pass
        if keywords:
            cleaned: list[str] = []
            for kw in keywords:
                s = re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", kw)
                s = re.sub(r"[^A-Za-z0-9\-\s]", " ", s)
                s = re.sub(r"\s+", " ", s).strip()
                if s:
                    cleaned.append(s.lower())
            final: list[str] = []
            seen2 = set()
            for s in cleaned:
                if s not in seen2:
                    seen2.add(s)
                    final.append(s)
                if len(final) >= max_keywords:
                    break
            return final
    except ImportError:
        pass
    except Exception as e:
        logging.error(f"Ollama keyword extraction (client) failed: {e}")
    endpoint = ollama_base.rstrip("/") + "/api/generate"
    prompt = (
        "You are an SEO assistant. From the following English Markdown title and body, "
        f"extract up to {max_keywords} concise English keywords for SEO. "
        "Return ONLY a compact JSON array of strings, no explanations. "
        "Keywords must be English words or phrases, deduplicated, no punctuation except hyphen. "
        "Do not include Chinese characters.\n\n"
        f"Title: {title}\n\n"
        f"Body:\n{body}\n"
    )
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "max_tokens": 70,
        "temperature": 0.25,
        "num_ctx": 512,
    }
    keywords: list[str] = []
    try:
        resp = requests.post(endpoint, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        raw = data.get("response")
        if isinstance(raw, str):
            # Try to locate a JSON array in the response
            m = re.search(r"\[[\s\S]*\]", raw)
            text = m.group(0) if m else raw
            try:
                arr = json.loads(text)
                if isinstance(arr, list):
                    keywords = [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                pass
    except requests.RequestException as e:
        logging.error(f"Ollama keyword extraction failed: {e}")

    # Fallback: derive keywords from title and body
    if not keywords:
        base_text = (title or "") + "\n" + (body or "")
        # Extract candidate words/phrases
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9\-]{1,60}", base_text)
        # Deduplicate preserving order
        seen = set()
        deduped = []
        for t in tokens:
            k = t.lower()
            if k not in seen:
                seen.add(k)
                deduped.append(t)
        keywords = deduped[:max_keywords]

    # Clean: strip CJK, keep English letters/numbers/hyphens and spaces, lowercased
    cleaned: list[str] = []
    for kw in keywords:
        s = re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", kw)
        s = re.sub(r"[^A-Za-z0-9\-\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        if s:
            cleaned.append(s.lower())

    # Final dedupe and cap
    final: list[str] = []
    seen2 = set()
    for s in cleaned:
        if s not in seen2:
            seen2.add(s)
            final.append(s)
        if len(final) >= max_keywords:
            break

    return final
    return categories, tags


def normalize_category(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9\-_.\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Title Case
    return " ".join(w.capitalize() for w in s.split(" ")) if s else ""


def normalize_tag(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9\-_.]", "", s)
    s = re.sub(r"-{2,}", "-", s)
    return s


def _closest_option_local(term: str, options: list[str]) -> str:
    if not options:
        return term
    best = options[0]
    best_score = -1.0
    for opt in options:
        score = SequenceMatcher(None, term.lower(), opt.lower()).ratio()
        if score > best_score:
            best_score = score
            best = opt
    return best


def choose_closest_with_ollama(ollama_base: str, model: str, term: str, options: list[str], label: str, wait: float = 0.0) -> str:
    """Ask Ollama to choose the closest option for the term among options.
    Falls back to local similarity if the call fails.
    """
    if not options:
        return term
    # 可选等待
    if wait and wait > 0:
        try:
            time.sleep(wait)
        except Exception:
            pass
    # 优先使用官方 Ollama Python 库
    try:
        client = Client(host=ollama_base)
        resp = client.generate(
            model=model,
            prompt=(
                f"You are a taxonomy assistant. Choose the single closest {label} from the provided options for the term.\n"
                f"Term: {term}\n"
                f"Options (one per line):\n" + "\n".join(options) + "\n\n"
                "Respond with ONLY the chosen option text, no extra words."
            ),
            stream=False,
            options={"temperature": 0.25, "num_ctx": 512},
        )
        choice = resp.get("response") if isinstance(resp, dict) else None
        if isinstance(choice, str):
            choice = choice.strip()
            for opt in options:
                if choice.lower() == opt.lower():
                    return opt
    except ImportError:
        pass
    except Exception as e:
        logging.error(f"Ollama closest-choice (client) failed: {e}")
    endpoint = ollama_base.rstrip("/") + "/api/generate"
    prompt = (
        f"You are a taxonomy assistant. Choose the single closest {label} from the provided options for the term.\n"
        f"Term: {term}\n"
        f"Options (one per line):\n" + "\n".join(options) + "\n\n"
        "Respond with ONLY the chosen option text, no extra words."
    )
    payload = {"model": model, "prompt": prompt, "stream": False, "max_tokens": 70, "temperature": 0.25, "num_ctx": 512}
    try:
        resp = requests.post(endpoint, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        choice = data.get("response")
        if isinstance(choice, str):
            choice = choice.strip()
            # If the choice matches one of the options, return it; else fallback
            for opt in options:
                if choice.lower() == opt.lower():
                    return opt
    except requests.RequestException as e:
        logging.error(f"Ollama closest-choice failed: {e}")
    return _closest_option_local(term, options)


def reconcile_terms(proposed: list[str], pool: list[str], max_size: int, ollama_base: str, ollama_model: str, for_category: bool, ollama_wait: float = 0.0) -> list[str]:
    """Reconcile proposed terms with a global pool under a size cap.
    - Normalize terms
    - If pool size < cap and term not present, add to pool
    - If at cap, map to closest existing option (via Ollama or local)
    Returns the list of selected terms (canonical form from pool).
    """
    selected: list[str] = []
    normalize = normalize_category if for_category else normalize_tag
    for term in proposed:
        norm = normalize(term)
        if not norm:
            continue
        # Case-insensitive membership check using lower()
        pool_lower = [p.lower() for p in pool]
        if norm.lower() in pool_lower:
            # Use the canonical form from pool
            idx = pool_lower.index(norm.lower())
            selected.append(pool[idx])
            continue
        if len(pool) < max_size:
            pool.append(norm)
            selected.append(norm)
        else:
            choice = choose_closest_with_ollama(ollama_base, ollama_model, norm, pool, "category" if for_category else "tag", wait=ollama_wait)
            selected.append(choice)
    return selected


def call_firecrawl_start(firecrawl_base: str, start_url: str, auth_header: str = "") -> dict:
    """同步包装：委托到异步的启动函数。"""
    try:
        return asyncio.run(call_firecrawl_start_async(firecrawl_base, start_url, auth_header))
    except RuntimeError:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(call_firecrawl_start_async(firecrawl_base, start_url, auth_header))


async def call_firecrawl_start_async(firecrawl_base: str, start_url: str, auth_header: str = "") -> dict:
    """启动 Firecrawl v2 爬取并返回官方 start 响应结构。

    返回结构体与官方 API /v2/crawl 一致：
    {"success": true, "id": "<string>", "url": "<string>"}
    不在此函数中拉取数据，数据获取由调用方使用返回的 url 调用状态接口完成。
    """
    # 读取限制与默认抓取选项
    try:
        limit = int(os.environ.get("FIRECRAWL_LIMIT", "100") or "100")
    except Exception:
        limit = 100
    # 额外控制项的默认值（支持 ENV 覆盖）
    try:
        max_concurrency = int(os.environ.get("FIRECRAWL_MAX_CONCURRENCY", "100") or "100")
    except Exception:
        max_concurrency = 100
    sitemap_strategy = os.environ.get("FIRECRAWL_SITEMAP", "include") or "include"
    try:
        max_discovery_depth = int(os.environ.get("FIRECRAWL_MAX_DISCOVERY_DEPTH", "2") or "2")
    except Exception:
        max_discovery_depth = 2
    # 是否爬取整个域名，默认 true，可用 ENV 覆盖
    try:
        ced_raw = os.environ.get("FIRECRAWL_CRAWL_ENTIRE_DOMAIN", "true")
        crawl_entire_domain = str(ced_raw).strip().lower() in ("true", "1", "yes", "on")
    except Exception:
        crawl_entire_domain = True

    scrape_options: dict = {
        "formats": ["markdown"],
        "waitFor": 1000,
    }
    extra_json = os.environ.get("FIRECRAWL_EXTRA_SCRAPE_OPTIONS")
    if extra_json:
        try:
            extra = json.loads(extra_json)
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if isinstance(v, dict) and isinstance(scrape_options.get(k), dict):
                        scrape_options[k].update(v)
                    else:
                        scrape_options[k] = v
                # 防止误将 crawlOptions 放入 scrape_options
                if "crawlOptions" in scrape_options:
                    scrape_options.pop("crawlOptions", None)
        except Exception as e:
            logging.warning(f"Failed to parse FIRECRAWL_EXTRA_SCRAPE_OPTIONS: {e}")

    # 尝试使用官方异步 SDK
    try:
        # 从 Authorization 头或环境变量解析 API Key
        api_key_env = os.environ.get("FIRECRAWL_API_KEY")
        api_key = None
        if auth_header and auth_header.strip():
            # 兼容 "Bearer <KEY>" 或直接传入 KEY
            ah = auth_header.strip()
            if ah.lower().startswith("bearer "):
                api_key = ah[7:].strip()
            else:
                api_key = ah
        if not api_key:
            api_key = api_key_env

        # 初始化异步 SDK 客户端
        firecrawl_kwargs = {}
        if api_key:
            firecrawl_kwargs["api_key"] = api_key
        if firecrawl_base:
            firecrawl_kwargs["api_url"] = firecrawl_base.rstrip("/")

        client = AsyncFirecrawl(**firecrawl_kwargs)

        # 异步启动爬取
        started = await client.start_crawl(
            url=start_url,
            limit=limit,
            scrape_options=scrape_options,
            max_concurrency=max_concurrency,
            sitemap=sitemap_strategy,
            max_discovery_depth=max_discovery_depth,
            crawl_entire_domain=crawl_entire_domain,
        )
        start_status_url = f"{firecrawl_base.rstrip('/')}/v2/crawl/{getattr(started, 'id', '')}"
        return {
            "success": True if getattr(started, "id", None) else False,
            "id": getattr(started, "id", None),
            "url": start_status_url,
        }
    except Exception as e:
        logging.warning(f"Firecrawl 异步 SDK 调用失败，回退到 HTTP API：{e}")

    # 回退：直接调用 HTTP API 的 /v2/crawl
    endpoint = firecrawl_base.rstrip("/") + "/v2/crawl"
    payload = {
        "url": start_url,
        "scrapeOptions": scrape_options,
        "limit": limit,
        "maxConcurrency": max_concurrency,
        "sitemap": sitemap_strategy,
        "maxDiscoveryDepth": max_discovery_depth,
        "crawlEntireDomain": crawl_entire_domain,
    }
    headers = {"Authorization": auth_header} if auth_header else None
    try:
        # 在异步函数中使用 httpx
        try:
            async with httpx.AsyncClient() as ac:
                resp = await ac.post(endpoint, json=payload, headers=headers, timeout=60)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logging.warning(f"Firecrawl httpx 调用失败，回退到同步请求：{e}")
            # 若无 httpx，则在线程中运行同步请求
            def _sync_post():
                resp = requests.post(endpoint, json=payload, headers=headers, timeout=60)
                resp.raise_for_status()
                return resp.json()
            data = await asyncio.to_thread(_sync_post)

        if isinstance(data, dict):
            cid = data.get("id")
            if cid and not data.get("url"):
                data["url"] = f"{firecrawl_base.rstrip('/')}/v2/crawl/{cid}"
        return data
    except Exception as e:
        logging.error(f"Firecrawl start failed for {start_url}: {e}")
        return {"success": False, "id": None, "url": None}


def call_firecrawl_next(next_url: str, auth_header: str = "", firecrawl_base: str | None = None) -> dict:
    """同步包装：委托到异步的状态查询，并允许指定 AsyncFirecrawl 的后端 endpoint。"""
    try:
        return asyncio.run(call_firecrawl_next_async(next_url, auth_header, firecrawl_base))
    except RuntimeError:
        # 若已有运行中的事件循环（例如在某些环境），改用 to_thread 包装同步回退
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(call_firecrawl_next_async(next_url, auth_header, firecrawl_base))


async def call_firecrawl_next_async(next_url: str, auth_header: str = "", firecrawl_base: str | None = None) -> dict:
    """异步获取下一页数据：优先使用 SDK（在线程中执行），否则使用 httpx 异步请求。

    返回结构保持为包含 data 与 next 的字典。
    """
    if not next_url:
        return {}
    # 解析 api_url 与 crawl_id
    api_key = None
    try:
        parsed = urlparse(next_url)
        parsed_base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else None
        m = re.search(r"/v2/crawl/([a-zA-Z0-9\-]+)", next_url)
        crawl_id = m.group(1) if m else None
    except Exception:
        parsed_base = None
        crawl_id = None

    # 从 Authorization 或环境变量读取 API Key
    if auth_header and auth_header.strip():
        ah = auth_header.strip()
        api_key = ah[7:].strip() if ah.lower().startswith("bearer ") else ah
    if not api_key:
        api_key = os.environ.get("FIRECRAWL_API_KEY")

    # 轮询：当 total < 1 时继续等待；仅在 (total > 1 且 completed > 1) 或 (total == 1 且 completed == 1) 时不再等待
    try:
        min_delay = float(os.environ.get("FIRECRAWL_MIN_DELAY", 3.0))
    except Exception:
        min_delay = 3.0

    headers = {"Authorization": auth_header} if auth_header else None

    while True:
        result: dict | None = None

        # 选择 AsyncFirecrawl 的后端 endpoint：优先使用传入的 firecrawl_base，其次解析自 next_url，最后使用环境变量
        effective_base = None
        if firecrawl_base:
            effective_base = firecrawl_base.rstrip("/")
        elif parsed_base:
            effective_base = parsed_base.rstrip("/")
        else:
            effective_base = (os.environ.get("FIRECRAWL_BASE_URL") or "http://localhost:3002").rstrip("/")

        # 优先 SDK（解析到 crawl_id 时）
        if crawl_id and effective_base:
            try:
                kwargs = {}
                if api_key:
                    kwargs["api_key"] = api_key
                kwargs["api_url"] = effective_base
                client = AsyncFirecrawl(**kwargs)

                status = await client.get_crawl_status(crawl_id)
                result = {
                    "data": getattr(status, "data", []) or [],
                    "next": getattr(status, "next", None),
                    "status": getattr(status, "status", None),
                    "completed": getattr(status, "completed", None),
                    "total": getattr(status, "total", None),
                }
            except Exception as e:
                logging.warning(f"Firecrawl SDK 获取下一页失败，改用异步 HTTP：{e}")

        # 异步 HTTP 回退或 SDK 未成功
        if result is None:
            try:
                try:
                    async with httpx.AsyncClient() as ac:
                        resp = await ac.get(next_url, headers=headers, timeout=60)
                        resp.raise_for_status()
                        result = resp.json()
                except ImportError:
                    logging.info("httpx 未安装，改用同步 requests 在线程中执行。请安装：pip install httpx")

                    def _sync_get_json(url: str, headers: dict | None) -> dict:
                        resp = requests.get(url, headers=headers, timeout=60)
                        resp.raise_for_status()
                        return resp.json()

                    result = await asyncio.to_thread(_sync_get_json, next_url, headers)
            except Exception as e:
                logging.error(f"Firecrawl next failed for {next_url}: {e}")
                result = {}

        # 判定是否继续等待
        total = 0
        completed = 0
        try:
            total = int((result or {}).get("total", 0) or 0)
        except Exception:
            total = 0
        try:
            completed = int((result or {}).get("completed", 0) or 0)
        except Exception:
            completed = 0

        if total < 1:
            await asyncio.sleep(min_delay)
            continue

        stop_waiting = (total > 1 and completed > 1) or (total == 1 and completed == 1)
        if stop_waiting:
            return result or {}

        # 仍未达到停止条件，继续等待
        await asyncio.sleep(min_delay)

def get_md_and_links_from_firecrawl_result(result: dict) -> tuple[list[dict], str | None]:
    """Parse Firecrawl v2 crawl batch response.
    Returns (items, next_url). Each item includes {url, title, body}.
    """
    items: list[dict] = []
    next_url: str | None = None

    try:
        next_field = result.get("next")
        if isinstance(next_field, str) and next_field.strip():
            next_url = next_field.strip()
    except Exception:
        next_url = None

    data = result.get("data")
    logging.info(f"Firecrawl result: {result}")
    if isinstance(data, list):
        for entry in data:
            if not isinstance(entry, dict):
                continue
            md = entry.get("markdown") or entry.get("md") or entry.get("content")
            html = entry.get("html")
            body = md if isinstance(md, str) and md.strip() else (html_to_markdown(html) if isinstance(html, str) else "")
            meta = entry.get("metadata", {}) if isinstance(entry.get("metadata"), dict) else {}
            title = meta.get("title") or ""
            # description originates from metadata.metadata field (could be str/dict/list)
            raw_desc = meta.get("metadata")
            if isinstance(raw_desc, str):
                description = raw_desc
            elif isinstance(raw_desc, (dict, list)):
                try:
                    description = json.dumps(raw_desc, ensure_ascii=False, separators=(",", ":"))
                except Exception:
                    description = str(raw_desc)
            elif raw_desc is not None:
                description = str(raw_desc)
            else:
                description = ""
            source_url = meta.get("sourceURL") or meta.get("url") or ""
            if not source_url:
                continue
            items.append({"url": source_url, "title": title, "description": description, "body": body})

    return items, next_url


def main():
    setup_logger()
    # Load .env before parsing arguments so defaults can be sourced from it
    # Priority: --env-file > ENV_FILE > .env
    pre_env_path = find_cli_arg(sys.argv, "--env-file") or os.environ.get("ENV_FILE") or ".env"
    load_env_file(pre_env_path)
    parser = argparse.ArgumentParser(description="Crawl site via local Firecrawl v2 and write Markdown results")
    parser.add_argument("--start-url", required=True, help="Root URL to start crawling")
    parser.add_argument("--firecrawl-base", default=os.environ.get("FIRECRAWL_BASE_URL", "http://localhost:3002"), help="Base URL of local Firecrawl service")
    parser.add_argument("--max-pages", type=int, default=0, help="Limit number of pages (0 means unlimited)")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between requests in seconds")
    parser.add_argument("--min-delay", type=float, default=float(os.environ.get("FIRECRAWL_MIN_DELAY", 3.0)), help="轮询状态的最小等待秒数（至少等待该值）")
    parser.add_argument("--ollama-base", default=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434"), help="Base URL of local Ollama service")
    parser.add_argument("--ollama-model", default=os.environ.get("OLLAMA_MODEL", "qwen3:4b"), help="Ollama model name for translation (e.g., qwen:3b)")
    parser.add_argument("--ollama-wait", type=float, default=float(os.environ.get("OLLAMA_WAIT", 10.0)), help="Ollama 调用前等待秒数（默认 10 秒）")
    parser.add_argument("--output-dir", default=os.environ.get("OUTPUT_DIR", "results"), help="Destination directory to save generated Markdown files")
    parser.add_argument("--firecrawl-token", default=os.environ.get("FIRECRAWL_TOKEN", ""), help="Firecrawl 访问令牌（仅输入 token，程序会自动拼接 'Bearer '）")
    parser.add_argument("--firecrawl-auth", default=os.environ.get("FIRECRAWL_AUTH", ""), help="兼容参数：若未以 'Bearer ' 开头，将自动拼接")
    parser.add_argument("--env-file", default=pre_env_path, help="启动前加载的 .env 文件路径（支持 --env-file=path 或 ENV_FILE）")
    args = parser.parse_args()

    start_url = args.start_url
    firecrawl_base = args.firecrawl_base
    max_pages = args.max_pages
    delay = args.delay
    min_delay = args.min_delay
    ollama_base = args.ollama_base
    ollama_model = args.ollama_model
    ollama_wait = args.ollama_wait
    output_dir = args.output_dir
    firecrawl_token = args.firecrawl_token
    firecrawl_auth = args.firecrawl_auth
    # 构建 Authorization 头值：优先使用 --firecrawl-token / FIRECRAWL_TOKEN
    if firecrawl_token:
        auth_header = f"Bearer {firecrawl_token}"
    elif firecrawl_auth:
        auth_header = firecrawl_auth if firecrawl_auth.lower().startswith("bearer ") else f"Bearer {firecrawl_auth}"
    else:
        auth_header = ""

    written_files: list[str] = []
    used_urls: set[str] = set()
    # Global pools for categories and tags with caps
    global_categories_pool: list[str] = []
    global_tags_pool: list[str] = []
    last_prev_url: str | None = None
    pages_processed = 0

    # Attempt to resume from existing manifest
    manifest_path = os.path.join(output_dir, "manifest.json")
    manifest = load_manifest(manifest_path)
    latest_next_url = manifest.get("latest_next_url")
    if manifest:
        # Restore prior state if available
        pages_processed = int(manifest.get("pages_processed", pages_processed) or 0)
        used_urls = set(manifest.get("used_urls", []))
        global_categories_pool = manifest.get("global_categories_pool", []) or []
        global_tags_pool = manifest.get("global_tags_pool", []) or []
        last_prev_url = manifest.get("last_prev_url") or None
        prev_files = manifest.get("files", []) or []
        for rel in prev_files:
            try:
                abs_p = os.path.join(output_dir, rel)
            except Exception:
                abs_p = rel
            written_files.append(abs_p)
        if latest_next_url:
            logging.info("Resuming crawl from manifest next_url")
            result = call_firecrawl_next(latest_next_url, auth_header, firecrawl_base)
        else:
            logging.info(f"No next_url in manifest; starting fresh at {start_url}")
            start_info = call_firecrawl_start(firecrawl_base, start_url, auth_header)
            start_url_status = start_info.get("url") if isinstance(start_info, dict) else None
            result = call_firecrawl_next(start_url_status, auth_header, firecrawl_base) if start_url_status else {}
    else:
        logging.info(f"Starting Firecrawl v2 crawl at {start_url} via {firecrawl_base}")
        start_info = call_firecrawl_start(firecrawl_base, start_url, auth_header)
        start_url_status = start_info.get("url") if isinstance(start_info, dict) else None
        result = call_firecrawl_next(start_url_status, auth_header, firecrawl_base) if start_url_status else {}
        logging.info(f"Firecrawl result: {result}")
    while True:
        items, next_url = get_md_and_links_from_firecrawl_result(result)

        for item in items:
            if max_pages and pages_processed >= max_pages:
                logging.info(f"Reached max pages limit: {max_pages}")
                break

            source = item.get("url", "")
            title = item.get("title") or (urlparse(source).path.rstrip("/").split("/")[-1] or urlparse(source).hostname or "").replace("-", " ")
            description_raw = item.get("description", "")
            body = item.get("body", "")

            # Translate title and body to English using local Ollama
            title_en = translate_to_english_with_ollama(ollama_base, ollama_model, title, wait=ollama_wait) if title else title
            description_en = translate_to_english_with_ollama(ollama_base, ollama_model, description_raw, wait=ollama_wait) if description_raw else ""
            summary_en = description_en
            body_en = translate_to_english_with_ollama(ollama_base, ollama_model, body, wait=ollama_wait) if body else body
            categories, tags = extract_categories_and_tags_with_ollama(ollama_base, ollama_model, title_en or "", body_en or "", wait=ollama_wait)
            # Reconcile with global pools under caps (70 categories, 300 tags)
            categories_final = reconcile_terms(categories, global_categories_pool, 70, ollama_base, ollama_model, True, ollama_wait=ollama_wait)
            tags_final = reconcile_terms(tags, global_tags_pool, 300, ollama_base, ollama_model, False, ollama_wait=ollama_wait)
            # Extract up to 70 English SEO keywords
            keywords = extract_keywords_with_ollama(ollama_base, ollama_model, title_en or "", body_en or "", 70, wait=ollama_wait)

            dir_path, filename = path_to_file_parts(source, output_dir)
            url_field = make_unique_url_from_title(title_en, used_urls, 30)
            prev_url = last_prev_url if last_prev_url else None

            # Compute publishDate and lastmod based on scheduling rules
            publish_date_str = scheduled_timestamp_for_index(pages_processed)
            lastmod_str = publish_date_str

            full_path = write_markdown_file(dir_path, filename, title_en, description_en, summary_en, url_field, prev_url, body_en, categories_final, tags_final, keywords, publish_date_str, lastmod_str)
            written_files.append(full_path)
            last_prev_url = url_field
            pages_processed += 1

            # Persist manifest after each page to support resume on interruption
            posix_files: list[str] = []
            for p in written_files:
                try:
                    rel_p = os.path.relpath(p, output_dir)
                except Exception:
                    rel_p = p
                posix_files.append(to_posix_path(rel_p))
            manifest = {
                "start_url": start_url,
                "firecrawl_base": firecrawl_base,
                "ollama_base": ollama_base,
                "ollama_model": ollama_model,
                "pages_processed": pages_processed,
                "files": posix_files,
                "used_urls": sorted(list(used_urls)),
                "last_prev_url": last_prev_url,
                "global_categories_pool": global_categories_pool,
                "global_tags_pool": global_tags_pool,
                "latest_next_url": next_url,
            }
            os.makedirs(output_dir, exist_ok=True)
            try:
                with open(os.path.join(output_dir, "manifest.json"), "w", encoding="utf-8") as mf:
                    json.dump(manifest, mf, ensure_ascii=False, indent=2)
            except Exception as e:
                logging.warning(f"Failed to persist manifest: {e}")

        if max_pages and pages_processed >= max_pages:
            break

        if next_url:
             logging.info(f"Fetching next batch: {next_url}")
             effective_delay = max(delay, min_delay)
             time.sleep(effective_delay)
             result = call_firecrawl_next(next_url, auth_header, firecrawl_base)
             if not result:
                 logging.warning("No result returned for next batch; stopping.")
                 break
             continue
        else:
            logging.info("No next batch; crawl complete.")
            break

    logging.info(f"Crawl finished. Pages processed: {pages_processed}. Files written: {len(written_files)}")
    # Summary manifest
    # Normalize file paths to POSIX style, prefer paths relative to output_dir
    posix_files: list[str] = []
    for p in written_files:
        try:
            rel_p = os.path.relpath(p, output_dir)
        except Exception:
            rel_p = p
        posix_files.append(to_posix_path(rel_p))

    manifest = {
        "start_url": start_url,
        "firecrawl_base": firecrawl_base,
        "ollama_base": ollama_base,
        "ollama_model": ollama_model,
        "pages_processed": pages_processed,
        "files": posix_files,
        "used_urls": sorted(list(used_urls)),
        "last_prev_url": last_prev_url,
        "global_categories_pool": global_categories_pool,
        "global_tags_pool": global_tags_pool,
        "latest_next_url": next_url,
    }
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.")
        sys.exit(130)
