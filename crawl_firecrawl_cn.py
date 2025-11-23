import argparse
import asyncio
import logging
import os
import re
import time
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from urllib.parse import urlparse
from firecrawl import AsyncFirecrawl,Firecrawl  # type: ignore

import requests

# 全局计数：本次运行已写入的 Markdown 文件数量
ITEM_COUNTER = 0


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

def call_firecrawl_start_async(
    start_url: str,
    firecrawl_base: str,
    auth_header: str = "",
    limit: int = 50,
    max_concurrency: int = 10,
    scrape_options: Optional[Dict[str, Any]] = None,
    max_discovery_depth: int = 3,
    crawl_entire_domain: bool = False,
) -> Dict[str, Any]:
    scrape_options = scrape_options or {"formats": ["markdown", "html"]}
    if not start_url or not isinstance(start_url, str) or not start_url.strip():
        logging.error("未提供起始 URL")
        return {"success": False, "id": None, "url": None}
    api_key_env = os.environ.get("FIRECRAWL_API_KEY") or os.environ.get("FIRECRAWL_TOKEN") or ""
    try:
        from firecrawl import Firecrawl  # type: ignore
    except Exception:
        Firecrawl = None  # type: ignore

    try:
        api_key = None
        if auth_header and auth_header.strip():
            ah = auth_header.strip()
            api_key = ah[7:].strip() if ah.lower().startswith("bearer ") else ah
        if not api_key:
            api_key = api_key_env

        firecrawl_kwargs: Dict[str, Any] = {}
        if api_key:
            firecrawl_kwargs["api_key"] = api_key
        if firecrawl_base:
            firecrawl_kwargs["api_url"] = firecrawl_base.rstrip("/")

        if Firecrawl is None:
            raise RuntimeError("Firecrawl SDK not available")

        client = Firecrawl(**firecrawl_kwargs)

        started = client.start_crawl(
                url=start_url,
                limit=limit,
                scrape_options=scrape_options,
                max_concurrency=max_concurrency,
                max_discovery_depth=max_discovery_depth,
                crawl_entire_domain=crawl_entire_domain
            )
        started_id = started.get("id") if isinstance(started, dict) else getattr(started, "id", None)
        start_status_url = f"{firecrawl_base.rstrip('/')}/v2/crawl/{started_id or ''}"
        return {
            "success": True if started_id else False,
            "id": started_id,
            "url": start_status_url,
        }
    except Exception as e:
        logging.warning(f"Firecrawl 异步 SDK 调用失败，回退到 HTTP API：{e}")

    # 回退：直接调用 HTTP API 的 /v2/crawl（统一使用 requests 在线程中执行）
    endpoint = firecrawl_base.rstrip("/") + "/v2/crawl"
    payload = {
        "url": start_url,
        "scrapeOptions": scrape_options,
        "limit": limit,
        "maxConcurrency": max_concurrency,
        "maxDiscoveryDepth": max_discovery_depth,
        "crawlEntireDomain": crawl_entire_domain,
    }
    headers = {"Authorization": auth_header} if auth_header else None
    try:
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=60)
        status_code = resp.status_code
        resp_text = resp.text
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            cid = data.get("id")
            if cid and not data.get("url"):
                data["url"] = f"{firecrawl_base.rstrip('/')}/v2/crawl/{cid}"
        return data
    except Exception as e:
        try:
            logging.error(f"Firecrawl start failed for {start_url}: {status_code} {resp_text}")
        except Exception:
            logging.error(f"Firecrawl start failed for {start_url}: {e}")
        return {"success": False, "id": None, "url": None}


def call_firecrawl_next_async(next_url: str, auth_header: str = "", firecrawl_base: Optional[str] = None) -> Dict[str, Any]:
    if not next_url:
        return {}

    api_key: Optional[str] = None
    try:
        parsed = urlparse(next_url)
        parsed_base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else None
        m = re.search(r"/v2/crawl/([a-zA-Z0-9\-]+)", next_url)
        crawl_id = m.group(1) if m else None
    except Exception:
        parsed_base = None
        crawl_id = None

    if auth_header and auth_header.strip():
        ah = auth_header.strip()
        api_key = ah[7:].strip() if ah.lower().startswith("bearer ") else ah
    if not api_key:
        api_key = os.environ.get("FIRECRAWL_API_KEY") or os.environ.get("FIRECRAWL_TOKEN")

    try:
        min_delay = float(os.environ.get("FIRECRAWL_MIN_DELAY", 3.0))
    except Exception:
        min_delay = 3.0

    headers = {"Authorization": auth_header} if auth_header else None

    # 选择有效的后端地址：外部传入 > 从 next_url 解析 > 环境变量/默认
    effective_base = None
    if firecrawl_base and firecrawl_base.strip():
        effective_base = firecrawl_base.rstrip("/")
    elif parsed_base:
        effective_base = parsed_base
    else:
        effective_base = os.environ.get("FIRECRAWL_BASE_URL", "http://localhost:3002")

    while True:
        result: Optional[Dict[str, Any]] = None

        # 优先 SDK（有 crawl_id 时，使用有效后端地址）
        if crawl_id:
            try:
                from firecrawl import Firecrawl  # type: ignore
                kwargs = {}
                if api_key:
                    kwargs["api_key"] = api_key
                kwargs["api_url"] = effective_base
                client = Firecrawl(**kwargs)
                sdk_status = client.get_crawl_status(crawl_id)
                logging.info(f"SDK 状态查询：{sdk_status}")
                result = {
                    "data": getattr(sdk_status, "data", None),
                    "next": getattr(sdk_status, "next", None),
                    "status": getattr(sdk_status, "status", None),
                    "completed": getattr(sdk_status, "completed", 0),
                    "total": getattr(sdk_status, "total", 0),
                }
            except Exception as e:
                logging.warning(f"SDK 状态查询失败，回退到 HTTP：{e}")

        if result is None:
            try:
                r = requests.get(next_url, headers=headers, timeout=60)
                r.raise_for_status()
                result = r.json()
            except Exception as e:
                logging.warning(f"同步请求状态查询失败：{e}")

        total = (result or {}).get("total", 0) or 0
        completed = (result or {}).get("completed", 0) or 0

        if (total == 1 and completed == 1) or (total > 1 and completed > 1):
            return result or {}

        if total < 1 or completed < 1:
            time.sleep(max(min_delay, 3.0))
            continue

        return result or {}


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def slugify(text: str) -> str:
    text = re.sub(r"[^\w\-\s]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "-", text.strip())
    return text.lower() or "index"


def choose_filename(meta: Dict[str, Any], url: str, out_dir: str) -> str:
    title = meta.get("title") or "page"
    base = slugify(title)
    candidate = os.path.join(out_dir, f"{base}.md")
    i = 2
    while os.path.exists(candidate):
        candidate = os.path.join(out_dir, f"{base}-{i}.md")
        i += 1
    return candidate


def build_cn_front_matter(meta: Dict[str, Any], dt_obj: Optional[datetime] = None) -> str:
    title = meta.get("title") or "未命名"
    description = meta.get("description") or ""
    dt = (dt_obj or datetime.now()).strftime("%Y-%m-%dT%H:%M:%S")
    fm = [
        "---",
        f"title: \"{title}\"",
        f"description: \"{description}\"",
        "draft: true",
        "type: docs",
        f"lastmod: {dt}",
        "---",
        "",
    ]
    return "\n".join(fm)


def write_cn_markdown(item: Dict[str, Any], out_dir: str) -> Optional[str]:
    content = (
        item.get("markdown")
        or (item.get("content") or {}).get("markdown")
        or item.get("html")
        or ""
    )
    meta = item.get("metadata") or {}
    url = meta.get("sourceURL") or meta.get("url") or ""
    ensure_dir(out_dir)
    path = choose_filename(meta, url, out_dir)
    # 计算 lastmod 的日期增量：计数超过 100 后，每三个递增一天
    global ITEM_COUNTER
    ITEM_COUNTER += 1
    if ITEM_COUNTER <= 100:
        offset_days = 0
    else:
        # 101-103: +1 天，104-106: +2 天，以此类推
        offset_days = ((ITEM_COUNTER - 101) // 3) + 1
    dt_obj = datetime.now() + timedelta(days=offset_days)
    fm = build_cn_front_matter(meta, dt_obj)
    body = content or ""
    with open(path, "w", encoding="utf-8") as f:
        f.write(fm)
        f.write(body)
    logging.info(f"写入中文 Markdown: {path}")
    return path


def main() -> None:
    setup_logger()
    parser = argparse.ArgumentParser(description="使用 Firecrawl 抓取并写入中文 Markdown")
    parser.add_argument("--start-url", help="起始 URL")
    parser.add_argument("--firecrawl-base", default=os.environ.get("FIRECRAWL_BASE_URL", "http://localhost:3002"))
    parser.add_argument("--auth", default=(os.environ.get("FIRECRAWL_API_TOKEN") or os.environ.get("FIRECRAWL_TOKEN", "")), help="Authorization 头或 API Key")
    parser.add_argument("--output-dir", default=os.environ.get("CN_OUTPUT_DIR", "results"), help="中文 Markdown 输出目录")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--max-concurrency", type=int, default=10)
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--crawl-entire-domain", type=bool, default=True)
    args = parser.parse_args()

    start_url = args.start_url or os.environ.get("FIRECRAWL_START_URL") or os.environ.get("START_URL")
    if not start_url:
        logging.error("未提供起始 URL")
        return

    auth_header = args.auth
    if auth_header and not auth_header.lower().startswith("bearer "):
        auth_header = f"Bearer {auth_header}"

    started = call_firecrawl_start_async(
        start_url=start_url,
        firecrawl_base=args.firecrawl_base,
        auth_header=auth_header,
        limit=args.limit,
        max_concurrency=args.max_concurrency,
        max_discovery_depth=args.max_depth,
        crawl_entire_domain=args.crawl_entire_domain
    )

    if not started.get("success"):
        logging.error("启动抓取失败")
        return

    next_url = started.get("url")
    if not next_url:
        logging.error("未获取到状态查询 URL")
        return

    while True:
        status = call_firecrawl_next_async(next_url, auth_header, args.firecrawl_base)
        data = status.get("data") or []
        for item in data:
            try:
                write_cn_markdown(item, args.output_dir)
            except Exception as e:
                logging.warning(f"写入失败：{e}")

        # 继续轮询直到完成条件满足（在 call_firecrawl_next_async 内部处理）
        total = status.get("total", 0)
        completed = status.get("completed", 0)
        if total > 1 and completed == total:
            logging.info("抓取任务已达到停止条件，结束。")
            break
        time.sleep(max(float(os.environ.get("FIRECRAWL_MIN_DELAY", 3.0)), 3.0))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.")
        sys.exit(130)
