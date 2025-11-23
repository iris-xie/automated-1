import re
import json
from typing import Any, Dict


def _parse_ollama_text(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except Exception:
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


def parse_ollama_response(raw_text: str, max_keywords: int = 20, max_desc_len: int = 160) -> Dict[str, Any]:
    data = _parse_ollama_text(raw_text)
    if not isinstance(data, dict):
        data = {}
    out = (data.get("response") or data.get("message") or data.get("output") or raw_text or "").strip()
    out = _strip_code_fences(out)
    m = re.search(r"\{[\s\S]*\}", out)
    if not m:
        raw_clean = _strip_code_fences(raw_text)
        m = re.search(r"\{[\s\S]*\}", raw_clean)
    s = m.group(0) if m else out
    j = json.loads(s)
    title = str(j.get("title", "")).strip()
    desc = re.sub(r"[\r\n]+", " ", str(j.get("description", "")).strip()).strip()
    if len(desc) > max_desc_len:
        desc = desc[:max_desc_len]
    kws_raw = j.get("keywords", [])
    if isinstance(kws_raw, str):
        kws = [t.strip() for t in kws_raw.split(",") if t.strip()]
    else:
        kws = [str(x).strip() for x in kws_raw if str(x).strip()]
    if len(kws) > max_keywords:
        kws = kws[:max_keywords]
    cats = [str(x).strip() for x in j.get("categories", []) if str(x).strip()]
    tags = [str(x).strip() for x in j.get("tags", []) if str(x).strip()]
    cats = [re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", c) for c in cats]
    tags = [re.sub(r"[\u3400-\u4DBF\u4E00-\u9FFF]", "", t) for t in tags]
    return {"title": title, "description": desc, "keywords": kws, "categories": cats, "tags": tags}