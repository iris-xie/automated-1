import re
from typing import Any, List, Tuple

import yaml  # type: ignore

# CJK detection regex for filtering Chinese characters
CJK_REGEX = re.compile(
    r"[\u3400-\u4DBF\u4E00-\u9FFF\u3000-\u303F\uFE30-\uFE4F\uF900-\uFAFF\uFF00-\uFFEF\U00020000-\U0002A6DF\U0002A700-\U0002B81F\U0002B820-\U0002CEAF\U0002F800-\U0002FA1F]"
)


def contains_cjk(text: str) -> bool:
    try:
        return bool(CJK_REGEX.search(text or ""))
    except Exception:
        return False


def get_zh_en_translator():
    try:
        from argostranslate import translate as argos_translate  # type: ignore
        installed = argos_translate.get_installed_languages()
        zh = next((l for l in installed if getattr(l, 'code', '') == 'zh'), None)
        en = next((l for l in installed if getattr(l, 'code', '') == 'en'), None)
        if zh and en:
            tr = zh.get_translation(en)
            return getattr(tr, 'translate', None)
    except Exception:
        return None
    return None


def translate_if_cjk(s: Any, translator) -> Any:
    if not isinstance(s, str):
        return s
    if translator and contains_cjk(s):
        try:
            out = translator(s) or ""
            return re.sub(r"[\r\n]+", " ", out).strip()
        except Exception:
            return s
    return s


def translate_front_matter_fields(title: str, description: str, categories: List[str], tags: List[str], keywords: List[str], translator) -> Tuple[str, str, List[str], List[str], List[str]]:
    t = translate_if_cjk(title, translator)
    d = translate_if_cjk(description, translator)
    cats2 = [translate_if_cjk(x, translator) for x in categories]
    tags2 = [translate_if_cjk(x, translator) for x in tags]
    kws2 = [translate_if_cjk(x, translator) for x in keywords]
    return t, d, cats2, tags2, kws2


def translate_body_cjk_to_en(body: str, cancelled: bool = False) -> Tuple[str, int]:
    translator = get_zh_en_translator()
    replaced = 0
    try:
        out_chars: List[str] = []
        i = 0
        n = len(body)
        while i < n:
            if cancelled:
                raise KeyboardInterrupt
            ch = body[i]
            if CJK_REGEX.match(ch):
                j = i + 1
                while j < n and CJK_REGEX.match(body[j]):
                    j += 1
                src = body[i:j]
                trans = ""
                if translator:
                    try:
                        trans = translator(src) or ""
                    except Exception:
                        trans = ""
                replaced += 1
                trans = trans.strip()
                out_chars.append(f" {trans} " if trans else " ")
                i = j
            else:
                out_chars.append(ch)
                i += 1
        out = "".join(out_chars)
        # Cleanup any residual CJK chars
        out = CJK_REGEX.sub("", out)
        return out, replaced
    except KeyboardInterrupt:
        raise
    except Exception:
        # Fallback: strip CJK
        return CJK_REGEX.sub(" ", body), replaced


def build_yaml(fm: dict) -> str:
    dumped = yaml.safe_dump(fm, allow_unicode=True, sort_keys=False)
    return "---\n" + dumped + "---\n\n"

# ---- Ollama response parsing utilities ----
import json

def _parse_ollama_text(raw: str):
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


def parse_ollama_response(raw_text: str, max_keywords: int = 20, max_desc_len: int = 160):
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
