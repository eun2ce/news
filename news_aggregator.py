#!/usr/bin/env python3
import os
import re
import sys
import hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import feedparser
import yaml
from dateutil import parser as dtparser

import pandas as pd
import orjson

# (옵션) 본문 추출
ENABLE_FULLTEXT = os.environ.get("FETCH_FULLTEXT", "false").lower() == "true"
if ENABLE_FULLTEXT:
    import trafilatura

KST = timezone(timedelta(hours=9))

# ---------- 유틸 ----------
def now_kst() -> datetime:
    return datetime.now(KST)

def parse_time(entry: dict) -> datetime:
    for key in ("published", "updated", "pubDate"):
        val = entry.get(key)
        if val:
            try:
                return dtparser.parse(val).astimezone(KST)
            except Exception:
                pass
    for key in ("published_parsed", "updated_parsed"):
        val = entry.get(key)
        if val:
            try:
                return datetime(*val[:6], tzinfo=timezone.utc).astimezone(KST)
            except Exception:
                pass
    return now_kst()

def clean_text(html_or_text: str) -> str:
    if not html_or_text:
        return ""
    # 간단 태그 제거
    text = re.sub(r"<.*?>", "", html_or_text)
    # 공백 정리
    return re.sub(r"\s+", " ", text).strip()

def sha1(*parts: str) -> str:
    h = hashlib.sha1()
    h.update("|".join(parts).encode("utf-8"))
    return h.hexdigest()

# ---------- 라벨링 ----------
POLITICS_KW = ["정치", "대통령", "국회", "총선", "장관", "여당", "야당", "청와대", "외교", "안보", "북한"]
ECONOMY_KW  = ["경제", "증시", "코스피", "환율", "금리", "물가", "수출", "무역", "부동산", "채권", "원달러"]
SOCIETY_KW  = ["사회", "사건", "사고", "치안", "노동", "교육", "복지", "보건", "의료", "법원", "검찰"]
CULTURE_KW  = ["문화", "영화", "음악", "공연", "전시", "문학", "예술", "드라마", "방송", "연예"]
ITSCI_KW    = ["IT", "과학", "AI", "인공지능", "반도체", "스타트업", "클라우드", "보안", "게임", "통신", "소프트웨어"]
SPORTS_KW   = ["스포츠", "축구", "야구", "농구", "배구", "골프", "올림픽", "대표팀", "K리그", "KBO", "NBA"]

def guess_category(title: str, summary: str, src_category: Optional[str]) -> str:
    if src_category:
        return src_category
    text = f"{title} {summary}"
    def hit(words): return any(w in text for w in words)
    if hit(POLITICS_KW): return "politics"
    if hit(ECONOMY_KW):  return "economy"
    if hit(SOCIETY_KW):  return "society"
    if hit(CULTURE_KW):  return "culture"
    if hit(ITSCI_KW):    return "it_science"
    if hit(SPORTS_KW):   return "sports"
    return "general"

def make_tags(title: str, summary: str) -> List[str]:
    tags = set()
    t = title + " " + summary
    if any(w in t for w in ["속보", "단독", "긴급"]): tags.add("breaking")
    if any(w in t for w in ["해설", "분석", "심층"]): tags.add("analysis")
    if any(w in t for w in ["사설", "칼럼", "오피니언"]): tags.add("opinion")
    # 간단 길이 기반 태그
    if len(title) <= 25: tags.add("short_title")
    return sorted(tags)

# ---------- 데이터 수집 ----------
def load_sources(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    out = []
    for s in data.get("sources", []):
        out.append({
            "name": s.get("name"),
            "url": s.get("url"),
            "category": s.get("category")  # optional
        })
    return out

def fetch_rss(src: Dict[str, Any]) -> List[Dict[str, Any]]:
    feed = feedparser.parse(src["url"])
    items = []
    for e in feed.entries:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        summary = clean_text(e.get("summary") or e.get("description") or "")
        published_at = parse_time(e)
        item = {
            "id": sha1(title, link)[:16],
            "source_name": src["name"],
            "source_url": src["url"],
            "url": link,
            "title": title,
            "summary": summary,
            "published_at": published_at.isoformat(),
            "fetched_at": now_kst().isoformat(),
            "language": "ko",  # 한국 소스 기준
        }
        # 라벨
        item["category"] = guess_category(title, summary, src.get("category"))
        item["tags"] = make_tags(title, summary)
        items.append(item)
    return items

def fetch_fulltext(url: str) -> Optional[str]:
    if not ENABLE_FULLTEXT:
        return None
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return None
        text = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
        return (text or "").strip() or None
    except Exception:
        return None

def enrich_fulltext(items: List[Dict[str, Any]]) -> None:
    # 주의: 전체 본문 크롤은 트래픽/속도 이슈 → 상위 N개만 or 타임아웃 고려
    limit = int(os.environ.get("FULLTEXT_LIMIT", "40"))
    for i, it in enumerate(items):
        if i >= limit:
            break
        if it.get("url"):
            body = fetch_fulltext(it["url"])
            if body:
                it["content"] = body

# ---------- 저장 ----------
def ensure_dirs(base: str) -> Dict[str, str]:
    today = now_kst().strftime("%Y/%m/%d")
    dir_day = os.path.join(base, today)
    os.makedirs(dir_day, exist_ok=True)
    jsonl_path = os.path.join(dir_day, "news.jsonl")
    parquet_path = os.path.join(dir_day, "news.parquet")
    return {"jsonl": jsonl_path, "parquet": parquet_path}

def to_frame(items: List[Dict[str, Any]]) -> pd.DataFrame:
    # 스키마 고정(모델 학습에 안정적)
    cols = [
        "id", "source_name", "source_url", "url",
        "title", "summary", "content",
        "category", "tags", "language",
        "published_at", "fetched_at"
    ]
    # content/tags 없을 수도 있음
    for it in items:
        it.setdefault("content", None)
        it.setdefault("tags", [])
    df = pd.DataFrame(items, columns=cols)
    # 중복 제거(id)
    df = df.drop_duplicates(subset=["id"])
    return df

def save_jsonl(df: pd.DataFrame, path: str) -> None:
    with open(path, "wb") as f:
        for _, row in df.iterrows():
            obj = {k: (v.tolist() if hasattr(v, "tolist") else v) for k, v in row.to_dict().items()}
            f.write(orjson.dumps(obj, option=orjson.OPT_APPEND_NEWLINE))

def save_parquet(df: pd.DataFrame, path: str) -> None:
    df.to_parquet(path, index=False)

# ---------- 메인 ----------
def main():
    output_root = os.environ.get("OUTPUT_ROOT", "dataset")
    sources_file = os.environ.get("SOURCES_FILE", "sources.yaml")
    per_source_limit = int(os.environ.get("PER_SOURCE_LIMIT", "20"))
    total_limit = int(os.environ.get("TOTAL_LIMIT", "200"))

    sources = load_sources(sources_file)
    if not sources:
        print("No sources in sources.yaml", file=sys.stderr)
        sys.exit(1)

    all_items: List[Dict[str, Any]] = []
    for src in sources:
        try:
            items = fetch_rss(src)
            # 최신순 정렬 후 소스별 상위 N
            items.sort(key=lambda x: x["published_at"], reverse=True)
            all_items.extend(items[:per_source_limit])
        except Exception as e:
            print(f"[WARN] {src['name']} fetch failed: {e}", file=sys.stderr)

    # 전체 최신순 + 중복 제거
    uniq: Dict[str, Dict[str, Any]] = {}
    for it in sorted(all_items, key=lambda x: x["published_at"], reverse=True):
        uniq.setdefault(it["id"], it)
    items = list(uniq.values())[:total_limit]

    # (옵션) 본문 추출
    if ENABLE_FULLTEXT:
        enrich_fulltext(items)

    # 저장
    paths = ensure_dirs(output_root)
    df = to_frame(items)
    save_jsonl(df, paths["jsonl"])
    save_parquet(df, paths["parquet"])

    print(f"Saved JSONL: {paths['jsonl']}")
    print(f"Saved Parquet: {paths['parquet']}")

if __name__ == "__main__":
    main()

