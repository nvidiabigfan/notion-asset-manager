"""
weekly_summary_automation.py  (v5 - 억 단위 소수점 2자리)
Phase 4 - 자산평가결과 → 분류별 집계 → 주간자산요약 DB 저장

[자산평가결과 DB 컬럼 구조]
  평가일자   → Title (문자열 "2026-03-07")
  자산명     → rich_text
  자산분류   → select
  수량       → number
  금액       → number
  현재가     → number
  평가액     → number
  직전평가액 → number
  변동액     → number
  변동율     → number
[주간자산요약 DB 컬럼 구조]
  기준일_분류 → Title ("2026-03-07_한국주식")
  평가일자    → date
  자산분류    → select
  총평가액    → number
  직전평가액  → number
  변동액      → number
  종목수      → number
  변동율      → number
  구성비      → number
  정렬순서    → number
"""

import os
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
DB_EVAL_RESULT    = os.environ["DB_EVAL_RESULT"]
DB_WEEKLY_SUMMARY = os.environ["DB_WEEKLY_SUMMARY"]

HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

KST = timezone(timedelta(hours=9))

SORT_ORDER = {
    "예적금":   1,
    "한국주식": 2,
    "미국주식": 3,
    "부동산":   4,
    "암호화폐": 5,
    "전체":    99,
}


def get_run_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


# ── 1. 자산평가결과 DB 당일 데이터 조회 ──────────────────
def fetch_eval_results(run_date: str) -> list[dict]:
    """
    평가일자(Title) = run_date 인 행 조회
    Title 타입이므로 rich_text contains 필터 사용
    """
    url = f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query"
    payload = {
        "filter": {
            "property": "평가일자",
            "title": {"equals": run_date}        # Title 타입은 title 필터 사용
        },
        "page_size": 100,
    }

    rows = []
    has_more = True
    start_cursor = None

    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor

        res = requests.post(url, headers=HEADERS, json=payload)
        res.raise_for_status()
        data = res.json()

        for page in data.get("results", []):
            props = page["properties"]

            category = props.get("자산분류", {}).get("select", {})
            category = category.get("name", "") if category else ""

            amount = props.get("평가액", {}).get("number")
            prev   = props.get("직전평가액", {}).get("number")

            if category and amount is not None:
                rows.append({
                    "분류":       category,
                    "평가액":     float(amount),
                    "직전평가액": float(prev) if prev is not None else None,
                })

        has_more     = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    print(f"[Summary] 평가결과 조회: {len(rows)}건")
    return rows


# ── 2. 분류별 집계 ────────────────────────────────────────
def aggregate(rows: list[dict]) -> dict:
    totals = defaultdict(lambda: {"평가액": 0.0, "직전평가액": 0.0, "종목수": 0, "직전있음": 0})

    for row in rows:
        cat = row["분류"]
        totals[cat]["평가액"]  += row["평가액"]
        totals[cat]["종목수"]  += 1
        if row["직전평가액"] is not None:
            totals[cat]["직전평가액"] += row["직전평가액"]
            totals[cat]["직전있음"]   += 1

    # 직전평가액: 분류 내 일부만 있으면 None (비교 불가)
    for cat, v in totals.items():
        if v["직전있음"] < v["종목수"]:
            v["직전평가액"] = None

    # 전체 합산
    grand = {"평가액": 0.0, "직전평가액": 0.0, "종목수": 0, "직전있음": 0}
    for cat, v in totals.items():
        grand["평가액"]  += v["평가액"]
        grand["종목수"]  += v["종목수"]
        if v["직전평가액"] is not None:
            grand["직전평가액"] += v["직전평가액"]
            grand["직전있음"]   += 1

    if grand["직전있음"] < len(totals):
        grand["직전평가액"] = None

    totals["전체"] = grand

    for cat, v in totals.items():
        prev_str = f'{v["직전평가액"]:,.0f}원' if v["직전평가액"] is not None else "없음"
        print(f"[Summary] {cat:8s} | {v['평가액']:>15,.0f}원 | 직전: {prev_str} | {v['종목수']}종목")

    return dict(totals)


# ── 3. 구성비 계산 (반올림 오차 보정) ────────────────────
def calc_ratio(totals: dict) -> dict[str, float]:
    grand_total = totals.get("전체", {}).get("평가액", 0)
    if grand_total == 0:
        return {k: 0.0 for k in totals}

    categories  = [k for k in totals if k != "전체"]
    ratios      = {}
    running_sum = 0.0

    for i, cat in enumerate(categories):
        if i < len(categories) - 1:
            r = round(totals[cat]["평가액"] / grand_total * 100, 1)
        else:
            r = round(100.0 - running_sum, 1)
        ratios[cat] = r
        running_sum += r

    ratios["전체"] = 100.0
    return ratios


# ── 4. 주간자산요약 DB 저장 ───────────────────────────────
def save_summary(category: str, data: dict, ratio: float, run_date: str) -> None:
    eval_amount = data["평가액"]
    prev_amount = data["직전평가액"]
    count       = data["종목수"]
    sort_no     = SORT_ORDER.get(category, 50)
    title_val   = f"{run_date}_{category}"

    change_amount = None
    change_rate   = None
    if prev_amount is not None and prev_amount > 0:
        change_amount = eval_amount - prev_amount
        change_rate   = round(change_amount / prev_amount * 100, 2)

    properties = {
        "기준일_분류": {
            "title": [{"text": {"content": title_val}}]
        },
        "평가일자": {
            "date": {"start": run_date}
        },
        "자산분류": {
            "select": {"name": category}
        },
        "총평가액": {
            "number": round(eval_amount / 1e8, 2)
        },
        "종목수": {
            "number": count
        },
        "구성비": {
            "number": ratio
        },
        "정렬순서": {
            "number": sort_no
        },
    }

    if prev_amount is not None:
        properties["직전평가액"] = {"number": round(prev_amount / 1e8, 2)}
    if change_amount is not None:
        properties["변동액"] = {"number": round(change_amount / 1e8, 2)}
    if change_rate is not None:
        properties["변동율"] = {"number": change_rate}

    url = "https://api.notion.com/v1/pages"
    payload = {
        "parent": {"database_id": DB_WEEKLY_SUMMARY},
        "properties": properties,
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    change_str = f"  변동: {change_amount:+,.0f}원 ({change_rate:+.2f}%)" if change_amount is not None else ""
    print(f"[Notion] {category:8s} | {eval_amount:>15,.0f}원 | 구성비: {ratio:.1f}%{change_str}  저장완료")


# ── MAIN ──────────────────────────────────────────────────
def main():
    run_date = get_run_date()
    print(f"\n{'='*50}")
    print(f"[Summary] 실행일(KST): {run_date}")
    print(f"{'='*50}")

    rows = fetch_eval_results(run_date)
    if not rows:
        print("[Summary] 당일 평가결과 없음 - 종료")
        return

    totals = aggregate(rows)
    ratios = calc_ratio(totals)

    ordered = sorted(totals.keys(), key=lambda k: SORT_ORDER.get(k, 50))
    for cat in ordered:
        save_summary(cat, totals[cat], ratios[cat], run_date)

    ratio_sum = sum(v for k, v in ratios.items() if k != "전체")
    print(f"\n[Summary] 완료 | 구성비 합계: {ratio_sum:.1f}% | {len(ordered)}개 분류")


if __name__ == "__main__":
    main()
