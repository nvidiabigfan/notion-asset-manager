"""
weekly_summary_automation.py  (v6 - 직전기준일 자동 조회)
Phase 4 - 자산평가결과 → 분류별 집계 → 주간자산요약 DB 저장

[변경사항 v6]
  직전평가액을 자산평가결과 DB의 컬럼에 의존하지 않고,
  주간자산요약 DB에서 run_date 이전 가장 최근 기준일 데이터를 직접 조회하여 사용.

[자산평가결과 DB 컬럼 구조]
  평가일자   → Title (문자열 "2026-03-07")
  자산명     → rich_text
  자산분류   → select
  수량       → number
  금액       → number
  현재가     → number
  평가액     → number

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
    "연금":     6,
    "전체":     7,
}


def get_run_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


# ── 1. 자산평가결과 DB 당일 데이터 조회 ──────────────────
def fetch_eval_results(run_date: str) -> list[dict]:
    """
    평가일자(Title) = run_date 인 행 조회 → 분류별 평가액만 집계
    직전평가액은 이 함수에서 읽지 않음 (v6에서 별도 조회로 변경)
    """
    url = f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query"
    payload = {
        "filter": {
            "property": "평가일자",
            "title": {"equals": run_date}
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

            if category and amount is not None:
                rows.append({
                    "분류":   category,
                    "평가액": float(amount),
                })

        has_more     = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    print(f"[Summary] 평가결과 조회: {len(rows)}건")
    return rows


# ── 2. 분류별 집계 ────────────────────────────────────────
def aggregate(rows: list[dict]) -> dict:
    totals = defaultdict(lambda: {"평가액": 0.0, "종목수": 0})

    for row in rows:
        cat = row["분류"]
        totals[cat]["평가액"] += row["평가액"]
        totals[cat]["종목수"] += 1

    # 전체 합산
    grand_total = sum(v["평가액"] for v in totals.values())
    grand_count = sum(v["종목수"] for v in totals.values())
    totals["전체"] = {"평가액": grand_total, "종목수": grand_count}

    for cat, v in totals.items():
        prev_str = f"{v['평가액']:>15,.0f}원"
        print(f"[Summary] {cat:8s} | {prev_str} | {v['종목수']}종목")

    return dict(totals)


# ── 3. 주간자산요약 DB에서 직전 기준일 데이터 조회 ────────
def fetch_prev_summary(run_date: str) -> dict[str, float]:
    """
    주간자산요약 DB에서 run_date 이전 가장 최근 기준일의
    분류별 총평가액을 반환.
    반환: {"한국주식": 72000000.0, "미국주식": ..., "전체": ...}
    """
    url = f"https://api.notion.com/v1/databases/{DB_WEEKLY_SUMMARY}/query"
    payload = {
        "filter": {
            "property": "평가일자",
            "date": {"before": run_date}
        },
        "sorts": [
            {"property": "평가일자", "direction": "descending"}
        ],
        "page_size": 100,
    }

    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()
    data = res.json()

    results = data.get("results", [])
    if not results:
        print("[Summary] 직전 기준일 데이터 없음")
        return {}

    # 가장 최근 기준일 추출
    first_props = results[0]["properties"]
    prev_date_val = first_props.get("평가일자", {}).get("date", {})
    prev_date = prev_date_val.get("start") if prev_date_val else None

    if not prev_date:
        print("[Summary] 직전 기준일 날짜 파싱 실패")
        return {}

    print(f"[Summary] 직전 기준일: {prev_date}")

    # 해당 기준일의 모든 분류 수집
    prev_map = {}
    for page in results:
        props = page["properties"]
        date_val = props.get("평가일자", {}).get("date", {})
        date_str = date_val.get("start") if date_val else None

        if date_str != prev_date:
            break  # 날짜 내림차순 정렬이므로 다른 날짜 나오면 중단

        cat_val = props.get("자산분류", {}).get("select", {})
        cat = cat_val.get("name", "") if cat_val else ""

        amt = props.get("총평가액", {}).get("number")

        if cat and amt is not None:
            # 억 단위 → 원 단위로 복원
            prev_map[cat] = float(amt) * 1e8

    print(f"[Summary] 직전 기준일 분류: {list(prev_map.keys())}")
    return prev_map


# ── 4. 구성비 계산 (반올림 오차 보정) ────────────────────
def calc_ratio(totals: dict) -> dict[str, float]:
    grand_total = totals.get("전체", {}).get("평가액", 0)
    if not grand_total:
        return {cat: 0.0 for cat in totals}

    categories = [c for c in totals if c != "전체"]
    ratios = {}
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


# ── 5. 주간자산요약 DB 저장 ───────────────────────────────
def save_summary(category: str, data: dict, ratio: float,
                 run_date: str, prev_map: dict) -> None:
    eval_amount = data["평가액"]
    count       = data["종목수"]
    sort_no     = SORT_ORDER.get(category, 50)
    title_val   = run_date

    # 직전평가액: prev_map에서 해당 분류 조회
    prev_amount = prev_map.get(category)

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
        "parent":     {"database_id": DB_WEEKLY_SUMMARY},
        "properties": properties,
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    change_str = (
        f"  변동: {change_amount:+,.0f}원 ({change_rate:+.2f}%)"
        if change_amount is not None else "  직전없음"
    )
    print(f"[Notion] {category:8s} | {eval_amount:>15,.0f}원 | 구성비: {ratio:.1f}%{change_str} 저장완료")


# ── MAIN ──────────────────────────────────────────────────
def main():
    run_date = get_run_date()
    print(f"\n{'='*50}")
    print(f"[Summary] 실행일(KST): {run_date}")
    print(f"{'='*50}")

    # 1. 당일 평가결과 조회 및 집계
    rows = fetch_eval_results(run_date)
    if not rows:
        print("[Summary] 당일 평가결과 없음 - 종료")
        return

    totals = aggregate(rows)
    ratios = calc_ratio(totals)

    # 2. 직전 기준일 데이터 조회
    prev_map = fetch_prev_summary(run_date)

    # 3. 분류별 저장
    ordered = sorted(totals.keys(), key=lambda k: SORT_ORDER.get(k, 50))
    for cat in ordered:
        save_summary(cat, totals[cat], ratios[cat], run_date, prev_map)

    ratio_sum = sum(v for k, v in ratios.items() if k != "전체")
    print(f"\n[Summary] 완료 | 구성비 합계: {ratio_sum:.1f}% | {len(ordered)}개 분류")


if __name__ == "__main__":
    main()
