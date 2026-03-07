"""
weekly_summary_automation.py  (Phase 4 → 암호화폐 추가 버전)

자산평가결과 DB → 분류별 집계 → 주간자산요약 DB 저장

그룹핑 기준: 예적금 / 한국주식 / 미국주식 / 부동산 / 암호화폐 / 전체
- 금액 단위: 억원 (소수점 2자리)
- 구성비: 원화 기준, 반올림 오차 보정으로 합계 100% 보장
- 평가액 None 행은 집계 제외
- 정렬순서로 노션 뷰 고정 (전체 = 99)
"""

import os
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── 환경변수 ──────────────────────────────────────────────
NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
DB_EVAL_RESULT    = os.environ["DB_EVAL_RESULT"]     # 자산평가결과 DB ID
DB_WEEKLY_SUMMARY = os.environ["DB_WEEKLY_SUMMARY"]  # 주간자산요약 DB ID

HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

KST = timezone(timedelta(hours=9))

# 정렬 순서 (전체는 항상 마지막)
SORT_ORDER = {
    "예적금":   1,
    "한국주식": 2,
    "미국주식": 3,
    "부동산":   4,
    "암호화폐": 5,   # ← 신규 추가
    "전체":    99,
}


# ── 실행 기준일 ───────────────────────────────────────────
def get_run_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


# ── 1. 자산평가결과 DB에서 당일 데이터 조회 ──────────────
def fetch_eval_results(run_date: str) -> list[dict]:
    """
    평가일자 = run_date 인 전체 행 조회
    반환: [{"분류": "한국주식", "평가액": 5000000}, ...]
    """
    url = f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query"
    payload = {
        "filter": {
            "property": "평가일자",
            "date": {"equals": run_date}
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
                rows.append({"분류": category, "평가액": float(amount)})

        has_more     = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    print(f"[Summary] 평가결과 조회: {len(rows)}건 (평가액 있는 항목)")
    return rows


# ── 2. 분류별 집계 ────────────────────────────────────────
def aggregate(rows: list[dict]) -> dict[str, float]:
    """
    분류별 합산 (KRW 원 단위)
    반환: {"예적금": 10000000, "한국주식": 50000000, ..., "전체": 합계}
    """
    totals: dict[str, float] = defaultdict(float)

    for row in rows:
        totals[row["분류"]] += row["평가액"]

    # 전체 합산
    totals["전체"] = sum(v for k, v in totals.items() if k != "전체")

    print(f"[Summary] 집계 결과: { {k: f'{v:,.0f}원' for k,v in totals.items()} }")
    return dict(totals)


# ── 3. 구성비 계산 (반올림 오차 보정) ────────────────────
def calc_ratio(totals: dict[str, float]) -> dict[str, float]:
    """
    전체 합계 대비 각 분류 구성비 (%)
    마지막 항목에서 오차 흡수 → 합계 100% 보장
    전체 행은 100.0으로 고정
    """
    grand_total = totals.get("전체", 0)
    if grand_total == 0:
        return {k: 0.0 for k in totals}

    categories = [k for k in totals if k != "전체"]
    ratios: dict[str, float] = {}
    running_sum = 0.0

    for i, cat in enumerate(categories):
        if i < len(categories) - 1:
            r = round(totals[cat] / grand_total * 100, 1)
        else:
            # 마지막 항목: 100에서 누적 합 차감으로 오차 흡수
            r = round(100.0 - running_sum, 1)
        ratios[cat] = r
        running_sum += r

    ratios["전체"] = 100.0
    return ratios


# ── 4. 주간자산요약 DB에 저장 ─────────────────────────────
def save_summary(category: str, amount_krw: float, ratio: float,
                 run_date: str) -> None:
    """
    주간자산요약 DB에 1건 저장
    - 평가액: 억원 단위 소수점 2자리
    - 구성비: % (소수점 1자리)
    """
    amount_ok = round(amount_krw / 1e8, 2)   # 원 → 억원
    sort_no   = SORT_ORDER.get(category, 50)

    properties = {
        "자산분류": {
            "title": [{"text": {"content": category}}]
        },
        "평가액(억원)": {
            "number": amount_ok
        },
        "구성비(%)": {
            "number": ratio
        },
        "기준일": {
            "date": {"start": run_date}
        },
        "정렬순서": {
            "number": sort_no
        },
    }

    url = "https://api.notion.com/v1/pages"
    payload = {
        "parent": {"database_id": DB_WEEKLY_SUMMARY},
        "properties": properties,
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    print(f"[Notion] {category:8s} | {amount_ok:.2f}억원 | {ratio:.1f}%  저장 완료")


# ── MAIN ──────────────────────────────────────────────────
def main():
    run_date = get_run_date()
    print(f"\n{'='*50}")
    print(f"[Summary] 실행일(KST): {run_date}")
    print(f"{'='*50}")

    # 1. 당일 평가결과 조회
    rows = fetch_eval_results(run_date)
    if not rows:
        print("[Summary] 당일 평가결과 없음 - 종료")
        return

    # 2. 집계
    totals = aggregate(rows)

    # 3. 구성비
    ratios = calc_ratio(totals)

    # 4. 정렬 순서대로 저장
    ordered = sorted(totals.keys(), key=lambda k: SORT_ORDER.get(k, 50))
    for cat in ordered:
        save_summary(cat, totals[cat], ratios[cat], run_date)

    # 검증
    ratio_sum = sum(v for k, v in ratios.items() if k != "전체")
    print(f"\n[Summary] 완료 | 구성비 합계: {ratio_sum:.1f}% | 총 {len(ordered)}개 분류")


if __name__ == "__main__":
    main()
