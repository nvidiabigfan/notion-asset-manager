"""
weekly_summary_automation.py
Phase 4 — 자산분류별 주간 요약 집계
실행 시점: 환율/주식/부동산 업데이트 완료 후 마지막에 실행
그룹핑 기준: 자산분류 (예적금 / 한국주식 / 미국주식 / 부동산) — 전체 원화 기준
"""

import os
import requests
from datetime import datetime
import pytz

# ── 인증 ──────────────────────────────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ── DB IDs ────────────────────────────────────────────────────────────────────
DB_ASSET_RESULT   = "31a64e13bb46802c91e1f5502631a154"  # 자산평가 결과 (기존)
DB_WEEKLY_SUMMARY = os.environ.get("DB_WEEKLY_SUMMARY", "")  # 주간자산요약 (신규, Secret 또는 직접 입력)
# ※ DB_WEEKLY_SUMMARY 는 노션에서 DB 생성 후 ID를 GitHub Secret 'DB_WEEKLY_SUMMARY' 에 추가하거나
#   아래 라인에 직접 하드코딩 가능
# DB_WEEKLY_SUMMARY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# ── 분류 고정 순서 (노션 Select 옵션과 동일하게 유지) ─────────────────────────
CATEGORY_ORDER = ["예적금", "한국주식", "미국주식", "부동산", "전체"]

KST = pytz.timezone("Asia/Seoul")


# ─────────────────────────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────────────────────────

def today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def get_number(props: dict, key: str) -> float:
    val = props.get(key, {})
    if val.get("type") == "number":
        return val.get("number") or 0
    if val.get("type") == "formula":
        inner = val.get("formula", {})
        return inner.get("number") or 0
    return val.get("number") or 0


# ─────────────────────────────────────────────────────────────────────────────
# 노션 조회
# ─────────────────────────────────────────────────────────────────────────────

def fetch_today_results(target_date: str) -> list[dict]:
    """자산평가 결과 DB → 오늘 날짜의 모든 행 조회"""
    url = f"https://api.notion.com/v1/databases/{DB_ASSET_RESULT}/query"
    results, cursor = [], None

    while True:
        payload: dict = {
            "filter": {
                "property": "평가일자",
                "title": {"contains": target_date}
            },
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor

        res = requests.post(url, headers=HEADERS, json=payload)
        res.raise_for_status()
        data = res.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    print(f"[INFO] 자산평가 결과 조회: {len(results)}건 ({target_date})")
    return results


def fetch_prev_weekly_summary(target_date: str) -> dict[str, float]:
    """주간자산요약 DB → target_date 이전 가장 최근 기준일의 분류별 총평가액 반환"""
    url = f"https://api.notion.com/v1/databases/{DB_WEEKLY_SUMMARY}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "평가일자", "date": {"before": target_date}},
                {"property": "자산분류", "select": {"does_not_equal": "전체"}},  # 전체 행 제외
            ]
        },
        "sorts": [{"property": "평가일자", "direction": "descending"}],
        "page_size": 20,
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()
    rows = res.json().get("results", [])

    prev: dict[str, float] = {}
    latest_date = None

    for row in rows:
        props = row["properties"]
        row_date_obj = props.get("평가일자", {}).get("date")
        if not row_date_obj:
            continue
        row_date = row_date_obj["start"]

        if latest_date is None:
            latest_date = row_date
        if row_date != latest_date:
            break  # 가장 최근 날짜 데이터만 수집

        분류_obj = props.get("자산분류", {}).get("select")
        분류 = 분류_obj["name"] if 분류_obj else ""
        총평가액 = get_number(props, "총평가액")
        if 분류:
            prev[분류] = 총평가액

    if latest_date:
        print(f"[INFO] 직전 주간요약 기준일: {latest_date} | 분류수: {len(prev)}")
    else:
        print("[INFO] 직전 주간요약 없음 (최초 실행)")

    return prev


# ─────────────────────────────────────────────────────────────────────────────
# 집계
# ─────────────────────────────────────────────────────────────────────────────

def aggregate_by_category(rows: list[dict]) -> dict[str, dict]:
    """자산분류별 총평가액 / 종목수 집계 (원화 기준 — 미국주식도 KRW 환산값 사용)"""
    summary: dict[str, dict] = {}

    for row in rows:
        props = row["properties"]

        # 자산분류
        cat_obj = props.get("자산분류", {}).get("select")
        분류 = cat_obj["name"] if cat_obj else "기타"

        # 평가액 (미국주식은 stock_price_automation.py 에서 이미 KRW 환산 저장)
        평가액 = get_number(props, "평가액")

        if 분류 not in summary:
            summary[분류] = {"총평가액": 0.0, "종목수": 0}
        summary[분류]["총평가액"] += 평가액
        summary[분류]["종목수"] += 1

    return summary


def enrich_with_composition(summary: dict[str, dict]) -> dict[str, dict]:
    """전체 합계 대비 구성비(%) 계산"""
    total = sum(v["총평가액"] for v in summary.values())
    for 분류 in summary:
        ratio = (summary[분류]["총평가액"] / total * 100) if total > 0 else 0.0
        summary[분류]["구성비"] = round(ratio, 2)
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# 노션 저장
# ─────────────────────────────────────────────────────────────────────────────

def build_properties(target_date: str, 분류: str, data: dict, prev_amount: float) -> dict:
    """노션 페이지 properties 빌드"""
    title_key = f"{target_date}_{분류}"
    return {
        "기준일_분류": {"title": [{"text": {"content": title_key}}]},
        "평가일자":   {"date":   {"start": target_date}},
        "자산분류":   {"select": {"name": 분류}},
        "총평가액":   {"number": round(data["총평가액"])},
        "직전평가액": {"number": round(prev_amount)},
        "구성비":    {"number": data.get("구성비", 0.0)},
        "종목수":    {"number": data["종목수"]},
    }


def upsert_row(target_date: str, 분류: str, data: dict, prev_amount: float):
    """주간자산요약 DB upsert (동일 기준일_분류 키 존재 시 UPDATE, 없으면 CREATE)"""
    title_key = f"{target_date}_{분류}"

    # 기존 행 조회
    res = requests.post(
        f"https://api.notion.com/v1/databases/{DB_WEEKLY_SUMMARY}/query",
        headers=HEADERS,
        json={"filter": {"property": "기준일_분류", "title": {"equals": title_key}}},
    )
    res.raise_for_status()
    existing = res.json().get("results", [])
    properties = build_properties(target_date, 분류, data, prev_amount)

    if existing:
        page_id = existing[0]["id"]
        requests.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=HEADERS,
            json={"properties": properties},
        ).raise_for_status()
        print(f"  [UPDATE] {title_key:<30} 총평가액: {data['총평가액']:>15,.0f}원  "
              f"직전: {prev_amount:>15,.0f}원  "
              f"구성비: {data.get('구성비', 0):>5.1f}%")
    else:
        requests.post(
            "https://api.notion.com/v1/pages",
            headers=HEADERS,
            json={"parent": {"database_id": DB_WEEKLY_SUMMARY}, "properties": properties},
        ).raise_for_status()
        print(f"  [CREATE] {title_key:<30} 총평가액: {data['총평가액']:>15,.0f}원  "
              f"직전: {prev_amount:>15,.0f}원  "
              f"구성비: {data.get('구성비', 0):>5.1f}%")


# ─────────────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if not DB_WEEKLY_SUMMARY:
        print("[ERROR] DB_WEEKLY_SUMMARY 환경변수(또는 코드 내 하드코딩)가 설정되지 않았습니다.")
        print("        노션에서 '주간자산요약' DB 생성 후 ID를 입력하세요.")
        raise SystemExit(1)

    target_date = today_kst()
    print(f"\n{'='*60}")
    print(f"  Phase 4 — 주간자산요약 집계  |  기준일: {target_date}")
    print(f"{'='*60}")

    # 1. 오늘 자산평가 결과 전체 조회
    rows = fetch_today_results(target_date)
    if not rows:
        print(f"[WARN] {target_date} 자산평가 결과가 없습니다. Phase 1~3 완료 후 재실행하세요.")
        return

    # 2. 분류별 집계 + 구성비 계산
    summary = aggregate_by_category(rows)
    summary = enrich_with_composition(summary)

    # 3. 직전 주 데이터 조회
    prev_summary = fetch_prev_weekly_summary(target_date)

    # 4. 분류별 노션 upsert (고정 순서대로)
    print("\n[분류별 집계 결과]")
    ordered_categories = [c for c in CATEGORY_ORDER if c != "전체" and c in summary]
    # CATEGORY_ORDER에 없는 분류(기타 등) 후순위 추가
    extra = [c for c in summary if c not in CATEGORY_ORDER]
    for 분류 in ordered_categories + extra:
        upsert_row(target_date, 분류, summary[분류], prev_summary.get(분류, 0.0))

    # 5. 전체 합계 행
    total_amount = sum(v["총평가액"] for v in summary.values())
    total_종목수  = sum(v["종목수"]  for v in summary.values())
    prev_total   = sum(prev_summary.values())
    upsert_row(target_date, "전체", {
        "총평가액": total_amount,
        "구성비":  100.0,
        "종목수":  total_종목수,
    }, prev_total)

    # 6. 요약 출력
    print(f"\n{'─'*60}")
    변동액 = total_amount - prev_total
    변동율 = (변동액 / prev_total * 100) if prev_total > 0 else 0.0
    print(f"  총 평가액:  {total_amount:>18,.0f} 원")
    print(f"  직전 평가액: {prev_total:>18,.0f} 원")
    print(f"  주간 변동:  {변동액:>+18,.0f} 원  ({변동율:+.2f}%)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
