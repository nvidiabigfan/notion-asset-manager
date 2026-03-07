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

# ── 분류 고정 순서 — 노션 정렬순서 컬럼값으로 사용 ──────────────────────────
# 전체는 항상 마지막(99)으로 고정
CATEGORY_ORDER = ["예적금", "한국주식", "미국주식", "부동산", "전체"]
CATEGORY_SORT  = {cat: idx + 1 for idx, cat in enumerate(CATEGORY_ORDER[:-1])}
CATEGORY_SORT["전체"] = 99  # 전체는 항상 맨 마지막

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

def fetch_latest_results() -> tuple[str, list[dict]]:
    """자산평가 결과 DB → 가장 최근 평가일자의 모든 행 조회
    
    반환: (최신_날짜_문자열, 행_리스트)
    
    ※ 자산평가 결과 DB의 '평가일자'는 Title 타입이므로 날짜 필터 불가.
       전체 조회 후 Title에서 날짜를 파싱하여 최신 기준일 데이터만 수집.
    """
    url = f"https://api.notion.com/v1/databases/{DB_ASSET_RESULT}/query"
    all_rows, cursor = [], None

    # 전체 조회 (페이지네이션)
    while True:
        payload: dict = {
            "sorts": [{"property": "평가일자", "direction": "descending"}],
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor

        res = requests.post(url, headers=HEADERS, json=payload)
        res.raise_for_status()
        data = res.json()
        all_rows.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    if not all_rows:
        return "", []

    # Title(평가일자)에서 날짜 파싱 → 최신 날짜 확정
    def parse_date_from_title(row: dict) -> str:
        title_parts = row["properties"].get("평가일자", {}).get("title", [])
        text = title_parts[0]["plain_text"] if title_parts else ""
        # "2026-03-07_종목명" 또는 "2026-03-07" 형태 모두 처리
        return text[:10] if len(text) >= 10 else ""

    latest_date = max((parse_date_from_title(r) for r in all_rows), default="")
    if not latest_date:
        return "", []

    # 최신 날짜 행만 필터
    latest_rows = [r for r in all_rows if parse_date_from_title(r) == latest_date]

    print(f"[INFO] 자산평가 결과 최신 기준일: {latest_date} | 전체 {len(all_rows)}건 중 최신: {len(latest_rows)}건")
    return latest_date, latest_rows


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
    """자산분류별 총평가액(백만원) / 종목수 집계

    - 평가액이 None(공란)인 행은 제외 (부동산 실거래 없음 등)
    - 미국주식은 stock_price_automation.py 에서 이미 KRW 환산 저장
    - 총평가액은 백만원 단위로 저장 (노션 가독성)
    """
    summary: dict[str, dict] = {}

    for row in rows:
        props = row["properties"]

        cat_obj = props.get("자산분류", {}).get("select")
        분류 = cat_obj["name"] if cat_obj else "기타"

        # 평가액 None 체크 — None이면 해당 행 집계 제외
        평가액_원 = props.get("평가액", {}).get("number")
        if 평가액_원 is None:
            continue

        if 분류 not in summary:
            summary[분류] = {"총평가액_원": 0.0, "종목수": 0}
        summary[분류]["총평가액_원"] += 평가액_원
        summary[분류]["종목수"] += 1

    # 억원 단위 변환 (소수점 2자리)
    for 분류 in summary:
        summary[분류]["총평가액"] = round(summary[분류]["총평가액_원"] / 100_000_000, 2)

    return summary


def enrich_with_composition(summary: dict[str, dict]) -> dict[str, dict]:
    """전체 합계 대비 구성비(%) 계산

    - 분모: 분류별 총평가액_원 합계 (전체 행 생성 전이므로 이중 집계 없음)
    - 반올림 오차는 마지막 항목에서 흡수하여 합계 100% 보장
    """
    total_원 = sum(v["총평가액_원"] for v in summary.values())
    if total_원 == 0:
        for 분류 in summary:
            summary[분류]["구성비"] = 0.0
        return summary

    ratios = {}
    for 분류 in summary:
        ratios[분류] = round(summary[분류]["총평가액_원"] / total_원 * 100, 2)

    # 반올림 오차 보정 → 합계 정확히 100.00%
    diff = round(100.0 - sum(ratios.values()), 2)
    last_key = list(ratios.keys())[-1]
    ratios[last_key] = round(ratios[last_key] + diff, 2)

    for 분류 in summary:
        summary[분류]["구성비"] = ratios[분류]

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# 노션 저장
# ─────────────────────────────────────────────────────────────────────────────

def build_properties(target_date: str, 분류: str, data: dict, prev_억: float) -> dict:
    """노션 페이지 properties 빌드 — 금액은 억원 단위"""
    title_key = f"{target_date}_{분류}"
    return {
        "기준일_분류": {"title": [{"text": {"content": title_key}}]},
        "평가일자":   {"date":   {"start": target_date}},
        "자산분류":   {"select": {"name": 분류}},
        "총평가액":   {"number": data["총평가액"]},   # 이미 억원 단위
        "직전평가액": {"number": round(prev_억, 2)},
        "구성비":    {"number": data.get("구성비", 0.0)},
        "종목수":    {"number": data["종목수"]},
        "정렬순서":  {"number": CATEGORY_SORT.get(분류, 50)},
    }


def upsert_row(target_date: str, 분류: str, data: dict, prev_억: float):
    """주간자산요약 DB upsert — 금액 단위: 억원"""
    title_key = f"{target_date}_{분류}"

    res = requests.post(
        f"https://api.notion.com/v1/databases/{DB_WEEKLY_SUMMARY}/query",
        headers=HEADERS,
        json={"filter": {"property": "기준일_분류", "title": {"equals": title_key}}},
    )
    res.raise_for_status()
    existing   = res.json().get("results", [])
    properties = build_properties(target_date, 분류, data, prev_억)

    action = "UPDATE" if existing else "CREATE"
    if existing:
        requests.patch(
            f"https://api.notion.com/v1/pages/{existing[0]['id']}",
            headers=HEADERS,
            json={"properties": properties},
        ).raise_for_status()
    else:
        requests.post(
            "https://api.notion.com/v1/pages",
            headers=HEADERS,
            json={"parent": {"database_id": DB_WEEKLY_SUMMARY}, "properties": properties},
        ).raise_for_status()

    print(f"  [{action}] {title_key:<30} "
          f"총평가액: {data['총평가액']:>8,.2f}억원  "
          f"직전: {prev_억:>8,.2f}억원  "
          f"구성비: {data.get('구성비', 0):>5.2f}%")


# ─────────────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if not DB_WEEKLY_SUMMARY:
        print("[ERROR] DB_WEEKLY_SUMMARY 환경변수(또는 코드 내 하드코딩)가 설정되지 않았습니다.")
        print("        노션에서 '주간자산요약' DB 생성 후 ID를 입력하세요.")
        raise SystemExit(1)

    run_date = today_kst()
    print(f"\n{'='*60}")
    print(f"  Phase 4 — 주간자산요약 집계  |  실행일: {run_date}")
    print(f"{'='*60}")

    # 1. 자산평가 결과 최신 기준일 데이터 조회
    target_date, rows = fetch_latest_results()
    if not rows:
        print("[WARN] 자산평가 결과가 없습니다. Phase 1~3 완료 후 재실행하세요.")
        return
    print(f"[INFO] 집계 기준일: {target_date} (총 {len(rows)}건)")

    # 2. 분류별 집계 + 구성비 계산
    summary = aggregate_by_category(rows)
    summary = enrich_with_composition(summary)

    # 3. 직전 주 요약 조회 (백만원 단위로 저장되어 있음)
    prev_summary = fetch_prev_weekly_summary(target_date)  # {분류: 백만원값}

    # 4. 분류별 노션 upsert (고정 순서대로)
    print("\n[분류별 집계 결과]")
    ordered = [c for c in CATEGORY_ORDER if c != "전체" and c in summary]
    extra   = [c for c in summary if c not in CATEGORY_ORDER]
    for 분류 in ordered + extra:
        upsert_row(target_date, 분류, summary[분류], prev_summary.get(분류, 0.0))

    # 5. 전체 합계 행 (백만원 단위)
    total_억   = sum(v["총평가액"] for v in summary.values())
    total_종목수 = sum(v["종목수"]  for v in summary.values())
    prev_total  = sum(prev_summary.values())
    upsert_row(target_date, "전체", {
        "총평가액": total_억,
        "구성비":  100.0,
        "종목수":  total_종목수,
    }, prev_total)

    # 6. 요약 출력
    print(f"\n{'─'*60}")
    변동 = total_억 - prev_total
    변동율 = (변동 / prev_total * 100) if prev_total > 0 else 0.0
    print(f"  총 평가액:   {total_억:>10,.2f} 억원")
    print(f"  직전 평가액:  {prev_total:>10,.2f} 억원")
    print(f"  주간 변동:   {변동:>+10,.2f} 억원  ({변동율:+.2f}%)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
