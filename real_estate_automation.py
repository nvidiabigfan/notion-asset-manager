"""
Phase 3: 부동산 실거래가 자동화
- 국토교통부 아파트 매매 실거래가 공공 API 활용
- 영등포구 신길동, 전용 89.16㎡ 기준 직전 5개 실거래가 평균 산출
- 부동산 실거래가 DB에 저장 + 자산평가 결과 DB 업데이트

GitHub Secrets 필요:
  - NOTION_TOKEN
  - PUBLIC_DATA_API_KEY  (공공데이터포털에서 발급)
"""

import os
import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


# ─── 설정 ────────────────────────────────────────────────────────────────────
NOTION_TOKEN        = os.environ["NOTION_TOKEN"]
PUBLIC_DATA_API_KEY = os.environ["PUBLIC_DATA_API_KEY"]  # URL-decoded key

# 노션 DB ID
DB_ASSET_STATUS    = "31a64e13bb46807b8673e94e7b416f34"  # 자산보유현황
DB_REAL_ESTATE     = "31a64e13bb4680c18668eec357e11222"  # 부동산 실거래가
DB_EVAL_RESULT     = "31a64e13bb46802c91e1f5502631a154"  # 자산평가 결과
DB_EXCHANGE_RATE   = "31a64e13bb4680a491b8c1c2ca7770bc"  # 환율정보

# 부동산 대상 물건 (자산보유현황 DB의 자산명과 일치해야 함)
REAL_ESTATE_TARGETS = [
    {
        "asset_name":   "서울시 영등포구 신길동",  # 자산보유현황 DB 자산명
        "lawd_cd":      "11560",                   # 영등포구 법정동코드 앞 5자리
        "dong":         "신길동",                  # 법정동명 필터
        "area":         89.16,                     # 전용면적 (㎡)
        "area_margin":  0.1,                       # 면적 허용 오차 (±㎡)
        "recent_count": 5,                         # 직전 N개 실거래 평균
        "search_months": 24,                       # 최대 소급 조회 개월수
    }
]

# 국토교통부 API endpoint (신버전 apis.data.go.kr)
MOLIT_API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type":  "application/json",
    "Notion-Version": "2022-06-28",
}


# ─── 유틸 ─────────────────────────────────────────────────────────────────────
def notion_request(method, url, **kwargs):
    """노션 API 호출 (429 rate limit 자동 재시도)"""
    for attempt in range(5):
        resp = requests.request(method, url, headers=NOTION_HEADERS, **kwargs)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            print(f"  ⚠ Rate limit, {wait}s 대기 후 재시도...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"노션 API 반복 실패: {url}")


def get_year_months(months_back: int) -> list[str]:
    """현재 월부터 N개월 전까지 YYYYMM 리스트 반환 (최신 순)"""
    now = datetime.now()
    result = []
    for i in range(months_back):
        dt = now - relativedelta(months=i)
        result.append(dt.strftime("%Y%m"))
    return result


# ─── 국토교통부 API ───────────────────────────────────────────────────────────
def fetch_apt_trades(lawd_cd: str, deal_ymd: str) -> list[dict]:
    """
    특정 지역/년월의 아파트 매매 실거래가 조회
    Returns: list of dict with keys: dong, apt_name, area, price_won, deal_date, floor
    """
    params = {
        "serviceKey": PUBLIC_DATA_API_KEY,
        "LAWD_CD":    lawd_cd,
        "DEAL_YMD":   deal_ymd,
        "numOfRows":  "1000",
        "pageNo":     "1",
    }
    try:
        resp = requests.get(MOLIT_API_URL, params=params, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ⚠ API 호출 실패 ({deal_ymd}): {e}")
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        print(f"  ⚠ XML 파싱 실패 ({deal_ymd}): {e}")
        return []

    # 결과코드 확인
    result_code = root.findtext(".//resultCode", "")
    if result_code not in ("00", "000", "0000"):
        result_msg = root.findtext(".//resultMsg", "")
        print(f"  ⚠ API 오류 ({deal_ymd}): {result_code} - {result_msg}")
        return []

    trades = []
    for item in root.findall(".//item"):
        def txt(tag):
            v = item.findtext(tag, "")
            return v.strip() if v else ""

        try:
            area_str  = txt("excluUseAr")           # 전용면적
            price_str = txt("dealAmount").replace(",", "")  # 거래금액 (만원)
            year      = txt("dealYear")
            month     = txt("dealMonth").zfill(2)
            day       = txt("dealDay").zfill(2)

            if not (area_str and price_str and year and month and day):
                continue

            trades.append({
                "dong":       txt("umdNm"),           # 법정동
                "apt_name":   txt("aptNm"),            # 아파트명
                "area":       float(area_str),         # 전용면적 (㎡)
                "price_won":  int(price_str) * 10000,  # 원 단위 변환
                "deal_date":  f"{year}-{month}-{day}", # 계약일
                "floor":      txt("floor"),            # 층
                "build_year": txt("buildYear"),        # 건축년도
            })
        except (ValueError, TypeError):
            continue

    return trades


def get_recent_trades(lawd_cd: str, dong: str, area: float,
                      area_margin: float, recent_count: int,
                      search_months: int) -> list[dict]:
    """
    동 + 면적 조건으로 필터링한 최근 N건 실거래 반환
    최신 월부터 소급 조회, N건 충족 시 중단
    """
    matched = []
    for ym in get_year_months(search_months):
        print(f"  📅 {ym} 조회 중...")
        trades = fetch_apt_trades(lawd_cd, ym)

        for t in trades:
            # 법정동 필터 (부분 일치)
            if dong not in t["dong"]:
                continue
            # 면적 필터 (±margin)
            if abs(t["area"] - area) > area_margin:
                continue
            matched.append(t)

        # 날짜 역순 정렬 후 N건 초과 여부 체크
        matched.sort(key=lambda x: x["deal_date"], reverse=True)
        if len(matched) >= recent_count:
            break

        time.sleep(0.3)  # API 과호출 방지

    return matched[:recent_count]


# ─── 노션 조회 ────────────────────────────────────────────────────────────────
def get_asset_info(asset_name: str) -> dict | None:
    """자산보유현황 DB에서 해당 자산 조회"""
    resp = notion_request(
        "POST",
        f"https://api.notion.com/v1/databases/{DB_ASSET_STATUS}/query",
        json={
            "filter": {
                "property": "자산명",
                "title": {"equals": asset_name}
            }
        }
    )
    results = resp.get("results", [])
    if not results:
        print(f"  ⚠ 자산보유현황에서 '{asset_name}' 없음")
        return None

    page = results[0]
    props = page["properties"]

    def num(key):
        v = props.get(key, {}).get("number")
        return v if v is not None else 0

    return {
        "page_id":   page["id"],
        "asset_name": asset_name,
        "category":  props.get("자산분류", {}).get("select", {}).get("name", ""),
        "quantity":  num("수량"),
        "unit_price": num("금액"),
    }


def get_prev_eval(asset_name: str) -> float:
    """자산평가 결과 DB에서 해당 자산의 직전 평가액 조회"""
    resp = notion_request(
        "POST",
        f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query",
        json={
            "filter": {
                "property": "자산명",
                "rich_text": {"contains": asset_name}
            },
            "sorts": [{"property": "평가일자", "direction": "descending"}],
            "page_size": 1
        }
    )
    results = resp.get("results", [])
    if not results:
        return 0.0
    props = results[0]["properties"]
    return props.get("평가액", {}).get("number") or 0.0


# ─── 노션 저장 ────────────────────────────────────────────────────────────────
def save_to_real_estate_db(asset_name: str, trades: list[dict], avg_price: float):
    """
    부동산 실거래가 DB에 저장
    노션 DB 실제 컬럼: 지번/주소(Title), 거래일자(Date), 거래금액(Number), 출처(Text), 비고(Text)
    기존 레코드(동일 지번/주소 + 오늘 날짜)가 있으면 업데이트, 없으면 신규 생성
    """
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 기존 레코드 조회 (지번/주소 + 오늘)
    resp = notion_request(
        "POST",
        f"https://api.notion.com/v1/databases/{DB_REAL_ESTATE}/query",
        json={
            "filter": {
                "and": [
                    {"property": "지번/주소", "title": {"equals": asset_name}},
                    {"property": "거래일자", "date": {"equals": today_str}},
                ]
            }
        }
    )
    existing = resp.get("results", [])

    # 비고: 최근 거래 목록 요약
    ref_lines = []
    for t in trades:
        price_uk = t["price_won"] // 100_000_000
        price_ck = (t["price_won"] % 100_000_000) // 10_000
        ref_lines.append(
            f"{t['deal_date']} | {t['apt_name']} {t['floor']}층 | "
            f"{t['area']}㎡ | {price_uk}억{price_ck:,}만원"
        )
    ref_text = "\n".join(ref_lines)

    properties = {
        "지번/주소": {"title": [{"text": {"content": asset_name}}]},
        "거래일자":  {"date": {"start": today_str}},
        "거래금액":  {"number": round(avg_price)},
        "출처":     {"rich_text": [{"text": {"content": "국토부"}}]},
        "비고":     {"rich_text": [{"text": {"content": ref_text[:2000]}}]},
    }

    if existing:
        page_id = existing[0]["id"]
        notion_request(
            "PATCH",
            f"https://api.notion.com/v1/pages/{page_id}",
            json={"properties": properties}
        )
        print(f"  ✅ 부동산 실거래가 DB 업데이트: {asset_name}")
    else:
        notion_request(
            "POST",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": DB_REAL_ESTATE}, "properties": properties}
        )
        print(f"  ✅ 부동산 실거래가 DB 신규 저장: {asset_name}")

    time.sleep(0.4)


def save_to_eval_result_db(asset_info: dict, current_price: float, prev_eval: float):
    """
    자산평가 결과 DB에 부동산 평가 레코드 저장
    주식과 동일한 구조 사용
    수량=1 (건물 1채), 금액=매수원가, 현재가=평균실거래가, 평가액=현재가×수량
    """
    today_str  = datetime.now().strftime("%Y-%m-%d")
    asset_name = asset_info["asset_name"]
    quantity   = asset_info["quantity"] if asset_info["quantity"] > 0 else 1
    unit_price = asset_info["unit_price"]  # 매수 당시 가격 (원)
    cost       = unit_price * quantity     # 매수원가
    eval_amt   = current_price * quantity  # 평가액

    # 기존 레코드 확인 (오늘 + 자산명)
    resp = notion_request(
        "POST",
        f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query",
        json={
            "filter": {
                "and": [
                    {"property": "평가일자", "title": {"equals": today_str}},
                    {"property": "자산명",   "rich_text": {"equals": asset_name}},
                ]
            }
        }
    )
    existing = resp.get("results", [])

    properties = {
        "평가일자": {"title": [{"text": {"content": today_str}}]},
        "자산명":   {"rich_text": [{"text": {"content": asset_name}}]},
        "자산분류": {"select": {"name": "부동산"}},
        "수량":     {"number": quantity},
        "금액":     {"number": cost},
        "현재가":   {"number": round(current_price)},
        "평가액":   {"number": round(eval_amt)},
        "직전평가액": {"number": round(prev_eval)},
        "참고값":   {"rich_text": [{"text": {"content": f"직전 {len([current_price])}개 실거래가 평균 적용"}}]},
    }

    if existing:
        page_id = existing[0]["id"]
        notion_request(
            "PATCH",
            f"https://api.notion.com/v1/pages/{page_id}",
            json={"properties": properties}
        )
        print(f"  ✅ 자산평가 결과 DB 업데이트: {asset_name} | 평가액 {eval_amt:,.0f}원")
    else:
        notion_request(
            "POST",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": DB_EVAL_RESULT}, "properties": properties}
        )
        print(f"  ✅ 자산평가 결과 DB 신규 저장: {asset_name} | 평가액 {eval_amt:,.0f}원")

    time.sleep(0.4)


# ─── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"🏠 Phase 3: 부동산 실거래가 자동화 시작")
    print(f"   실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S KST')}")
    print("=" * 60)

    for target in REAL_ESTATE_TARGETS:
        asset_name = target["asset_name"]
        print(f"\n📌 대상: {asset_name}")
        print(f"   조건: {target['dong']} | {target['area']}㎡ (±{target['area_margin']})")

        # 1) 국토교통부 API로 실거래 조회
        print(f"\n[1/4] 실거래가 API 조회 (최근 {target['recent_count']}건)")
        trades = get_recent_trades(
            lawd_cd       = target["lawd_cd"],
            dong          = target["dong"],
            area          = target["area"],
            area_margin   = target["area_margin"],
            recent_count  = target["recent_count"],
            search_months = target["search_months"],
        )

        if not trades:
            print(f"  ⚠ 실거래 데이터 없음 — 해당 조건의 거래 내역을 찾을 수 없습니다.")
            print(f"     ※ area_margin을 넓히거나 search_months를 늘려보세요.")
            continue

        print(f"  📊 조회된 거래: {len(trades)}건")
        for t in trades:
            price_uk = t["price_won"] // 100_000_000
            price_ck = (t["price_won"] % 100_000_000) // 10_000
            print(f"     {t['deal_date']} | {t['apt_name']} {t['floor']}층 | "
                  f"{t['area']}㎡ | {price_uk}억{price_ck:,}만원")

        # 2) 평균 산출
        avg_price = sum(t["price_won"] for t in trades) / len(trades)
        avg_uk    = avg_price // 100_000_000
        avg_ck    = (avg_price % 100_000_000) // 10_000
        print(f"\n[2/4] 평균 실거래가: {avg_uk:.0f}억 {avg_ck:,.0f}만원 "
              f"({avg_price:,.0f}원)")

        # 3) 부동산 실거래가 DB 저장
        print(f"\n[3/4] 부동산 실거래가 DB 저장")
        save_to_real_estate_db(asset_name, trades, avg_price)

        # 4) 자산평가 결과 DB 저장
        print(f"\n[4/4] 자산평가 결과 DB 저장")
        asset_info = get_asset_info(asset_name)
        if asset_info:
            prev_eval = get_prev_eval(asset_name)
            save_to_eval_result_db(asset_info, avg_price, prev_eval)
        else:
            print(f"  ⚠ 자산보유현황 미등록 — 자산평가 결과 저장 건너뜀")

    print("\n" + "=" * 60)
    print("✅ Phase 3 완료")
    print("=" * 60)


if __name__ == "__main__":
    main()
