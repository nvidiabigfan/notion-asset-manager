"""
Phase 3: 부동산 실거래가 자동화 (v4 - 거래금액 억 단위 소수점 2자리)

수정 내역:
  1. get_prev_eval(): 평가일자(Title) 필터 → rich_text contains + Python에서 날짜 비교
  2. save_to_eval_result_db(): None 값 properties에서 제외 (400 방지)
  3. 중복 체크 쿼리: 평가일자 Title 필터 유지 (equals는 동작함)
"""

import os
import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from dateutil.relativedelta import relativedelta


# ─── 환경변수 ─────────────────────────────────────────────────────────────────
NOTION_TOKEN        = os.environ["NOTION_TOKEN"]
PUBLIC_DATA_API_KEY = os.environ["PUBLIC_DATA_API_KEY"]

# ─── 노션 DB ID ───────────────────────────────────────────────────────────────
DB_ASSET_STATUS = "31a64e13bb46807b8673e94e7b416f34"  # 자산보유현황
DB_REAL_ESTATE  = "31a64e13bb4680c18668eec357e11222"  # 부동산 실거래가
DB_EVAL_RESULT  = "31a64e13bb46802c91e1f5502631a154"  # 자산평가 결과

# ─── 실거래가 조회 공통 설정 ──────────────────────────────────────────────────
RECENT_COUNT  = 5
SEARCH_MONTHS = 24
AREA_MARGIN   = 0.5

# ─── 국토교통부 API endpoint ──────────────────────────────────────────────────
MOLIT_API_URL = {
    "아파트":   "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
    "오피스텔": "https://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
}

# ─── 시군구 → 법정동코드 5자리 매핑 ──────────────────────────────────────────
LAWD_CD_MAP = {
    # 서울
    "종로구": "11110", "중구": "11140", "용산구": "11170",
    "성동구": "11200", "광진구": "11215", "동대문구": "11230",
    "중랑구": "11260", "성북구": "11290", "강북구": "11305",
    "도봉구": "11320", "노원구": "11350", "은평구": "11380",
    "서대문구": "11410", "마포구": "11440", "양천구": "11470",
    "강서구": "11500", "구로구": "11530", "금천구": "11545",
    "영등포구": "11560", "동작구": "11590", "관악구": "11620",
    "서초구": "11650", "강남구": "11680", "송파구": "11710",
    "강동구": "11740",
    # 경기도
    "수원시": "41110", "성남시": "41130", "의정부시": "41150",
    "안양시": "41170", "부천시": "41190", "광명시": "41210",
    "평택시": "41220", "동두천시": "41250", "안산시": "41270",
    "고양시": "41280", "과천시": "41290", "구리시": "41310",
    "남양주시": "41360", "오산시": "41370", "시흥시": "41390",
    "군포시": "41410", "의왕시": "41430", "하남시": "41450",
    "용인시": "41460", "파주시": "41480", "이천시": "41500",
    "안성시": "41550", "김포시": "41570", "화성시": "41590",
    "광주시": "41610", "양주시": "41630", "포천시": "41650",
    "여주시": "41670",
    # 충청남도
    "천안시": "44130", "동남구": "44131", "서북구": "44133",
    "공주시": "44150", "보령시": "44180", "아산시": "44200",
    "서산시": "44210", "논산시": "44230", "계룡시": "44250", "당진시": "44270",
    # 충청북도
    "청주시": "43110", "충주시": "43130", "제천시": "43150",
}

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


# ─── 유틸 ─────────────────────────────────────────────────────────────────────
def notion_request(method, url, **kwargs):
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


def get_year_months(months_back):
    now = datetime.now()
    result = []
    for i in range(months_back):
        dt = now - relativedelta(months=i)
        result.append(dt.strftime("%Y%m"))
    return result


def parse_address(asset_name):
    lawd_cd = None
    for sigungu, code in LAWD_CD_MAP.items():
        if sigungu.endswith("구") and sigungu in asset_name:
            lawd_cd = code
            break
    if not lawd_cd:
        for sigungu, code in LAWD_CD_MAP.items():
            if not sigungu.endswith("구") and sigungu in asset_name:
                lawd_cd = code
                break
    if not lawd_cd:
        print(f"  ⚠ 법정동코드 매핑 실패: '{asset_name}'")
        return {}
    matches = re.findall(r'(\S+(?:동|읍|면))', asset_name)
    if not matches:
        print(f"  ⚠ 동명 추출 실패: '{asset_name}'")
        return {}
    dong = matches[-1]
    return {"lawd_cd": lawd_cd, "dong": dong}


# ─── 자산보유현황 DB 조회 ──────────────────────────────────────────────────────
def get_real_estate_assets():
    resp = notion_request(
        "POST",
        f"https://api.notion.com/v1/databases/{DB_ASSET_STATUS}/query",
        json={
            "filter": {
                "property": "자산분류",
                "select": {"equals": "부동산"}
            }
        }
    )
    assets = []
    for page in resp.get("results", []):
        props = page["properties"]

        def num(key):
            v = props.get(key, {}).get("number")
            return v if v is not None else 0

        title_items = props.get("자산명", {}).get("title", [])
        asset_name  = title_items[0]["text"]["content"] if title_items else ""
        if not asset_name:
            continue

        area = num("전용면적")
        if area <= 0:
            print(f"  ⚠ '{asset_name}' — 전용면적 미입력, 건너뜀")
            continue

        apt_items = props.get("아파트명", {}).get("rich_text", [])
        apt_name  = apt_items[0]["text"]["content"] if apt_items else ""
        bldg_type = props.get("건물유형", {}).get("select", {}).get("name", "아파트")

        assets.append({
            "asset_name": asset_name,
            "quantity":   num("수량"),
            "unit_price": num("금액"),
            "area":       area,
            "apt_name":   apt_name,
            "bldg_type":  bldg_type,
        })

    print(f"  📋 부동산 자산 {len(assets)}건 조회됨")
    return assets


def get_prev_eval(asset_name, run_date):
    """
    직전 평가액 조회
    - 평가일자는 Title 타입 → rich_text contains로 전체 조회 후 Python에서 날짜 비교
    - run_date 미만 데이터 중 가장 최근 값 반환
    - 실패 시 0.0 반환
    """
    try:
        resp = notion_request(
            "POST",
            f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query",
            json={
                "filter": {
                    "and": [
                        {"property": "자산명",   "rich_text": {"equals": asset_name}},
                        {"property": "자산분류", "select":    {"equals": "부동산"}},
                    ]
                },
                "sorts": [{"property": "평가일자", "direction": "descending"}],
                "page_size": 10,
            }
        )
        for page in resp.get("results", []):
            props    = page["properties"]
            # 평가일자는 Title → title 배열에서 텍스트 추출
            title_arr   = props.get("평가일자", {}).get("title", [])
            stored_date = title_arr[0]["plain_text"].strip() if title_arr else ""
            # run_date 미만인 것만 사용
            if stored_date and stored_date < run_date:
                val = props.get("평가액", {}).get("number")
                return float(val) if val is not None else 0.0
        return 0.0
    except Exception as e:
        print(f"  ⚠ 직전평가액 조회 실패 (무시): {e}")
        return 0.0


# ─── 국토교통부 API ───────────────────────────────────────────────────────────
def fetch_apt_trades(lawd_cd, deal_ymd, bldg_type="아파트"):
    params = {
        "serviceKey": PUBLIC_DATA_API_KEY,
        "LAWD_CD":    lawd_cd,
        "DEAL_YMD":   deal_ymd,
        "numOfRows":  "1000",
        "pageNo":     "1",
    }
    api_url = MOLIT_API_URL.get(bldg_type, MOLIT_API_URL["아파트"])
    try:
        resp = requests.get(api_url, params=params, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ⚠ API 호출 실패 ({deal_ymd}): {e}")
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        print(f"  ⚠ XML 파싱 실패 ({deal_ymd}): {e}")
        return []

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
            area_str  = txt("excluUseAr")
            price_str = txt("dealAmount").replace(",", "")
            year      = txt("dealYear")
            month     = txt("dealMonth").zfill(2)
            day       = txt("dealDay").zfill(2)
            if not (area_str and price_str and year and month and day):
                continue
            trades.append({
                "dong":      txt("umdNm"),
                "apt_name":  txt("aptNm"),
                "area":      float(area_str),
                "price_won": int(price_str) * 10000,
                "deal_date": f"{year}-{month}-{day}",
                "floor":     txt("floor"),
            })
        except (ValueError, TypeError):
            continue
    return trades


def get_recent_trades(lawd_cd, dong, area, recent_count, apt_name="", bldg_type="아파트"):
    matched = []
    for ym in get_year_months(SEARCH_MONTHS):
        print(f"  📅 {ym} 조회 중...")
        trades = fetch_apt_trades(lawd_cd, ym, bldg_type)
        for t in trades:
            if dong not in t["dong"]:
                continue
            if abs(t["area"] - area) > AREA_MARGIN:
                continue
            if apt_name and apt_name not in t["apt_name"]:
                continue
            matched.append(t)
        matched.sort(key=lambda x: x["deal_date"], reverse=True)
        if len(matched) >= recent_count:
            break
        time.sleep(0.3)
    return matched[:recent_count]


# ─── 노션 저장 ────────────────────────────────────────────────────────────────
def save_to_real_estate_db(asset_name, trades, avg_price, run_date):
    resp = notion_request(
        "POST",
        f"https://api.notion.com/v1/databases/{DB_REAL_ESTATE}/query",
        json={
            "filter": {
                "and": [
                    {"property": "지번/주소", "title": {"equals": asset_name}},
                    {"property": "거래일자",  "date":  {"equals": run_date}},
                ]
            }
        }
    )
    existing = resp.get("results", [])

    ref_lines = []
    for t in trades:
        price_uk = t["price_won"] // 100_000_000
        price_ck = (t["price_won"] % 100_000_000) // 10_000
        ref_lines.append(
            f"{t['deal_date']} | {t['apt_name']} {t['floor']}층 | "
            f"{t['area']}㎡ | {price_uk}억{price_ck:,}만원"
        )

    properties = {
        "지번/주소": {"title": [{"text": {"content": asset_name}}]},
        "거래일자":  {"date":  {"start": run_date}},
        "거래금액":  {"number": round(avg_price / 1e8, 2)},
        "출처":     {"rich_text": [{"text": {"content": "국토부"}}]},
        "비고":     {"rich_text": [{"text": {"content": "\n".join(ref_lines)[:2000]}}]},
    }

    if existing:
        notion_request("PATCH",
            f"https://api.notion.com/v1/pages/{existing[0]['id']}",
            json={"properties": properties})
        print(f"  ✅ 부동산 실거래가 DB 업데이트: {asset_name}")
    else:
        notion_request("POST", "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": DB_REAL_ESTATE}, "properties": properties})
        print(f"  ✅ 부동산 실거래가 DB 저장: {asset_name}")
    time.sleep(0.4)


def save_to_eval_result_db(asset, current_price, prev_eval, run_date):
    """
    자산평가 결과 DB 저장
    - None 값은 properties에서 아예 제외 (노션 API 400 방지)
    - 평가일자: Title 타입
    - 자산명: rich_text 타입
    """
    asset_name = asset["asset_name"]
    quantity   = asset["quantity"] if asset["quantity"] > 0 else 1
    cost       = asset["unit_price"] * quantity
    eval_amt   = current_price * quantity if current_price is not None else None

    # 중복 체크 (평가일자 Title equals는 동작함)
    resp = notion_request(
        "POST",
        f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query",
        json={
            "filter": {
                "and": [
                    {"property": "평가일자", "rich_text": {"equals": run_date}},
                    {"property": "자산명",   "rich_text": {"equals": asset_name}},
                ]
            }
        }
    )
    existing = resp.get("results", [])

    # 기본 properties (None 없는 것만)
    properties = {
        "평가일자":  {"title":    [{"text": {"content": run_date}}]},
        "자산명":    {"rich_text": [{"text": {"content": asset_name}}]},
        "자산분류":  {"select":   {"name": "부동산"}},
        "수량":      {"number": quantity},
        "금액":      {"number": round(cost)},
    }

    # None이면 아예 넣지 않음
    if current_price is not None:
        properties["현재가"] = {"number": round(current_price)}
    if eval_amt is not None:
        properties["평가액"] = {"number": round(eval_amt)}
    if prev_eval and prev_eval > 0:
        properties["직전평가액"] = {"number": round(prev_eval)}

    if existing:
        notion_request("PATCH",
            f"https://api.notion.com/v1/pages/{existing[0]['id']}",
            json={"properties": properties})
        eval_str = f"{eval_amt:,.0f}원" if eval_amt is not None else "공란"
        print(f"  ✅ 자산평가 결과 DB 업데이트: {asset_name} | 평가액 {eval_str}")
    else:
        notion_request("POST", "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": DB_EVAL_RESULT}, "properties": properties})
        eval_str = f"{eval_amt:,.0f}원" if eval_amt is not None else "공란(실거래 데이터 없음)"
        print(f"  ✅ 자산평가 결과 DB 저장: {asset_name} | 평가액 {eval_str}")
    time.sleep(0.4)


# ─── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    run_date = datetime.now().strftime("%Y-%m-%d")

    print("=" * 60)
    print("🏠 Phase 3: 부동산 실거래가 자동화 시작")
    print(f"   평가일자: {run_date}")
    print("=" * 60)

    print("\n[사전] 자산보유현황 DB에서 부동산 목록 조회")
    assets = get_real_estate_assets()
    if not assets:
        print("  ⚠ 처리할 부동산 자산 없음 — 종료")
        return

    for asset in assets:
        asset_name = asset["asset_name"]
        area       = asset["area"]
        print(f"\n{'=' * 50}")
        print(f"📌 대상: {asset_name} | 전용 {area}㎡")

        addr = parse_address(asset_name)
        if not addr:
            print(f"  ⚠ 주소 파싱 실패 — 건너뜀")
            continue

        lawd_cd  = addr["lawd_cd"]
        dong     = addr["dong"]
        apt_name = asset["apt_name"]
        print(f"   법정동코드: {lawd_cd} | 동명: {dong} | {asset['bldg_type']} | 아파트명: {apt_name if apt_name else '(미입력)'}")

        print(f"\n[1/4] 실거래가 API 조회 (최근 {RECENT_COUNT}건, ±{AREA_MARGIN}㎡)")
        trades = get_recent_trades(lawd_cd, dong, area, RECENT_COUNT, asset["apt_name"], asset["bldg_type"])

        if not trades:
    fallback_price = asset["unit_price"]
    print(f"  ⚠ 실거래 데이터 없음 — 매수가({fallback_price:,.0f}원)로 대체")
    avg_price = float(fallback_price) if fallback_price > 0 else None
        else:
            print(f"  📊 조회된 거래: {len(trades)}건")
            for t in trades:
                price_uk = t["price_won"] // 100_000_000
                price_ck = (t["price_won"] % 100_000_000) // 10_000
                print(f"     {t['deal_date']} | {t['apt_name']} {t['floor']}층 | "
                      f"{t['area']}㎡ | {price_uk}억{price_ck:,}만원")
            avg_price = sum(t["price_won"] for t in trades) / len(trades)
            avg_uk = avg_price // 100_000_000
            avg_ck = (avg_price % 100_000_000) // 10_000
            print(f"\n[2/4] 평균 실거래가: {avg_uk:.0f}억 {avg_ck:,.0f}만원")

        if trades and avg_price is not None:
    print(f"\n[3/4] 부동산 실거래가 DB 저장")
    save_to_real_estate_db(asset_name, trades, avg_price, run_date)
else:
    print(f"\n[3/4] 부동산 실거래가 DB 저장 — 건너뜀 (실거래 없음)")

        print(f"\n[4/4] 자산평가 결과 DB 저장")
        prev_eval = get_prev_eval(asset_name, run_date)
        save_to_eval_result_db(asset, avg_price, prev_eval, run_date)

    print("\n" + "=" * 60)
    print("✅ Phase 3 완료")
    print("=" * 60)


if __name__ == "__main__":
    main()
