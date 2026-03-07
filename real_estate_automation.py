"""
Phase 3: 부동산 실거래가 자동화
- 자산보유현황 DB에서 '부동산' 분류 자산 자동 읽기 (하드코딩 없음)
- 자산명(주소)에서 법정동코드·동명 자동 추출
- 전용면적 컬럼을 자산보유현황 DB에서 읽어서 필터링에 사용
- 국토교통부 API로 직전 5건 실거래가 평균 산출
- 부동산 실거래가 DB + 자산평가 결과 DB에 저장

GitHub Secrets 필요:
  - NOTION_TOKEN
  - PUBLIC_DATA_API_KEY  (공공데이터포털 발급, Decoding Key)

노션 자산보유현황 DB 필요 컬럼:
  - 전용면적 (Number, 단위: ㎡) ← 새로 추가한 컬럼
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
RECENT_COUNT  = 5    # 직전 N개 실거래 평균
SEARCH_MONTHS = 24   # 최대 소급 조회 개월수
AREA_MARGIN   = 0.5  # 면적 허용 오차 (±㎡)

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
    # 충청남도 (천안시는 구 단위로 분리)
    "천안시": "44130", "동남구": "44131", "서북구": "44133", "공주시": "44150", "보령시": "44180",
    "아산시": "44200", "서산시": "44210", "논산시": "44230",
    "계룡시": "44250", "당진시": "44270",
    # 충청북도
    "청주시": "43110", "충주시": "43130", "제천시": "43150",
    # 필요 시 추가
}

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
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


def get_year_months(months_back):
    """현재 월부터 N개월 전까지 YYYYMM 리스트 반환 (최신 순)"""
    now = datetime.now()
    result = []
    for i in range(months_back):
        dt = now - relativedelta(months=i)
        result.append(dt.strftime("%Y%m"))
    return result


def parse_address(asset_name):
    """
    자산명(주소)에서 법정동코드와 동명 자동 추출
    예) "서울시 영등포구 신길동 364"   → lawd_cd="11560", dong="신길동"
    예) "경기도 화성시 병점동 123"     → lawd_cd="41590", dong="병점동"
    예) "충청남도 천안시 동남구 두정동" → lawd_cd="44130", dong="두정동"
    """
    lawd_cd = None
    # 1순위: "구" 단위 키 먼저 매칭 (동남구, 서북구 등 시보다 구체적)
    for sigungu, code in LAWD_CD_MAP.items():
        if sigungu.endswith("구") and sigungu in asset_name:
            lawd_cd = code
            break
    # 2순위: 구 단위 없으면 시/군 단위로 매칭
    if not lawd_cd:
        for sigungu, code in LAWD_CD_MAP.items():
            if not sigungu.endswith("구") and sigungu in asset_name:
                lawd_cd = code
                break

    if not lawd_cd:
        print(f"  ⚠ 법정동코드 매핑 실패: '{asset_name}' — LAWD_CD_MAP에 시군구 추가 필요")
        return {}

    # 동/읍/면 추출 (가장 마지막에 등장하는 것 사용)
    matches = re.findall(r'(\S+(?:동|읍|면))', asset_name)
    if not matches:
        print(f"  ⚠ 동명 추출 실패: '{asset_name}'")
        return {}

    dong = matches[-1]
    return {"lawd_cd": lawd_cd, "dong": dong}


# ─── 자산보유현황 DB 조회 ──────────────────────────────────────────────────────
def get_real_estate_assets():
    """
    자산보유현황 DB에서 '부동산' 분류 자산 전체 자동 조회
    전용면적 컬럼값을 읽어서 반환
    """
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
    results = resp.get("results", [])
    assets = []

    for page in results:
        props = page["properties"]

        def num(key):
            v = props.get(key, {}).get("number")
            return v if v is not None else 0

        title_items = props.get("자산명", {}).get("title", [])
        asset_name = title_items[0]["text"]["content"] if title_items else ""

        if not asset_name:
            continue

        area = num("전용면적")
        if area <= 0:
            print(f"  ⚠ '{asset_name}' — 전용면적 미입력, 건너뜀")
            continue

        # 아파트명 (Text 컬럼)
        apt_items = props.get("아파트명", {}).get("rich_text", [])
        apt_name = apt_items[0]["text"]["content"] if apt_items else ""

        # 건물유형 (Select 컬럼: 아파트 / 오피스텔)
        bldg_type = props.get("건물유형", {}).get("select", {}).get("name", "아파트")

        assets.append({
            "asset_name": asset_name,
            "quantity":   num("수량"),
            "unit_price": num("금액"),
            "area":       area,
            "apt_name":   apt_name,
            "bldg_type":  bldg_type,  # 아파트 / 오피스텔
        })

    print(f"  📋 부동산 자산 {len(assets)}건 조회됨")
    return assets


def get_prev_eval(asset_name):
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
    return results[0]["properties"].get("평가액", {}).get("number") or 0.0


# ─── 국토교통부 API ───────────────────────────────────────────────────────────
def fetch_apt_trades(lawd_cd, deal_ymd, bldg_type="아파트"):
    """특정 지역/년월의 아파트/오피스텔 매매 실거래가 XML 조회"""
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
    """동 + 면적 + 아파트명 조건으로 필터링한 최근 N건 실거래 반환"""
    matched = []
    for ym in get_year_months(SEARCH_MONTHS):
        print(f"  📅 {ym} 조회 중...")
        trades = fetch_apt_trades(lawd_cd, ym, bldg_type)

        for t in trades:
            if dong not in t["dong"]:
                continue
            if abs(t["area"] - area) > AREA_MARGIN:
                continue
            # 아파트명 필터 (입력된 경우만 적용)
            if apt_name and apt_name not in t["apt_name"]:
                continue
            matched.append(t)

        matched.sort(key=lambda x: x["deal_date"], reverse=True)
        if len(matched) >= recent_count:
            break

        time.sleep(0.3)

    return matched[:recent_count]


# ─── 노션 저장 ────────────────────────────────────────────────────────────────
def save_to_real_estate_db(asset_name, trades, avg_price):
    """부동산 실거래가 DB 저장 (오늘 날짜 기준 upsert)"""
    today_str = datetime.now().strftime("%Y-%m-%d")

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
        "거래일자":  {"date": {"start": today_str}},
        "거래금액":  {"number": round(avg_price)},
        "출처":     {"rich_text": [{"text": {"content": "국토부"}}]},
        "비고":     {"rich_text": [{"text": {"content": "\n".join(ref_lines)[:2000]}}]},
    }

    if existing:
        notion_request(
            "PATCH",
            f"https://api.notion.com/v1/pages/{existing[0]['id']}",
            json={"properties": properties}
        )
        print(f"  ✅ 부동산 실거래가 DB 업데이트: {asset_name}")
    else:
        notion_request(
            "POST",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": DB_REAL_ESTATE}, "properties": properties}
        )
        print(f"  ✅ 부동산 실거래가 DB 저장: {asset_name}")

    time.sleep(0.4)


def save_to_eval_result_db(asset, current_price, prev_eval):
    """자산평가 결과 DB 저장 - current_price=None이면 현재가/평가액 공란"""
    today_str  = datetime.now().strftime("%Y-%m-%d")
    asset_name = asset["asset_name"]
    quantity   = asset["quantity"] if asset["quantity"] > 0 else 1
    cost       = asset["unit_price"] * quantity
    eval_amt   = current_price * quantity if current_price is not None else None

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
        "평가일자":   {"title": [{"text": {"content": today_str}}]},
        "자산명":     {"rich_text": [{"text": {"content": asset_name}}]},
        "자산분류":   {"select": {"name": "부동산"}},
        "수량":       {"number": quantity},
        "금액":       {"number": round(cost)},
        "현재가":     {"number": round(current_price) if current_price is not None else None},
        "평가액":     {"number": round(eval_amt) if eval_amt is not None else None},
        "직전평가액": {"number": round(prev_eval)},
    }

    if existing:
        notion_request(
            "PATCH",
            f"https://api.notion.com/v1/pages/{existing[0]['id']}",
            json={"properties": properties}
        )
        eval_str = f"{eval_amt:,.0f}원" if eval_amt is not None else "공란"
        print(f"  ✅ 자산평가 결과 DB 업데이트: {asset_name} | 평가액 {eval_str}")
    else:
        notion_request(
            "POST",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": DB_EVAL_RESULT}, "properties": properties}
        )
        eval_str = f"{eval_amt:,.0f}원" if eval_amt is not None else "공란(실거래 데이터 없음)"
        print(f"  ✅ 자산평가 결과 DB 저장: {asset_name} | 평가액 {eval_str}")

    time.sleep(0.4)


# ─── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("🏠 Phase 3: 부동산 실거래가 자동화 시작")
    print(f"   실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S KST')}")
    print("=" * 60)

    # 자산보유현황 DB에서 부동산 목록 자동 조회
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

        # 주소에서 법정동코드·동명 자동 추출
        addr = parse_address(asset_name)
        if not addr:
            print(f"  ⚠ 주소 파싱 실패 — 건너뜀")
            continue

        lawd_cd = addr["lawd_cd"]
        dong    = addr["dong"]
        apt_name = asset["apt_name"]
        print(f"   법정동코드: {lawd_cd} | 동명: {dong} | {asset['bldg_type']} | 아파트명: {apt_name if apt_name else '(미입력)'}")

        # [1/4] 실거래가 API 조회
        print(f"\n[1/4] 실거래가 API 조회 (최근 {RECENT_COUNT}건, ±{AREA_MARGIN}㎡)")
        trades = get_recent_trades(lawd_cd, dong, area, RECENT_COUNT, asset["apt_name"], asset["bldg_type"])

        if not trades:
            print(f"  ⚠ 실거래 데이터 없음 — 현재가/평가액 공란으로 행 생성")
            avg_price = None
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

        # [3/4] 부동산 실거래가 DB 저장 (실거래 있을 때만)
        if trades and avg_price is not None:
            print(f"\n[3/4] 부동산 실거래가 DB 저장")
            save_to_real_estate_db(asset_name, trades, avg_price)
        else:
            print(f"\n[3/4] 부동산 실거래가 DB 저장 — 건너뜀 (데이터 없음)")

        # [4/4] 자산평가 결과 DB 저장 (항상 실행 - 공란이라도 행 생성)
        print(f"\n[4/4] 자산평가 결과 DB 저장")
        prev_eval = get_prev_eval(asset_name)
        save_to_eval_result_db(asset, avg_price, prev_eval)

    print("\n" + "=" * 60)
    print("✅ Phase 3 완료")
    print("=" * 60)


if __name__ == "__main__":
    main()
