"""
Phase 3: 부동산 실거래가 자동화 (v6 - API 오류 구분 + 수동시세)

수정 내역:
  1. 자산평가 결과 저장을 eval_result 모듈로 일원화 (평가일자 + 보유ID 키)
  2. 실거래가 조회 실패 시 매수가 폴백에 '시가미반영' 플래그를 함께 기록.
     이전에는 폴백 값이 시가와 구분 없이 합산돼 전체 수익률을 왜곡했다.
  3. 실행일을 KST로 통일 (UTC 러너의 날짜가 들어가 하루 어긋날 수 있었음)
  4. 항목별 실패를 ErrorTracker에 모아 한 건이라도 있으면 exit(1)
  5. 'API가 막힘'과 '해당 면적의 실거래가 없음'을 구분한다.
     이전에는 둘 다 빈 리스트여서, 서비스키가 승인되지 않았어도
     '실거래 없음'으로 보이고 매수가 폴백이 조용히 들어갔다.
     구분한 사유는 실행 로그와 부동산 실거래가 DB '출처'에 남는다.
  6. 인증·권한 오류는 한 번 뜨면 그 실행의 남은 조회를 전부 중단한다.
     승인 안 난 키로 5개 자산 × 24개월 = 120회를 기다릴 이유가 없다.
  7. 자산보유현황에 '수동시세'(number)가 입력돼 있으면 API를 건너뛰고
     그 값을 쓴다. API가 계속 막히면 분기 1회 수기 갱신으로 운영한다.

  주의: 실거래가 조회 실패는 여기서 exit(1)로 만들지 않는다.
  부동산 API 하나 때문에 암호화폐·요약·대시보드까지 멈추면 안 된다.
  대신 매수가 폴백에 '시가미반영'을 남기고, 그것이 3주 연속이면
  reconcile.py가 워크플로우를 실패시킨다.
"""

import os
import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

import eval_result

KST = timezone(timedelta(hours=9))


# ─── 환경변수 ─────────────────────────────────────────────────────────────────
NOTION_TOKEN        = os.environ["NOTION_TOKEN"]
PUBLIC_DATA_API_KEY = os.environ["PUBLIC_DATA_API_KEY"]

# ─── 노션 DB ID ───────────────────────────────────────────────────────────────
DB_ASSET_STATUS = "31a64e13bb46807b8673e94e7b416f34"  # 자산보유현황
DB_REAL_ESTATE  = "31a64e13bb4680c18668eec357e11222"  # 부동산 실거래가

# ─── 실거래가 조회 공통 설정 ──────────────────────────────────────────────────
RECENT_COUNT  = 5
SEARCH_MONTHS = 24
AREA_MARGIN   = 0.5

# ─── 국토교통부 API 인증·권한 오류 (재시도해도 같은 결과) ────────────────────
FATAL_API_CODES = {
    "20": "서비스 접근 거부",
    "22": "요청 한도 초과",
    "30": "등록되지 않은 서비스키",
    "31": "활용기간 만료",
    "32": "등록되지 않은 IP",
}

# 응답이 XML이 아닐 때(HTML 오류 페이지) 본문에서 찾아볼 표식
FATAL_API_MARKERS = (
    "SERVICE_KEY_IS_NOT_REGISTERED",
    "SERVICE_ACCESS_DENIED",
    "LIMITED_NUMBER_OF_SERVICE_REQUESTS",
    "DEADLINE_HAS_EXPIRED",
    "UNREGISTERED_IP",
)

# 인증 오류를 한 번 만나면 이 실행에서는 더 이상 API를 호출하지 않는다.
_api_blocked: str | None = None

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
    # 실행일과 같은 KST 기준. naive datetime.now()는 UTC 러너 시각이라
    # 매월 1일 09:00 KST 이전에는 한 달 전 목록이 나왔다.
    now = datetime.now(KST)
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

        owner_items = props.get("보유자", {}).get("rich_text", [])
        owner       = owner_items[0]["plain_text"].strip() if owner_items else ""

        assets.append({
            "id":         page["id"],
            "asset_name": asset_name,
            "quantity":   num("수량"),
            "unit_price": num("금액"),
            "area":       area,
            "apt_name":   apt_name,
            "bldg_type":  bldg_type,
            "owner":      owner,
            # 선택 속성. 값이 있으면 API를 건너뛰고 이 값을 시세로 쓴다.
            # 속성 자체가 없으면 num()이 0을 돌려주므로 그대로 무시된다.
            "manual":     num("수동시세"),
        })

    print(f"  📋 부동산 자산 {len(assets)}건 조회됨")
    return assets




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

    global _api_blocked
    if _api_blocked:
        return [], _api_blocked

    try:
        resp = requests.get(api_url, params=params, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        reason = f"API 호출 실패: {e}"
        print(f"  ⚠ {reason} ({deal_ymd})")
        return [], reason

    # data.go.kr은 키가 거부되면 XML 대신 HTML 오류 페이지를 주기도 한다.
    # 그 페이지가 우연히 well-formed XML이면 파싱은 성공하고 resultCode만
    # 비어 있어서 '거래 없음'으로 오인됐다. 파싱 전에 본문부터 본다.
    for marker in FATAL_API_MARKERS:
        if marker in resp.text:
            _api_blocked = f"국토부 API 인증 오류: {marker}"
            print(f"  ⚠ {_api_blocked} — 이번 실행의 남은 조회를 중단합니다")
            return [], _api_blocked

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        reason = f"XML 파싱 실패: {e}"
        print(f"  ⚠ {reason} ({deal_ymd})")
        return [], reason

    result_code = (root.findtext(".//resultCode")
                   or root.findtext(".//returnReasonCode") or "").strip()
    result_msg  = (root.findtext(".//resultMsg")
                   or root.findtext(".//returnAuthMsg")
                   or root.findtext(".//errMsg") or "").strip()

    if not result_code:
        # 정상 응답이라면 header에 resultCode가 반드시 있다.
        # 없다면 우리가 기대한 응답이 아니므로 '거래 없음'으로 넘기지 않는다.
        reason = "응답에 resultCode가 없습니다 (예상과 다른 형식)"
        print(f"  ⚠ {reason} ({deal_ymd})")
        return [], reason

    if result_code not in ("00", "000", "0000"):
        try:
            norm = str(int(result_code))      # '030' → '30'
        except ValueError:
            norm = result_code
        if norm in FATAL_API_CODES:
            _api_blocked = (f"국토부 API 인증 오류 {result_code} "
                            f"({FATAL_API_CODES[norm]}) {result_msg}".strip())
            print(f"  ⚠ {_api_blocked} — 이번 실행의 남은 조회를 중단합니다")
            return [], _api_blocked
        reason = f"API 오류 {result_code} {result_msg}".strip()
        print(f"  ⚠ {reason} ({deal_ymd})")
        return [], reason

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
    return trades, None


def get_recent_trades(lawd_cd, dong, area, recent_count, apt_name="", bldg_type="아파트"):
    """반환: (조건에 맞는 거래목록, 조회 중 발생한 오류 사유 목록)"""
    matched = []
    errors: list[str] = []
    for ym in get_year_months(SEARCH_MONTHS):
        if _api_blocked:
            errors.append(_api_blocked)
            break
        print(f"  📅 {ym} 조회 중...")
        trades, err = fetch_apt_trades(lawd_cd, ym, bldg_type)
        if err and err not in errors:
            errors.append(err)
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
    return matched[:recent_count], errors


# ─── 노션 저장 ────────────────────────────────────────────────────────────────
def save_to_real_estate_db(asset_name, trades, avg_price, run_date, source=None):
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
        "출처":     {"rich_text": [{"text": {"content": (source or ("국토부" if trades else "매수가(실거래없음)"))[:200]}}]},
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




def save_eval(asset, label, owner, asset_name, avg_price,
              stale, run_date, available, tracker):
    """자산평가 결과 DB 저장. 수동시세 경로와 API 경로가 함께 쓴다."""
    print("\n[4/4] 자산평가 결과 DB 저장")
    quantity = asset["quantity"] if asset["quantity"] > 0 else 1
    cost     = asset["unit_price"] * quantity
    eval_amt = avg_price * quantity if avg_price is not None else None

    try:
        prev_eval = eval_result.fetch_prev_eval(asset["id"], run_date)
        action = eval_result.upsert(
            holding_id=asset["id"],
            owner=owner,
            asset_name=asset_name,
            category="부동산",
            run_date=run_date,
            quantity=quantity,
            unit_price=avg_price,
            eval_amount=eval_amt,
            purchase_amount=cost,
            prev_eval_amount=prev_eval,
            stale_price=stale,
            available=available,
        )
        eval_str = f"{eval_amt:,.0f}원" if eval_amt is not None else "공란"
        flag = " [시가미반영]" if stale else ""
        print(f"  ✅ 자산평가 결과 DB {action}: {label} | 평가액 {eval_str}{flag}")
    except Exception as e:
        tracker.record(f"{label} 자산평가 결과 저장", e)


# ─── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    # 실행일은 KST 기준으로 통일한다. 이전에는 datetime.now()를 써서
    # UTC 러너의 날짜가 들어갔고, 스케줄이 밀려 15:00 UTC를 넘기면
    # 주식·암호화폐와 평가일자가 하루 어긋날 수 있었다.
    run_date = datetime.now(KST).strftime("%Y-%m-%d")

    print("=" * 60)
    print("🏠 Phase 3: 부동산 실거래가 자동화 시작")
    print(f"   평가일자: {run_date} (KST)")
    print("=" * 60)

    tracker   = eval_result.ErrorTracker("부동산")
    available = eval_result.require_schema()

    print("\n[사전] 자산보유현황 DB에서 부동산 목록 조회")
    assets = get_real_estate_assets()
    if not assets:
        print("  ⚠ 처리할 부동산 자산 없음 — 종료")
        return

    stale_count    = 0   # 매수가 폴백 건수
    api_fail_count = 0   # 그중 API 오류가 원인인 건수
    manual_count   = 0   # 수동시세를 쓴 건수

    for asset in assets:
        asset_name = asset["asset_name"]
        area       = asset["area"]
        owner      = asset["owner"]
        label      = f"{owner or '미분류'}/{asset_name}"
        print(f"\n{'=' * 50}")
        print(f"📌 대상: {label} | 전용 {area}㎡")

        # 수동시세가 입력돼 있으면 API를 아예 부르지 않는다.
        # API가 계속 막히는 동안 분기 1회 수기 갱신으로 운영하기 위한 경로다.
        if asset["manual"] and asset["manual"] > 0:
            avg_price = float(asset["manual"])
            trades    = []
            stale     = False
            source    = "수동시세"
            print(f"\n[1/4] 수동시세 사용 — {avg_price:,.0f}원 (API 조회 생략)")
            print(f"[2/4] 시세: {avg_price // 100_000_000:.0f}억 "
                  f"{(avg_price % 100_000_000) // 10_000:,.0f}만원")
            manual_count += 1
            print("\n[3/4] 부동산 실거래가 DB 저장 (수동시세)")
            try:
                save_to_real_estate_db(asset_name, [], avg_price, run_date, source)
            except Exception as e:
                tracker.record(f"{label} 실거래가 DB 저장", e)
            save_eval(asset, label, owner, asset_name, avg_price,
                      stale, run_date, available, tracker)
            continue

        addr = parse_address(asset_name)
        if not addr:
            tracker.record(f"{label} 주소 파싱", "법정동코드 매핑 실패")
            continue

        lawd_cd  = addr["lawd_cd"]
        dong     = addr["dong"]
        apt_name = asset["apt_name"]
        print(f"   법정동코드: {lawd_cd} | 동명: {dong} | {asset['bldg_type']} | 아파트명: {apt_name if apt_name else '(미입력)'}")

        print(f"\n[1/4] 실거래가 API 조회 (최근 {RECENT_COUNT}건, ±{AREA_MARGIN}㎡)")
        api_errors: list[str] = []
        try:
            trades, api_errors = get_recent_trades(
                lawd_cd, dong, area, RECENT_COUNT,
                asset["apt_name"], asset["bldg_type"])
        except Exception as e:
            api_errors = [f"예외: {e}"]
            trades = []

        # 실거래가를 못 구하면 매수가로 채우되, 그 사실을 '시가미반영'으로 남긴다.
        # 이전에는 폴백 값이 시가와 구분 없이 합산돼 전체 수익률을 왜곡했다.
        #
        # 여기서 exit(1)을 내지 않는 것은 의도적이다. 부동산 API 하나 때문에
        # 뒤의 암호화폐·요약·대시보드가 통째로 멈추면 손해가 더 크다.
        # 3주 연속 폴백이면 reconcile.py가 워크플로우를 실패시킨다.
        stale = False
        if not trades:
            fallback_price = asset["unit_price"]
            if api_errors:
                source = f"매수가(API오류: {api_errors[0]})"
                api_fail_count += 1
                print(f"  ⚠ 실거래가 조회 실패 — {api_errors[0]}")
            else:
                source = "매수가(실거래없음)"
                print(f"  ⚠ 조건에 맞는 실거래 없음 (최근 {SEARCH_MONTHS}개월, "
                      f"{dong} {area}㎡ ±{AREA_MARGIN})")
            print(f"  → 매수가({fallback_price:,.0f}원)로 대체 [시가미반영]")
            avg_price = float(fallback_price) if fallback_price > 0 else None
            stale = True
            stale_count += 1
        else:
            source = "국토부"
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
            print("\n[3/4] 부동산 실거래가 DB 저장")
            try:
                save_to_real_estate_db(asset_name, trades, avg_price, run_date, source)
            except Exception as e:
                tracker.record(f"{label} 실거래가 DB 저장", e)
        elif avg_price is not None:
            print("\n[3/4] 부동산 실거래가 DB 저장 (매수가 대체)")
            try:
                save_to_real_estate_db(asset_name, [], avg_price, run_date, source)
            except Exception as e:
                tracker.record(f"{label} 실거래가 DB 저장", e)
        else:
            print("\n[3/4] 부동산 실거래가 DB 저장 — 건너뜀 (데이터 없음)")

        save_eval(asset, label, owner, asset_name, avg_price,
                  stale, run_date, available, tracker)

    print("\n" + "=" * 60)
    print(f"✅ Phase 3 완료 — 부동산 {len(assets)}건")
    print(f"   시가미반영 {stale_count}건 (그중 API 오류 {api_fail_count}건) / "
          f"수동시세 {manual_count}건")
    if _api_blocked:
        print(f"   ⚠ 국토부 API가 막혀 있습니다: {_api_blocked}")
        print("     서비스키 승인 상태를 data.go.kr 마이페이지에서 확인하거나,")
        print("     자산보유현황에 '수동시세'(number)를 넣어 수기 운영으로 전환하세요.")
    print("=" * 60)

    tracker.exit_if_any()


if __name__ == "__main__":
    main()