"""
pension_etf_automation.py
Phase 6 v5 - 연금ETF/펀드 현재가 조회 및 자산평가결과 DB 저장

[변경이력]
  v5 - v4의 엔드포인트 오류 수정
       주식 API(getStockPriceInfo) → 수익증권 API(getStocksecuritiesPriceInfo)
       GitHub Actions 정상 작동 확인된 apis.data.go.kr 기반 유지
       ISIN 탐색 로직 강화 (itmsNm 포함 풀스캔 fallback 추가)

[공공데이터포털 API 구조 - 금융위원회_주식시세정보 (15094808)]
  ① 주식시세    : /GetStockSecuritiesInfoService/getStockPriceInfo
  ② 수익증권시세: /GetStockSecuritiesInfoService/getStocksecuritiesPriceInfo  ← 펀드용
  ③ ETF시세     : /GetSecuritiesProductInfoService/getETFPriceInfo (별도 데이터셋)

[수익증권(펀드) ISIN 규칙]
  표준코드  → ISIN 변환: KR5 + 표준코드(7자) + 숫자체크디지트(1~3자리)
  예) A0040Y0  → KR5A0040Y008
  ※ 마지막 체크디지트는 펀드마다 다를 수 있으므로 복수 시도 필요

[데이터 갱신]
  T+1 영업일 오후 1시 이후 / 토요일 실행 시 금요일 기준가 조회
"""

import os
import re
import time
import requests
from datetime import datetime, timezone, timedelta

# ── 환경변수 ──────────────────────────────────────────────
NOTION_TOKEN         = os.environ["NOTION_TOKEN"]
DB_ASSET_HOLDINGS    = os.environ["DB_ASSET_HOLDINGS"]
DB_EVAL_RESULT       = os.environ["DB_EVAL_RESULT"]
PUBLIC_DATA_API_KEY  = os.environ["PUBLIC_DATA_API_KEY"]

HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

KST = timezone(timedelta(hours=9))

# 공공데이터포털 기본 URL
DATA_GO_BASE = "https://apis.data.go.kr/1160100/service"

# 수익증권 ISIN 체크디지트 후보 (펀드마다 다름, 순차 시도)
ISIN_SUFFIXES = ["008", "009", "000", "001", "010", "002", "003"]


def get_run_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def is_fund_code(ticker: str) -> bool:
    """A로 시작하는 7자리 → 비상장 펀드 (수익증권 표준코드)"""
    return bool(re.match(r'^[Aa][0-9A-Za-z]{6}$', ticker))


def get_recent_business_days(n: int = 5) -> list[str]:
    """오늘부터 최근 n개 영업일 반환 (YYYYMMDD), 주말 제외"""
    kst_now = datetime.now(KST)
    result = []
    for i in range(n * 3):
        d = kst_now - timedelta(days=i)
        if d.weekday() < 5:
            result.append(d.strftime("%Y%m%d"))
        if len(result) >= n:
            break
    return result


def extract_price_from_response(data: dict) -> float | None:
    """공공데이터포털 표준 응답 구조에서 종가(clpr) 추출"""
    try:
        items = (
            data.get("response", {})
                .get("body", {})
                .get("items", {})
                .get("item", [])
        )
        # 단건 응답은 dict, 복수는 list
        if isinstance(items, dict):
            items = [items]
        if not items:
            return None
        item = items[0]
        # clpr: 종가 (수익증권의 경우 = 기준가 NAV)
        price_raw = item.get("clpr") or item.get("mkp") or item.get("hipr")
        if price_raw:
            price = float(str(price_raw).replace(",", "").strip())
            if price > 0:
                return price
    except Exception:
        pass
    return None


def get_total_count(data: dict) -> int:
    try:
        return int(
            data.get("response", {})
                .get("body", {})
                .get("totalCount", 0)
        )
    except Exception:
        return 0


# ── 1. 자산보유현황 DB에서 연금 항목 조회 ────────────────
def fetch_pension_holdings() -> list[dict]:
    url = f"https://api.notion.com/v1/databases/{DB_ASSET_HOLDINGS}/query"
    payload = {
        "filter": {"property": "자산분류", "select": {"equals": "연금"}}
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    merged: dict[str, dict] = {}
    for page in res.json().get("results", []):
        props = page["properties"]

        name_arr   = props.get("자산명", {}).get("title", [])
        name       = name_arr[0]["plain_text"].strip() if name_arr else ""
        ticker_arr = props.get("티커/코드", {}).get("rich_text", [])
        ticker     = ticker_arr[0]["plain_text"].strip() if ticker_arr else ""
        quantity   = props.get("수량", {}).get("number") or 0
        avg_price  = props.get("금액", {}).get("number") or 0

        if not ticker or quantity <= 0:
            print(f"[Holdings] ⚠️  '{name}' 티커/수량 미입력 - 건너뜀")
            continue

        if ticker in merged:
            prev = merged[ticker]
            total_qty = prev["quantity"] + quantity
            merged[ticker]["quantity"]  = total_qty
            merged[ticker]["avg_price"] = (
                prev["avg_price"] * prev["quantity"] + avg_price * quantity
            ) / total_qty
        else:
            merged[ticker] = {
                "name":      name,
                "ticker":    ticker,
                "quantity":  quantity,
                "avg_price": avg_price,
                "is_fund":   is_fund_code(ticker),
            }

    holdings = list(merged.values())
    etf_cnt  = sum(1 for h in holdings if not h["is_fund"])
    fund_cnt = sum(1 for h in holdings if h["is_fund"])
    print(f"[Holdings] 연금 {len(holdings)}종목 조회 (ETF {etf_cnt}개, 펀드 {fund_cnt}개)")
    return holdings


# ── 2-A. 야후파이낸스 (ETF, 숫자 6자리) ──────────────────
def fetch_yahoo_price(ticker: str) -> float | None:
    yahoo_symbol = f"{ticker}.KS"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
    params = {"interval": "1d", "range": "1d"}

    for attempt in range(3):
        try:
            res = requests.get(
                url, params=params,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            res.raise_for_status()
            meta  = res.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            if price:
                return float(price)
        except Exception as e:
            print(f"[Yahoo] ⚠️  {ticker}.KS 시도 {attempt+1}/3 실패: {e}")
            if attempt < 2:
                time.sleep(3)
    return None


# ── 2-B. 공공데이터포털 수익증권 시세 (펀드 전용) ─────────
def fetch_data_go_fund_price(ticker: str) -> float | None:
    """
    금융위원회_주식시세정보 > ②수익증권시세조회
    엔드포인트: getStocksecuritiesPriceInfo  (★주식용 getStockPriceInfo 아님)

    ISIN 체계:
      수익증권 ISIN = KR5 + 표준코드(7자) + 체크디지트(3자)
      체크디지트는 펀드마다 다름 → 008부터 순차 시도
    """
    url = f"{DATA_GO_BASE}/GetStockSecuritiesInfoService/getStocksecuritiesPriceInfo"
    business_days = get_recent_business_days(5)
    code = ticker.upper()

    # ISIN 후보 목록 생성
    isin_candidates = [f"KR5{code}{sfx}" for sfx in ISIN_SUFFIXES]

    for isin in isin_candidates:
        for date_str in business_days[:3]:  # ISIN당 최근 3영업일만 시도
            try:
                params = {
                    "serviceKey": PUBLIC_DATA_API_KEY,
                    "resultType": "json",
                    "isinCd":     isin,
                    "basDt":      date_str,
                    "numOfRows":  "1",
                    "pageNo":     "1",
                }
                res = requests.get(url, params=params, timeout=10)
                res.raise_for_status()

                if not res.text.strip():
                    time.sleep(0.2)
                    continue

                data = res.json()
                total = get_total_count(data)

                if total > 0:
                    price = extract_price_from_response(data)
                    if price:
                        print(f"[DataGo] ✅ {ticker} (ISIN:{isin}, {date_str}) → {price:,.2f}원")
                        return price

            except Exception as e:
                print(f"[DataGo] ⚠️  {ticker} ISIN:{isin} ({date_str}) 오류: {e}")
            time.sleep(0.2)

    # ISIN 변환 모두 실패 → itmsNm(종목명) 키워드 풀스캔 fallback
    print(f"[DataGo] ⚠️  {ticker} ISIN 매핑 실패 → 종목명 풀스캔 시도")
    return fetch_data_go_fund_by_name(ticker, business_days[0])


def fetch_data_go_fund_by_name(ticker: str, date_str: str) -> float | None:
    """
    ISIN 불명 시: 수익증권 전체 조회 후 표준코드(srtnCd) 매칭
    numOfRows=1000으로 당일 전체 수익증권을 가져와 표준코드로 필터링
    """
    url = f"{DATA_GO_BASE}/GetStockSecuritiesInfoService/getStocksecuritiesPriceInfo"
    code = ticker.upper()

    try:
        params = {
            "serviceKey": PUBLIC_DATA_API_KEY,
            "resultType": "json",
            "basDt":      date_str,
            "numOfRows":  "2000",
            "pageNo":     "1",
        }
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()

        if not res.text.strip():
            return None

        data = res.json()
        items = (
            data.get("response", {})
                .get("body", {})
                .get("items", {})
                .get("item", [])
        )
        if isinstance(items, dict):
            items = [items]

        for item in items:
            # srtnCd(단축코드) 또는 isinCd에 표준코드 포함 여부 확인
            srtn = str(item.get("srtnCd", "")).upper()
            isin = str(item.get("isinCd", "")).upper()
            if code in srtn or code in isin:
                price_raw = item.get("clpr")
                if price_raw:
                    price = float(str(price_raw).replace(",", "").strip())
                    if price > 0:
                        found_isin = item.get("isinCd", "")
                        print(f"[DataGo-Scan] ✅ {ticker} 풀스캔 매칭 (ISIN:{found_isin}) → {price:,.2f}원")
                        return price

        print(f"[DataGo-Scan] ❌ {ticker} 풀스캔에도 매칭 없음 ({date_str})")
    except Exception as e:
        print(f"[DataGo-Scan] ⚠️  {ticker} 풀스캔 오류: {e}")

    return None


# ── 2-C. 공공데이터포털 ETF 시세 fallback (야후 실패시) ──
def fetch_data_go_etf_price(ticker: str) -> float | None:
    """
    금융위원회_증권상품시세정보 > ETF시세조회
    엔드포인트: /GetSecuritiesProductInfoService/getETFPriceInfo
    ETF ISIN: KR7 + 6자리 티커 + 008
    """
    isin = f"KR7{ticker}008"
    business_days = get_recent_business_days(3)
    url = f"{DATA_GO_BASE}/GetSecuritiesProductInfoService/getETFPriceInfo"

    for date_str in business_days:
        try:
            params = {
                "serviceKey": PUBLIC_DATA_API_KEY,
                "resultType": "json",
                "isinCd":     isin,
                "basDt":      date_str,
                "numOfRows":  "1",
            }
            res = requests.get(url, params=params, timeout=10)
            res.raise_for_status()

            if not res.text.strip():
                time.sleep(0.2)
                continue

            data = res.json()
            if get_total_count(data) > 0:
                price = extract_price_from_response(data)
                if price:
                    print(f"[DataGo-ETF] ✅ {ticker} ({date_str}) → {price:,.0f}원")
                    return price
        except Exception as e:
            print(f"[DataGo-ETF] ⚠️  {ticker} 오류 ({date_str}): {e}")
        time.sleep(0.2)

    return None


# ── 2. 가격 조회 통합 (ETF/펀드 분기) ────────────────────
def fetch_price(holding: dict) -> float | None:
    ticker = holding["ticker"]

    if holding["is_fund"]:
        price = fetch_data_go_fund_price(ticker)
    else:
        price = fetch_yahoo_price(ticker)
        if price:
            print(f"[Yahoo] {ticker}.KS → {price:,.0f}원")
        else:
            print(f"[Yahoo] {ticker}.KS 실패 → 공공데이터포털 ETF API fallback")
            price = fetch_data_go_etf_price(ticker)

    time.sleep(0.3)
    return price


# ── 3. 직전 평가액 조회 ───────────────────────────────────
def fetch_prev_eval(asset_name: str, run_date: str) -> float | None:
    try:
        url = f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query"
        payload = {
            "filter": {
                "and": [
                    {"property": "자산분류", "select":    {"equals": "연금"}},
                    {"property": "평가일자", "rich_text": {"is_not_empty": True}},
                ]
            },
            "sorts": [{"property": "평가일자", "direction": "descending"}],
            "page_size": 100,
        }
        res = requests.post(url, headers=HEADERS, json=payload)
        res.raise_for_status()

        for page in res.json().get("results", []):
            props       = page["properties"]
            title_arr   = props.get("평가일자", {}).get("title", [])
            stored_date = title_arr[0]["plain_text"].strip() if title_arr else ""
            if not stored_date or stored_date >= run_date:
                continue
            name_arr    = props.get("자산명", {}).get("rich_text", [])
            stored_name = name_arr[0]["plain_text"].strip() if name_arr else ""
            if stored_name == asset_name:
                val = props.get("평가액", {}).get("number")
                return float(val) if val is not None else None
        return None
    except Exception as e:
        print(f"[PrevEval] ⚠️  {asset_name} 직전평가액 조회 실패 (무시): {e}")
        return None


# ── 4. 자산평가결과 DB 저장 ───────────────────────────────
def save_eval_result(holding: dict, price: float | None, run_date: str) -> None:
    name        = holding["name"]
    quantity    = holding["quantity"]
    avg_price   = holding["avg_price"]
    cost_total  = round(avg_price * quantity)
    eval_amount = round(price * quantity) if price is not None else None
    prev_amount = fetch_prev_eval(name, run_date)

    properties = {
        "평가일자": {"title":     [{"text": {"content": run_date}}]},
        "자산명":   {"rich_text": [{"text": {"content": name}}]},
        "자산분류": {"select":    {"name": "연금"}},
        "수량":     {"number": quantity},
        "금액":     {"number": cost_total},
    }
    if price is not None:
        properties["현재가"] = {"number": price}
    if eval_amount is not None:
        properties["평가액"] = {"number": eval_amount}
    if prev_amount is not None:
        properties["직전평가액"] = {"number": prev_amount}

    url = "https://api.notion.com/v1/pages"
    res = requests.post(url, headers=HEADERS,
                        json={"parent": {"database_id": DB_EVAL_RESULT},
                              "properties": properties})
    res.raise_for_status()

    kind       = "펀드(NAV)" if holding["is_fund"] else "ETF"
    price_str  = f"{price:,.2f}원" if holding["is_fund"] and price else \
                 f"{price:,.0f}원" if price else "조회실패"
    amount_str = f"{eval_amount:,.0f}원" if eval_amount is not None else "-"
    change_str = ""
    if eval_amount and prev_amount:
        change_str = f"  변동: {eval_amount - prev_amount:+,.0f}원"
    print(f"[Notion] {name} [{kind}] 저장완료 | 가격: {price_str} | 평가액: {amount_str}{change_str}")


# ── MAIN ──────────────────────────────────────────────────
def main():
    run_date = get_run_date()
    print(f"\n{'='*55}")
    print(f"[Pension] 실행일(KST): {run_date}")
    print(f"{'='*55}")

    holdings = fetch_pension_holdings()
    if not holdings:
        print("[Pension] 보유 연금 자산 없음 - 종료")
        return

    for h in holdings:
        price = fetch_price(h)
        save_eval_result(h, price, run_date)

    print(f"\n[Pension] 완료 - {len(holdings)}종목 처리")


if __name__ == "__main__":
    main()
