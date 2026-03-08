"""
pension_etf_automation.py
Phase 6 v3 - 연금ETF/펀드 현재가 조회 및 자산평가결과 DB 저장

[변경이력]
  v3 - KOFIA API 완전 재설계
       dis.kofia.or.kr (서비스명 오류로 잘린 XML 반환) 제거
       → 검증된 3단계 fallback 구조
         1순위: KOFIA 공식 selFundStdPrc (form POST)
         2순위: KRX 정보데이터시스템 (MDCSTAT04401)
         3순위: 금융감독원 efund 포털

[티커/코드 입력 규칙]
  ETF  (거래소 상장) : 숫자 6자리   예) 360750, 465580
  펀드 (비상장 수익증권): A로 시작  예) A0040Y0, A441800

[현재가 조회 방법]
  ETF  → 야후파이낸스 (ticker.KS)
  펀드 → 3단계 fallback chain (KOFIA → KRX → FSS)
"""

import os
import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

# ── 환경변수 ──────────────────────────────────────────────
NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
DB_ASSET_HOLDINGS = os.environ["DB_ASSET_HOLDINGS"]
DB_EVAL_RESULT    = os.environ["DB_EVAL_RESULT"]

HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

KST = timezone(timedelta(hours=9))


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


# ══════════════════════════════════════════════════════════
# 펀드 NAV 조회 — 3단계 fallback chain
# ══════════════════════════════════════════════════════════

def _parse_price_from_response(res: requests.Response) -> float | None:
    """응답에서 가격 추출 (JSON/XML 자동 판별)"""
    body = res.text.strip()
    if not body:
        return None

    # JSON 시도
    try:
        data = res.json()
        if isinstance(data, dict):
            # 단일값 구조
            for key in ["standardPrice", "stdPrc", "nav", "basePrc", "BAS_PRC"]:
                val = data.get(key)
                if val:
                    price = float(str(val).replace(",", "").strip())
                    if price > 0:
                        return price
            # 리스트 구조
            for list_key in ["list", "items", "output", "OutBlock_1", "result"]:
                items = data.get(list_key)
                if isinstance(items, list) and items:
                    row = items[0]
                    for key in ["standardPrice", "stdPrc", "nav", "basePrc", "BAS_PRC"]:
                        val = row.get(key)
                        if val:
                            price = float(str(val).replace(",", "").strip())
                            if price > 0:
                                return price
                elif isinstance(items, dict):
                    for key in ["standardPrice", "stdPrc"]:
                        val = items.get(key)
                        if val:
                            price = float(str(val).replace(",", "").strip())
                            if price > 0:
                                return price
    except (ValueError, TypeError):
        pass

    # XML 시도
    try:
        root = ET.fromstring(body)
        for tag in ["standardPrice", "stdPrc", "nav", "basePrc", "uOriginalAmt"]:
            node = root.find(f".//{tag}")
            if node is not None and node.text:
                price = float(node.text.replace(",", "").strip())
                if price > 0:
                    return price
    except ET.ParseError:
        pass

    return None


# ── 2-B-1. KOFIA 공식 API (1순위) ────────────────────────
def fetch_kofia_price_v1(ticker: str) -> float | None:
    """
    금융투자협회 공식 펀드 기준가 API
    POST https://www.kofia.or.kr/biz/fund/sttus/selFundStdPrc.do
    """
    business_days = get_recent_business_days(5)
    url = "https://www.kofia.or.kr/biz/fund/sttus/selFundStdPrc.do"

    for date_str in business_days:
        try:
            res = requests.post(
                url,
                data={"standardCd": ticker.upper(), "standardDt": date_str},
                headers={
                    "Content-Type":      "application/x-www-form-urlencoded; charset=UTF-8",
                    "User-Agent":        "Mozilla/5.0",
                    "Referer":           "https://www.kofia.or.kr/",
                    "X-Requested-With":  "XMLHttpRequest",
                },
                timeout=10
            )
            price = _parse_price_from_response(res)
            if price:
                print(f"[KOFIA-1] ✅ {ticker} ({date_str}) → {price:,.2f}원")
                return price
            print(f"[KOFIA-1] ⚠️  {ticker} 가격 없음 ({date_str})")
        except Exception as e:
            print(f"[KOFIA-1] ⚠️  {ticker} 오류 ({date_str}): {e}")
        time.sleep(0.3)

    return None


# ── 2-B-2. KRX 정보데이터시스템 (2순위) ──────────────────
def fetch_krx_fund_price(ticker: str) -> float | None:
    """
    KRX 정보데이터시스템 펀드 기준가
    POST https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd
    bld: dbms/MDC/STAT/standard/MDCSTAT04401
    """
    fund_id = ticker.upper().lstrip("A")
    business_days = get_recent_business_days(5)
    url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    for date_str in business_days:
        try:
            res = requests.post(
                url,
                data={
                    "bld":           "dbms/MDC/STAT/standard/MDCSTAT04401",
                    "fundId":        fund_id,
                    "trdDd":         date_str,
                    "share":         "1",
                    "money":         "1",
                    "csvxls_isNo":   "false",
                },
                headers={
                    "Content-Type":      "application/x-www-form-urlencoded; charset=UTF-8",
                    "User-Agent":        "Mozilla/5.0",
                    "Referer":           "https://data.krx.co.kr/",
                    "X-Requested-With":  "XMLHttpRequest",
                },
                timeout=10
            )
            price = _parse_price_from_response(res)
            if price:
                print(f"[KRX] ✅ {ticker} ({date_str}) → {price:,.2f}원")
                return price
            print(f"[KRX] ⚠️  {ticker} 가격 없음 ({date_str})")
        except Exception as e:
            print(f"[KRX] ⚠️  {ticker} 오류 ({date_str}): {e}")
        time.sleep(0.3)

    return None


# ── 2-B-3. 금융감독원 efund (3순위) ──────────────────────
def fetch_fss_fund_price(ticker: str) -> float | None:
    """
    금융감독원 전자공시 펀드 기준가
    POST https://efund.fss.or.kr/pkt/aio/mnutPriceList.do
    """
    business_days = get_recent_business_days(5)
    url = "https://efund.fss.or.kr/pkt/aio/mnutPriceList.do"

    for date_str in business_days:
        fmt_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        try:
            res = requests.post(
                url,
                data={"fundCd": ticker.upper(), "srchDt": fmt_date,
                      "pageIndex": "1", "pageSize": "1"},
                headers={
                    "Content-Type":      "application/x-www-form-urlencoded; charset=UTF-8",
                    "User-Agent":        "Mozilla/5.0",
                    "Referer":           "https://efund.fss.or.kr/",
                    "X-Requested-With":  "XMLHttpRequest",
                },
                timeout=10
            )
            price = _parse_price_from_response(res)
            if price:
                print(f"[FSS] ✅ {ticker} ({date_str}) → {price:,.2f}원")
                return price
            print(f"[FSS] ⚠️  {ticker} 가격 없음 ({date_str})")
        except Exception as e:
            print(f"[FSS] ⚠️  {ticker} 오류 ({date_str}): {e}")
        time.sleep(0.3)

    return None


# ── 2-B. 펀드 NAV — fallback chain 통합 ──────────────────
def fetch_kofia_price(ticker: str) -> float | None:
    price = fetch_kofia_price_v1(ticker)
    if price:
        return price

    print(f"[Fallback] {ticker} KOFIA 실패 → KRX 시도")
    price = fetch_krx_fund_price(ticker)
    if price:
        return price

    print(f"[Fallback] {ticker} KRX 실패 → FSS 시도")
    price = fetch_fss_fund_price(ticker)
    if price:
        return price

    print(f"[Fallback] ❌ {ticker} 모든 경로 실패")
    return None


# ── 2. 가격 조회 통합 (ETF/펀드 분기) ────────────────────
def fetch_price(holding: dict) -> float | None:
    ticker = holding["ticker"]
    if holding["is_fund"]:
        price = fetch_kofia_price(ticker)
    else:
        price = fetch_yahoo_price(ticker)
        if price:
            print(f"[Yahoo] {ticker}.KS → {price:,.0f}원")
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
