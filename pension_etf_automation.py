"""
pension_etf_automation.py
Phase 6 - 연금ETF/펀드 현재가 조회 및 자산평가결과 DB 저장

[티커/코드 입력 규칙]
  ETF  (거래소 상장) : 숫자 6자리   예) 360750, 465580
  펀드 (비상장 수익증권): A로 시작  예) A0040Y0, A441800

[현재가 조회 방법]
  ETF  → 야후파이낸스  (ticker.KS)
  펀드 → 네이버 금융 모바일 API  (m.stock.naver.com/api/fund/{code}/basic)
         └ 기준가(NAV): 전일 장 마감 기준 T+1 공시
"""

import os
import re
import time
import requests
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

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://m.stock.naver.com/",
    "Accept": "application/json, text/plain, */*",
}


def get_run_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def is_fund_code(ticker: str) -> bool:
    """A로 시작하는 7자리 → 비상장 펀드 (수익증권 표준코드)"""
    return bool(re.match(r'^[Aa][0-9A-Za-z]{6}$', ticker))


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


# ── 2-A. 야후파이낸스 (ETF) ───────────────────────────────
def fetch_yahoo_price(ticker: str) -> float | None:
    """숫자 6자리 ETF → Yahoo Finance .KS"""
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


# ── 2-B. 네이버 금융 모바일 API (펀드) ────────────────────
def fetch_naver_fund_price(ticker: str) -> float | None:
    """
    A코드 펀드 → 네이버 금융 모바일 API
    엔드포인트: https://m.stock.naver.com/api/fund/{fundCode}/basic
    응답 키: nav (기준가, 전일 장 마감 기준 T+1 공시)
    """
    url = f"https://m.stock.naver.com/api/fund/{ticker}/basic"

    for attempt in range(3):
        try:
            res = requests.get(url, headers=BROWSER_HEADERS, timeout=10)
            res.raise_for_status()
            data = res.json()

            # nav 필드: 기준가 (문자열 "1,234.56" 또는 숫자)
            nav_raw = (
                data.get("nav")
                or data.get("standardPrice")
                or data.get("currentPrice")
            )
            if nav_raw is not None:
                # 콤마/공백 제거 후 float 변환
                price = float(str(nav_raw).replace(",", "").strip())
                print(f"[Naver] {ticker} 기준가 → {price:,.2f}원")
                return price
            else:
                print(f"[Naver] ⚠️  {ticker} 응답에 기준가 키 없음: {list(data.keys())}")
                return None

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            print(f"[Naver] ⚠️  {ticker} HTTP {status} 시도 {attempt+1}/3")
            if status == 404:
                print(f"[Naver] ❌ {ticker} 펀드코드 미등록 - 건너뜀")
                return None
            if attempt < 2:
                time.sleep(3)
        except Exception as e:
            print(f"[Naver] ⚠️  {ticker} 시도 {attempt+1}/3 실패: {e}")
            if attempt < 2:
                time.sleep(3)

    return None


# ── 2. 가격 조회 (ETF/펀드 분기) ─────────────────────────
def fetch_price(holding: dict) -> float | None:
    ticker = holding["ticker"]
    if holding["is_fund"]:
        price = fetch_naver_fund_price(ticker)
    else:
        price = fetch_yahoo_price(ticker)
        if price:
            print(f"[Yahoo] {ticker}.KS → {price:,.0f}원")
    time.sleep(0.5)
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

    kind        = "펀드(NAV)" if holding["is_fund"] else "ETF"
    price_str   = f"{price:,.2f}원" if holding["is_fund"] and price else \
                  f"{price:,.0f}원" if price else "조회실패"
    amount_str  = f"{eval_amount:,.0f}원" if eval_amount is not None else "-"
    change_str  = ""
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
