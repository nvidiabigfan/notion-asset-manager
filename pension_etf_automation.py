"""
pension_etf_automation.py
Phase 6 - 연금ETF/펀드 현재가 조회 및 자산평가결과 DB 저장

[티커/코드 입력 규칙]
  ETF  (거래소 상장) : 숫자 6자리   예) 360750, 465580
  펀드 (비상장 수익증권): A로 시작  예) A0040Y0, A441800

[현재가 조회 방법]
  ETF  → 야후파이낸스  (ticker.KS) - GitHub Actions 환경에서 정상 작동
  펀드 → KOFIA freesis.kofia.or.kr POST API (공공기관, IP 차단 없음, API키 불필요)
         └ 기준가(NAV): 전일 장 마감 기준 T+1 공시
         └ 실패시 pykrx 라이브러리로 fallback

[네이버 금융 API 사용 불가 이유]
  GitHub Actions IP(Azure 미국 대역)를 네이버가 봇으로 차단함
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


# ── 2-B. KOFIA freesis API (펀드, A코드) ─────────────────
def fetch_kofia_price(ticker: str) -> float | None:
    """
    금융투자협회 종합통계 포털 비공식 API
    URL: https://freesis.kofia.or.kr/gw/fundPub/FundInfoService/getStdPrcByStdCd
    방식: POST, Content-Type: application/json
    요청: {"standardCd": "A0040Y0", "standardDt": "20260307"}
    응답: {"standardPrice": "1234.56", "standardDt": "20260307", ...}

    - 공공기관 서버 → GitHub Actions IP 차단 없음
    - API 키 불필요
    - 기준일자를 최근 영업일로 자동 탐색 (최대 7일 전까지)
    """
    base_url = "https://freesis.kofia.or.kr/gw/fundPub/FundInfoService/getStdPrcByStdCd"
    kst_now  = datetime.now(KST)

    # 최근 영업일 탐색 (오늘부터 최대 7일 전까지)
    for days_ago in range(7):
        target_dt = kst_now - timedelta(days=days_ago)
        # 주말 건너뜀
        if target_dt.weekday() >= 5:
            continue
        date_str = target_dt.strftime("%Y%m%d")

        try:
            payload = {"standardCd": ticker.upper(), "standardDt": date_str}
            res = requests.post(
                base_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://freesis.kofia.or.kr/",
                },
                timeout=10
            )
            res.raise_for_status()
            data = res.json()

            # 응답 구조 탐색
            price_raw = (
                data.get("standardPrice")
                or data.get("nav")
                or data.get("stdPrc")
                or (data.get("output", {}) or {}).get("standardPrice")
            )
            if price_raw:
                price = float(str(price_raw).replace(",", "").strip())
                print(f"[KOFIA] {ticker} 기준가({date_str}) → {price:,.2f}원")
                return price

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            if status == 404:
                print(f"[KOFIA] ❌ {ticker} 펀드코드 미존재 ({date_str})")
                return None
            print(f"[KOFIA] ⚠️  {ticker} HTTP {status} ({date_str})")
        except Exception as e:
            print(f"[KOFIA] ⚠️  {ticker} 오류 ({date_str}): {e}")

        time.sleep(0.3)

    # KOFIA 실패 시 pykrx fallback
    return fetch_pykrx_fund_price(ticker)


def fetch_pykrx_fund_price(ticker: str) -> float | None:
    """
    pykrx 라이브러리 fallback (KRX 공식 데이터)
    주의: pykrx는 A코드 펀드를 지원하지 않을 수 있음
    """
    try:
        from pykrx import stock
        kst_now = datetime.now(KST)
        # 최근 5영업일 범위로 조회
        to_date   = kst_now.strftime("%Y%m%d")
        from_date = (kst_now - timedelta(days=10)).strftime("%Y%m%d")

        df = stock.get_fund_ohlcv_by_date(from_date, to_date, ticker)
        if df is not None and not df.empty:
            price = float(df["종가"].iloc[-1])
            print(f"[pykrx] {ticker} 기준가 → {price:,.2f}원")
            return price
    except Exception as e:
        print(f"[pykrx] ⚠️  {ticker} 조회 실패: {e}")
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
