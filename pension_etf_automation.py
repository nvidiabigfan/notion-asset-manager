"""
pension_etf_automation.py
Phase 6 - 연금ETF 현재가 조회 및 자산평가결과 DB 저장

[설계 기준]
- 대상: 자산보유현황 DB에서 자산분류='연금' 인 항목
- 현재가: 야후파이낸스 비공식 API (종목코드.KS) - 기존 한국주식과 동일
- 동일 ETF가 여러 계좌에 있어도 종목 단위로 합산 저장
- 평가기준: KRW (원화)
- 평가일자: 실행일 KST 기준 통일 (토요일)
- 직전평가액: run_date 미만 데이터 중 가장 최근값 (당일 재실행 오염 방지)

[자산보유현황 DB 입력 방법]
  자산명    : ETF명 (예: TIGER미국S&P500)
  자산분류  : 연금
  티커/코드 : 종목코드 (예: 360750)  ← .KS 없이 숫자만
  수량      : 보유좌수 (여러 계좌 합산 입력)
  금액      : 평균매입단가
  메모      : 증권사/계좌유형 (참고용, 집계엔 미사용)
"""

import os
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


# ── 1. 자산보유현황 DB에서 연금ETF 조회 ──────────────────
def fetch_pension_holdings() -> list[dict]:
    """
    자산분류 = '연금' 인 항목 조회
    동일 종목코드가 여러 행으로 입력된 경우 → 수량/금액 합산
    반환: [{"name": "TIGER미국S&P500", "ticker": "360750", "quantity": 100, "avg_price": 15000}, ...]
    """
    url = f"https://api.notion.com/v1/databases/{DB_ASSET_HOLDINGS}/query"
    payload = {
        "filter": {
            "property": "자산분류",
            "select": {"equals": "연금"}
        }
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    # 종목코드 기준 합산 딕셔너리
    merged: dict[str, dict] = {}

    for page in res.json().get("results", []):
        props = page["properties"]

        name_arr = props.get("자산명", {}).get("title", [])
        name     = name_arr[0]["plain_text"].strip() if name_arr else ""

        ticker_arr = props.get("티커/코드", {}).get("rich_text", [])
        ticker     = ticker_arr[0]["plain_text"].strip() if ticker_arr else ""

        quantity  = props.get("수량", {}).get("number") or 0
        avg_price = props.get("금액", {}).get("number") or 0   # 매입단가

        if not ticker or quantity <= 0:
            print(f"[Holdings] ⚠️  '{name}' 티커/수량 미입력 - 건너뜀")
            continue

        if ticker in merged:
            # 동일 종목코드: 수량 합산, 자산명은 첫 번째 것 유지
            prev = merged[ticker]
            total_qty   = prev["quantity"] + quantity
            # 가중평균 매입단가
            weighted_avg = (prev["avg_price"] * prev["quantity"] + avg_price * quantity) / total_qty
            merged[ticker]["quantity"]  = total_qty
            merged[ticker]["avg_price"] = weighted_avg
        else:
            merged[ticker] = {
                "name":      name,
                "ticker":    ticker,
                "quantity":  quantity,
                "avg_price": avg_price,
            }

    holdings = list(merged.values())
    print(f"[Holdings] 연금ETF {len(holdings)}종목 조회 (합산 후)")
    return holdings


# ── 2. 야후파이낸스 현재가 조회 ───────────────────────────
def fetch_yahoo_prices(tickers: list[str]) -> dict[str, float]:
    """
    야후파이낸스 비공식 API로 KRW 현재가 조회
    ticker → ticker.KS 변환 (한국거래소 상장 ETF)
    반환: {"360750": 15420.0, ...}
    """
    prices = {}
    for ticker in tickers:
        yahoo_symbol = f"{ticker}.KS"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
        params = {"interval": "1d", "range": "1d"}

        for attempt in range(3):
            try:
                res = requests.get(url, params=params,
                                   headers={"User-Agent": "Mozilla/5.0"},
                                   timeout=10)
                res.raise_for_status()
                data  = res.json()
                meta  = data["chart"]["result"][0]["meta"]
                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                if price:
                    prices[ticker] = float(price)
                    print(f"[Yahoo] {ticker}.KS → {price:,.0f}원")
                else:
                    print(f"[Yahoo] ⚠️  {ticker}.KS 가격 없음")
                break
            except Exception as e:
                print(f"[Yahoo] ⚠️  {ticker}.KS 시도 {attempt+1}/3 실패: {e}")
                if attempt < 2:
                    time.sleep(3)

        time.sleep(0.5)   # 야후 Rate limit 방지

    return prices


# ── 3. 직전 평가액 조회 ───────────────────────────────────
def fetch_prev_eval(asset_name: str, run_date: str) -> float | None:
    """
    자산평가결과 DB에서 직전 평가액 조회
    - 자산분류='연금' + 평가일자 < run_date
    - 자산명 매칭은 Python에서 처리
    - 실패 시 None 반환
    """
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
            props = page["properties"]

            # 평가일자 (Title) 텍스트 추출
            title_arr   = props.get("평가일자", {}).get("title", [])
            stored_date = title_arr[0]["plain_text"].strip() if title_arr else ""

            # run_date 미만만 참조
            if not stored_date or stored_date >= run_date:
                continue

            # 자산명 (rich_text) 매칭
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
    """
    자산평가결과 DB 저장
    - 평가일자 : Title
    - 자산명   : rich_text
    - 자산분류 : select = '연금'
    - 수량     : number
    - 금액     : number (총매입금액 = 평균단가 × 수량)
    - 현재가   : number (주당/좌당)
    - 평가액   : number (현재가 × 수량)
    - 직전평가액: number
    """
    name      = holding["name"]
    ticker    = holding["ticker"]
    quantity  = holding["quantity"]
    avg_price = holding["avg_price"]

    cost_total  = round(avg_price * quantity)
    eval_amount = round(price * quantity) if price is not None else None
    prev_amount = fetch_prev_eval(name, run_date)

    properties = {
        "평가일자":  {"title":    [{"text": {"content": run_date}}]},
        "자산명":    {"rich_text": [{"text": {"content": name}}]},
        "자산분류":  {"select":   {"name": "연금"}},
        "수량":      {"number": quantity},
        "금액":      {"number": cost_total},
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

    change_str = ""
    if eval_amount is not None and prev_amount is not None:
        diff = eval_amount - prev_amount
        change_str = f"  변동: {diff:+,.0f}원"

    price_str  = f"{price:,.0f}원" if price is not None else "조회실패"
    amount_str = f"{eval_amount:,.0f}원" if eval_amount is not None else "-"
    print(f"[Notion] {name}({ticker}) 저장완료 | "
          f"현재가: {price_str} | 평가액: {amount_str}{change_str}")


# ── MAIN ──────────────────────────────────────────────────
def main():
    run_date = get_run_date()
    print(f"\n{'='*50}")
    print(f"[Pension] 실행일(KST): {run_date}")
    print(f"{'='*50}")

    # 1. 보유 현황 조회
    holdings = fetch_pension_holdings()
    if not holdings:
        print("[Pension] 보유 연금ETF 없음 - 종료")
        return

    # 2. 야후파이낸스 현재가 조회
    tickers = [h["ticker"] for h in holdings]
    prices  = fetch_yahoo_prices(tickers)

    # 3. 자산평가결과 저장
    for h in holdings:
        price = prices.get(h["ticker"])
        save_eval_result(h, price, run_date)

    print(f"\n[Pension] 완료 - {len(holdings)}종목 처리")


if __name__ == "__main__":
    main()
