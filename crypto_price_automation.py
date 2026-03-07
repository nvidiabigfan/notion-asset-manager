"""
crypto_price_automation.py
Phase 5 - 암호화폐 현재가 조회 및 자산평가결과 DB 저장

- 업비트 공개 REST API (v1/ticker) 사용 - API 키 불필요
- 자산분류: 암호화폐
- 평가기준: KRW (김치프리미엄 반영, 업비트 기준)
- 실행 시점: 매주 토요일 KST 12:00 (GitHub Actions cron: 0 3 * * 6)
"""

import os
import json
import requests
from datetime import datetime, timezone, timedelta

# ── 환경변수 ──────────────────────────────────────────────
NOTION_TOKEN        = os.environ["NOTION_TOKEN"]
DB_ASSET_HOLDINGS   = os.environ["DB_ASSET_HOLDINGS"]    # 자산보유현황 DB ID
DB_EVAL_RESULT      = os.environ["DB_EVAL_RESULT"]       # 자산평가결과 DB ID

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type":  "application/json",
    "Notion-Version": "2022-06-28",
}

KST = timezone(timedelta(hours=9))


# ── 실행 기준일 (KST 토요일) ─────────────────────────────
def get_run_date() -> str:
    """KST 기준 오늘 날짜를 YYYY-MM-DD 문자열로 반환"""
    return datetime.now(KST).strftime("%Y-%m-%d")


# ── 1. 자산보유현황 DB에서 암호화폐 항목 조회 ─────────────
def fetch_crypto_holdings(run_date: str) -> list[dict]:
    """
    자산분류 = '암호화폐' 인 행 조회
    반환: [{"name": "비트코인", "symbol": "BTC", "quantity": 0.5, "page_id": "..."}, ...]
    """
    url = f"https://api.notion.com/v1/databases/{DB_ASSET_HOLDINGS}/query"
    payload = {
        "filter": {
            "property": "자산분류",
            "select": {"equals": "암호화폐"}
        }
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    holdings = []
    for page in res.json().get("results", []):
        props = page["properties"]

        # 자산명 (Title)
        name_arr = props.get("자산명", {}).get("title", [])
        name = name_arr[0]["plain_text"].strip() if name_arr else ""

        # 티커/코드 → 심볼 (BTC, ETH 등)
        symbol_arr = props.get("티커/코드", {}).get("rich_text", [])
        symbol = symbol_arr[0]["plain_text"].strip().upper() if symbol_arr else ""

        # 수량
        quantity = props.get("수량", {}).get("number") or 0

        if symbol and quantity > 0:
            holdings.append({
                "page_id": page["id"],
                "name":     name,
                "symbol":   symbol,
                "quantity": quantity,
            })

    print(f"[Holdings] 암호화폐 보유 {len(holdings)}건 조회")
    return holdings


# ── 2. 업비트 현재가 일괄 조회 ────────────────────────────
def fetch_upbit_prices(symbols: list[str]) -> dict[str, float]:
    """
    업비트 KRW 마켓 현재가 조회
    symbols: ["BTC", "ETH", ...]
    반환: {"BTC": 135000000.0, "ETH": 5200000.0, ...}
    """
    markets = ",".join(f"KRW-{s}" for s in symbols)
    url = f"https://api.upbit.com/v1/ticker?markets={markets}"

    res = requests.get(url, headers={"Accept": "application/json"})
    res.raise_for_status()

    prices = {}
    for item in res.json():
        symbol = item["market"].replace("KRW-", "")
        prices[symbol] = float(item["trade_price"])   # 최근 체결가 (KRW)

    # 조회 실패 심볼 로깅
    for s in symbols:
        if s not in prices:
            print(f"[Upbit] ⚠️  {s} 가격 조회 실패 (마켓 미존재 가능)")

    print(f"[Upbit] 가격 조회 완료: {prices}")
    return prices


# ── 3. 직전 평가액 조회 (run_date 미만) ──────────────────
def fetch_prev_eval(asset_name: str, run_date: str) -> float | None:
    """
    자산평가결과 DB에서 해당 자산명의 직전 평가액 조회
    run_date 미만 데이터 중 가장 최근 값
    자산평가결과 DB에 티커/코드 컬럼 없으므로 자산명(Title)으로 필터
    """
    url = f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "자산명",   "title":  {"equals": asset_name}},
                {"property": "자산분류", "select": {"equals": "암호화폐"}},
                {"property": "평가일자", "date":   {"before": run_date}},
            ]
        },
        "sorts": [{"property": "평가일자", "direction": "descending"}],
        "page_size": 1,
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    results = res.json().get("results", [])
    if not results:
        return None

    val = results[0]["properties"].get("평가액", {}).get("number")
    return float(val) if val is not None else None


# ── 4. 자산평가결과 DB에 저장 ─────────────────────────────
def save_eval_result(holding: dict, price: float | None, run_date: str) -> None:
    """
    자산평가결과 DB에 1건 저장
    - 평가액   = 현재가 × 수량  (KRW)
    - 현재가   = 업비트 체결가  (KRW)
    - 직전평가액 = run_date 미만 최근 평가액
    """
    symbol   = holding["symbol"]
    quantity = holding["quantity"]

    eval_amount  = round(price * quantity) if price is not None else None
    prev_amount  = fetch_prev_eval(holding["name"], run_date)  # 자산명으로 직전값 조회

    properties = {
        "자산명": {
            "title": [{"text": {"content": holding["name"]}}]
        },
        "자산분류": {
            "select": {"name": "암호화폐"}
        },
        # 티커/코드 컬럼은 자산평가결과 DB에 없으므로 저장하지 않음
        "수량": {
            "number": quantity
        },
        "평가일자": {
            "date": {"start": run_date}
        },
    }

    # 현재가 (KRW)
    if price is not None:
        properties["현재가"] = {"number": price}

    # 평가액 (현재가 × 수량)
    if eval_amount is not None:
        properties["평가액"] = {"number": eval_amount}

    # 직전평가액
    if prev_amount is not None:
        properties["직전평가액"] = {"number": prev_amount}

    url = "https://api.notion.com/v1/pages"
    payload = {
        "parent": {"database_id": DB_EVAL_RESULT},
        "properties": properties,
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    change_str = ""
    if eval_amount is not None and prev_amount is not None:
        diff = eval_amount - prev_amount
        change_str = f"  변동: {diff:+,.0f}원"

    print(f"[Notion] {holding['name']}({symbol}) 저장 완료 | "
          f"현재가: {price:,.0f}원 | 평가액: {eval_amount:,.0f}원{change_str}")


# ── MAIN ──────────────────────────────────────────────────
def main():
    run_date = get_run_date()
    print(f"\n{'='*50}")
    print(f"[Crypto] 실행일(KST): {run_date}")
    print(f"{'='*50}")

    # 1. 보유 현황 조회
    holdings = fetch_crypto_holdings(run_date)
    if not holdings:
        print("[Crypto] 보유 암호화폐 없음 - 종료")
        return

    # 2. 업비트 현재가 일괄 조회
    symbols = list({h["symbol"] for h in holdings})
    prices  = fetch_upbit_prices(symbols)

    # 3. 자산평가결과 저장
    for h in holdings:
        price = prices.get(h["symbol"])   # None이면 마켓 미존재
        save_eval_result(h, price, run_date)

    print(f"\n[Crypto] 완료 - {len(holdings)}건 처리")


if __name__ == "__main__":
    main()
