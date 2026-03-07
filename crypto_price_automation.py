"""
crypto_price_automation.py  (v3 - DB 스키마 완전 반영)
Phase 5 - 암호화폐 현재가 조회 및 자산평가결과 DB 저장

[자산평가결과 DB 실제 컬럼 구조]
  평가일자   → Title   ← 노션 페이지 제목
  자산명     → text
  자산분류   → select
  수량       → number
  금액       → number  (매입가, 보유현황에서 복사)
  현재가     → number
  평가액     → number
  직전평가액 → number
  변동액     → number  (또는 formula)
  변동율     → number  (또는 formula)
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


# ── 1. 자산보유현황 DB에서 암호화폐 항목 조회 ─────────────
def fetch_crypto_holdings() -> list[dict]:
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

        # 티커/코드
        symbol_arr = props.get("티커/코드", {}).get("rich_text", [])
        symbol = symbol_arr[0]["plain_text"].strip().upper() if symbol_arr else ""

        # 수량
        quantity = props.get("수량", {}).get("number") or 0

        # 금액 (매입가)
        amount = props.get("금액", {}).get("number") or 0

        if symbol and quantity > 0:
            holdings.append({
                "name":     name,
                "symbol":   symbol,
                "quantity": quantity,
                "amount":   amount,
            })

    print(f"[Holdings] 암호화폐 보유 {len(holdings)}건 조회")
    return holdings


# ── 2. 업비트 현재가 일괄 조회 ────────────────────────────
def fetch_upbit_prices(symbols: list[str]) -> dict[str, float]:
    markets = ",".join(f"KRW-{s}" for s in symbols)
    url = f"https://api.upbit.com/v1/ticker?markets={markets}"

    for attempt in range(3):
        try:
            res = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
            res.raise_for_status()
            prices = {}
            for item in res.json():
                sym = item["market"].replace("KRW-", "")
                prices[sym] = float(item["trade_price"])
            for s in symbols:
                if s not in prices:
                    print(f"[Upbit] ⚠️  {s} 가격 조회 실패 (마켓 미존재 가능)")
            print(f"[Upbit] 가격 조회 완료: {prices}")
            return prices
        except Exception as e:
            print(f"[Upbit] ⚠️  시도 {attempt+1}/3 실패: {e}")
            if attempt < 2:
                time.sleep(5)

    print("[Upbit] ❌ 3회 모두 실패")
    return {}


# ── 3. 직전 평가액 조회 ───────────────────────────────────
def fetch_prev_eval(asset_name: str, run_date: str) -> float | None:
    """
    자산평가결과 DB 구조:
      - Title = 평가일자 (날짜 문자열이 페이지 제목)
      - 자산명 = rich_text
      - 자산분류 = select

    필터: 자산분류=암호화폐 + 평가일자(date)<run_date
    자산명 매칭은 Python에서 처리
    전체 try/except 로 감싸 절대 중단되지 않도록
    """
    try:
        url = f"https://api.notion.com/v1/databases/{DB_EVAL_RESULT}/query"
        payload = {
            "filter": {
                "and": [
                    {"property": "자산분류", "select": {"equals": "암호화폐"}},
                    {"property": "평가일자", "date":   {"before": run_date}},
                ]
            },
            "sorts": [{"property": "평가일자", "direction": "descending"}],
            "page_size": 100,
        }
        res = requests.post(url, headers=HEADERS, json=payload)
        res.raise_for_status()

        for page in res.json().get("results", []):
            props = page["properties"]
            # 자산명은 rich_text 타입
            name_arr = props.get("자산명", {}).get("rich_text", [])
            stored_name = name_arr[0]["plain_text"].strip() if name_arr else ""
            if stored_name == asset_name:
                val = props.get("평가액", {}).get("number")
                return float(val) if val is not None else None

        return None

    except Exception as e:
        print(f"[PrevEval] ⚠️  {asset_name} 직전평가액 조회 실패 (무시): {e}")
        return None


# ── 4. 자산평가결과 DB에 저장 ─────────────────────────────
def save_eval_result(holding: dict, price: float | None, run_date: str) -> None:
    """
    자산평가결과 DB 저장 - 실제 DB 스키마 기준

    Title(평가일자): 날짜 문자열 → 페이지 제목으로 저장
    자산명: rich_text
    자산분류: select
    수량: number
    금액: number  (매입가)
    현재가: number
    평가액: number
    직전평가액: number
    """
    symbol   = holding["symbol"]
    quantity = holding["quantity"]
    name     = holding["name"]
    amount   = holding["amount"]

    eval_amount = round(price * quantity) if price is not None else None
    prev_amount = fetch_prev_eval(name, run_date)

    # Title = 평가일자 (페이지 제목)
    properties = {
        "평가일자": {
            "title": [{"text": {"content": run_date}}]
        },
        "자산명": {
            "rich_text": [{"text": {"content": name}}]
        },
        "자산분류": {
            "select": {"name": "암호화폐"}
        },
        "수량": {
            "number": quantity
        },
        "금액": {
            "number": amount
        },
    }

    if price is not None:
        properties["현재가"] = {"number": price}

    if eval_amount is not None:
        properties["평가액"] = {"number": eval_amount}

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

    price_str  = f"{price:,.0f}원" if price is not None else "조회실패"
    amount_str = f"{eval_amount:,.0f}원" if eval_amount is not None else "-"
    print(f"[Notion] {name}({symbol}) 저장 완료 | 현재가: {price_str} | 평가액: {amount_str}{change_str}")


# ── MAIN ──────────────────────────────────────────────────
def main():
    run_date = get_run_date()
    print(f"\n{'='*50}")
    print(f"[Crypto] 실행일(KST): {run_date}")
    print(f"{'='*50}")

    holdings = fetch_crypto_holdings()
    if not holdings:
        print("[Crypto] 보유 암호화폐 없음 - 종료")
        return

    symbols = list({h["symbol"] for h in holdings})
    prices  = fetch_upbit_prices(symbols)

    for h in holdings:
        price = prices.get(h["symbol"])
        save_eval_result(h, price, run_date)

    print(f"\n[Crypto] 완료 - {len(holdings)}건 처리")


if __name__ == "__main__":
    main()
