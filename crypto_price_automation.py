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

import eval_result

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

        # 보유자 (rich_text) — 없으면 빈 문자열
        owner_arr = props.get("보유자", {}).get("rich_text", [])
        owner = owner_arr[0]["plain_text"].strip() if owner_arr else ""

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
                "id":       page["id"],
                "name":     name,
                "symbol":   symbol,
                "quantity": quantity,
                "amount":   amount,
                "owner":    owner,
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


# ── 3. 자산평가결과 DB에 저장 ─────────────────────────────
#
# 이전 구현은 두 가지가 틀려 있었다.
#   (1) 직전평가액 조회에서 title 타입인 '평가일자'에 date 필터를 걸어
#       매주 400 Bad Request가 났고, 전부 except로 삼켜져 암호화폐
#       직전평가액은 한 번도 채워진 적이 없다.
#   (2) upsert 없이 항상 POST여서 재실행하면 같은 날짜 행이 중복 생성됐다.
# 두 가지 모두 eval_result 모듈의 공통 경로로 대체한다.


# ── MAIN ──────────────────────────────────────────────────
def main():
    run_date = get_run_date()
    print(f"\n{'='*50}")
    print(f"[Crypto] 실행일(KST): {run_date}")
    print(f"{'='*50}")

    tracker   = eval_result.ErrorTracker("암호화폐")
    available = eval_result.require_schema()

    holdings = fetch_crypto_holdings()
    if not holdings:
        print("[Crypto] 보유 암호화폐 없음 - 종료")
        return

    symbols = list({h["symbol"] for h in holdings})
    prices  = fetch_upbit_prices(symbols)

    saved = 0
    for h in holdings:
        label = f"{h['owner'] or '미분류'}/{h['name']}"
        price = prices.get(h["symbol"])

        if price is None:
            tracker.record(f"{label} 업비트 시세", f"{h['symbol']} 가격 조회 실패")
            continue

        quantity    = h["quantity"]
        eval_amount = price * quantity

        try:
            prev_amount = eval_result.fetch_prev_eval(h["id"], run_date)
            action = eval_result.upsert(
                holding_id=h["id"],
                owner=h["owner"],
                asset_name=h["name"],
                category="암호화폐",
                run_date=run_date,
                quantity=quantity,
                unit_price=price,
                eval_amount=eval_amount,
                purchase_amount=h["amount"],
                prev_eval_amount=prev_amount,
                ticker=h["symbol"],
                available=available,
            )
        except Exception as e:
            tracker.record(f"{label} 노션 저장", e)
            continue

        change_str = ""
        if prev_amount is not None:
            change_str = f"  변동: {eval_amount - prev_amount:+,.0f}원"
        print(f"[Notion] [{action}] {label}({h['symbol']}) | "
              f"현재가: {price:,.0f}원 | 평가액: {eval_amount:,.0f}원{change_str}")
        saved += 1

    print(f"\n[Crypto] 완료 - {saved}/{len(holdings)}건 저장")
    tracker.exit_if_any()


if __name__ == "__main__":
    main()
