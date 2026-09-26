"""
Phase 2: 주식 시세 자동화
- 야후 파이낸스에서 삼성전자, 삼성전자우, 엔비디아 주가 조회
- 미국주식은 환율정보 DB의 최신 환율 적용하여 원화 환산
- 자산보유현황 DB 기준으로 평가금액 계산
- 자산평가 결과 DB에 저장

■ 포트폴리오 종목 진입/이탈 처리 정책
  - 매수 신규 진입: 자산보유현황 DB에 추가되면 그 주 토요일 실행 시 자동 포함
  - 매도 이탈:      자산보유현황 DB에서 삭제되면 그 주 토요일부터 레코드 미생성 (조용히 제외)
  - 과거 이력:      매도 이전의 자산평가 결과 레코드는 삭제하지 않고 그대로 보존
  ※ 별도 매도 감지 로직 없이, 매주 실행 시점의 자산보유현황을
     source of truth로 사용하는 것만으로 자연스럽게 구현됨

■ 평가일자 기준
  - 야후 파이낸스가 반환하는 실제 거래일(금요일 등)이 아닌
    스크립트 실행일(KST 토요일)을 평가일자로 통일 저장
  - 실제 참조한 거래일은 로그에만 출력

■ 자산분류별 처리
  - 한국주식: 티커에 .KS 자동 추가 후 야후 파이낸스 조회 (KRW 그대로)
  - 미국주식: 야후 파이낸스 조회 후 환율 적용하여 KRW 환산
  - 연금:     한국주식과 동일하게 .KS 자동 추가 후 야후 파이낸스 조회 (KRW 그대로)
"""

import os
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta

import eval_result


# ── 설정 ──────────────────────────────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]

DB_ASSET_HOLDINGS  = "31a64e13bb46807b8673e94e7b416f34"  # 자산보유현황
DB_EXCHANGE_RATE   = "31a64e13bb4680a491b8c1c2ca7770bc"  # 환율정보
DB_REAL_ESTATE     = "31a64e13bb4680c18668eec357e11222"  # 부동산 실거래가

KST = timezone(timedelta(hours=9))

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

NOTION_CALL_INTERVAL = 0.4  # 초당 3회 제한 대응

# 티커에 .KS 자동 추가 대상 분류
KS_CATEGORIES = ("한국주식", "연금")


# ── Notion API 헬퍼 ───────────────────────────────────────────────────────────
def notion_request(method: str, path: str, body: dict = None) -> dict:
    url  = f"https://api.notion.com/v1{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, headers=HEADERS, method=method)

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            retry_after = int(e.headers.get("Retry-After", 60))
            print(f"  [RATE LIMIT] {retry_after}초 대기 후 재시도...")
            time.sleep(retry_after)
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read())
        else:
            raise

    time.sleep(NOTION_CALL_INTERVAL)
    return result


def query_db(db_id: str, filter_body: dict = None, sorts: list = None) -> list:
    results = []
    body = {}
    if filter_body:
        body["filter"] = filter_body
    if sorts:
        body["sorts"] = sorts

    while True:
        resp = notion_request("POST", f"/databases/{db_id}/query", body)
        results.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        body["start_cursor"] = resp["next_cursor"]

    return results


def get_prop(page: dict, name: str):
    prop  = page.get("properties", {}).get(name, {})
    ptype = prop.get("type")

    if ptype == "title":
        items = prop.get("title", [])
        return items[0]["plain_text"] if items else ""
    if ptype == "rich_text":
        items = prop.get("rich_text", [])
        return items[0]["plain_text"] if items else ""
    if ptype == "number":
        return prop.get("number")
    if ptype == "select":
        sel = prop.get("select")
        return sel["name"] if sel else ""
    if ptype == "date":
        d = prop.get("date")
        return d["start"] if d else ""
    return None


# ── 야후 파이낸스 주가 조회 ───────────────────────────────────────────────────
def fetch_stock_price(ticker: str) -> dict:
    """
    야후 파이낸스 비공식 API로 주가 조회.
    토요일/공휴일 등 비거래일에 실행해도 가장 최근 거래일 종가를 반환.

    반환: {
        "price":           float,  # 최근 거래일 종가
        "currency":        str,    # KRW / USD
        "last_trade_date": str,    # 실제 마지막 거래일 (로그용, 노션 저장 안 함)
        "market_state":    str,    # CLOSED / PRE / REGULAR / POST
    }
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}"
        f"?interval=1d&range=5d"
    )
    req_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    result       = data["chart"]["result"][0]
    meta         = result["meta"]
    currency     = meta.get("currency", "")
    market_state = meta.get("marketState", "")

    closes     = result["indicators"]["quote"][0].get("close", [])
    timestamps = result.get("timestamp", [])

    valid_pairs = [
        (ts, c) for ts, c in zip(timestamps, closes) if c is not None
    ]
    if not valid_pairs:
        price = meta.get("previousClose") or meta.get("regularMarketPrice")
        last_trade_date = "unknown"
    else:
        last_ts, price  = valid_pairs[-1]
        last_trade_date = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d")

    return {
        "price":           price,
        "currency":        currency,
        "last_trade_date": last_trade_date,   # 로그 출력용 only
        "market_state":    market_state,
    }


# ── 환율 조회 ─────────────────────────────────────────────────────────────────
def get_latest_usd_krw() -> float:
    rows = query_db(
        DB_EXCHANGE_RATE,
        sorts=[{"property": "조회일자", "direction": "descending"}],
    )
    if not rows:
        raise ValueError("환율정보 DB에 데이터가 없습니다.")
    rate = get_prop(rows[0], "USD/KRW 환율")
    if not rate:
        raise ValueError("환율 값을 읽을 수 없습니다.")
    print(f"  [환율] USD/KRW = {rate:,.2f}")
    return float(rate)


# ── 자산보유현황 DB 조회 ──────────────────────────────────────────────────────
def get_holdings() -> list:
    """
    보유 '행' 하나가 곧 하나의 포지션이다.
    같은 종목을 여러 계좌가 나눠 들고 있으므로(팔란티어 900주 / 73.368384주 /
    51주 / 20주) 자산명이나 티커로는 구분되지 않는다. page id를 함께 들고 온다.
    """
    rows     = query_db(DB_ASSET_HOLDINGS)
    holdings = []
    for row in rows:
        category = get_prop(row, "자산분류")
        # ↓ 수정: 연금 카테고리 추가
        if category not in ("한국주식", "미국주식", "연금"):
            continue

        name           = get_prop(row, "자산명")
        ticker         = get_prop(row, "티커/코드") or ""
        quantity       = get_prop(row, "수량") or 0
        unit_price_buy = get_prop(row, "금액")
        owner          = get_prop(row, "보유자") or ""

        if not ticker.strip():
            print(f"  [SKIP] {name} — 티커/코드 미입력")
            continue

        # ↓ 수정: 한국주식 + 연금 모두 .KS 자동 추가
        if category in KS_CATEGORIES and not ticker.upper().endswith(".KS"):
            ticker = ticker + ".KS"

        holdings.append({
            "id":             row["id"],
            "name":           name,
            "ticker":         ticker.strip(),
            "quantity":       quantity,
            "category":       category,
            "unit_price_buy": unit_price_buy,
            "owner":          owner,
        })
    return holdings


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    run_date = datetime.now(KST).strftime("%Y-%m-%d")
    print(f"\n{'='*55}")
    print(f"  주식 시세 자동화 실행 — {run_date} (KST)")
    print(f"{'='*55}")

    tracker   = eval_result.ErrorTracker("주식")
    available = eval_result.require_schema()

    # 1) 환율 조회
    print("\n[1] 환율 조회")
    usd_krw = get_latest_usd_krw()

    # 2) 보유 주식 조회
    print("\n[2] 자산보유현황 조회")
    holdings = get_holdings()
    if not holdings:
        print("  보유 주식 없음. 종료.")
        return
    for h in holdings:
        owner_str = h["owner"] or "미분류"
        print(f"  - {h['category']} / {owner_str} / {h['name']} / {h['quantity']}주")

    # 3) 주가 조회 및 평가 결과 저장
    print("\n[3] 주가 조회 및 노션 저장")
    summary = []

    for holding in holdings:
        name           = holding["name"]
        ticker         = holding["ticker"]
        qty            = holding["quantity"]
        category       = holding["category"]
        unit_price_buy = holding["unit_price_buy"]
        owner          = holding["owner"]
        label          = f"{owner or '미분류'}/{name}"

        print(f"\n  >> {label} ({ticker})")
        try:
            stock = fetch_stock_price(ticker)
        except Exception as e:
            # 건너뛰되 삼키지 않는다. main 끝에서 exit(1)로 이어진다.
            tracker.record(f"{label} 주가 조회", e)
            continue

        price           = stock["price"]
        currency        = stock["currency"]
        last_trade_date = stock["last_trade_date"]

        print(f"     실제거래일: {last_trade_date}  →  평가일자: {run_date} (실행일 기준 통일)")
        print(f"     종가: {price} {currency}  (시장상태: {stock['market_state']})")

        if currency == "KRW":
            unit_price_krw = price
            buy_eval = (unit_price_buy * qty) if unit_price_buy is not None else None
        else:
            unit_price_krw = price * usd_krw
            print(f"     원화환산: {unit_price_krw:,.0f}원 (×{usd_krw:,.2f})")
            buy_eval = (unit_price_buy * qty * usd_krw) if unit_price_buy is not None else None

        if buy_eval is not None:
            print(f"     매수원가: {buy_eval:,.0f}원 ({qty}주 × {unit_price_buy})")

        eval_amount = unit_price_krw * qty
        print(f"     평가금액: {eval_amount:,.0f}원 ({qty}주)")

        try:
            prev_eval = eval_result.fetch_prev_eval(holding["id"], run_date)
            if prev_eval is not None:
                print(f"     직전평가액: {prev_eval:,.0f}원")
            else:
                print("     직전평가액: 없음 (첫 등록)")

            action = eval_result.upsert(
                holding_id=holding["id"],
                owner=owner,
                asset_name=name,
                category=category,
                run_date=run_date,
                quantity=qty,
                unit_price=unit_price_krw,
                eval_amount=eval_amount,
                purchase_amount=buy_eval,
                prev_eval_amount=prev_eval,
                ticker=ticker,
                available=available,
            )
            print(f"  [{action}] {label}: {eval_amount:,.0f}원")
        except Exception as e:
            tracker.record(f"{label} 노션 저장", e)
            continue

        summary.append({
            "name":            label,
            "eval_amount":     eval_amount,
            "category":        category,
            "last_trade_date": last_trade_date,
        })

    # 4) 요약 출력
    print(f"\n{'='*55}")
    print("  평가 요약")
    print(f"{'='*55}")
    total = 0
    for s in summary:
        print(f"  {s['name']:24s}  {s['eval_amount']:>15,.0f} 원  (거래일: {s['last_trade_date']})")
        total += s["eval_amount"]
    print(f"  {'합계':24s}  {total:>15,.0f} 원  ({len(summary)}건)")
    print(f"{'='*55}\n")

    tracker.exit_if_any()


if __name__ == "__main__":
    main()
