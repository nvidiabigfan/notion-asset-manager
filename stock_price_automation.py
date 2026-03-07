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
"""

import os
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta


# ── 설정 ──────────────────────────────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]

DB_ASSET_HOLDINGS  = "31a64e13bb46807b8673e94e7b416f34"  # 자산보유현황
DB_EXCHANGE_RATE   = "31a64e13bb4680a491b8c1c2ca7770bc"  # 환율정보
DB_REAL_ESTATE     = "31a64e13bb4680c18668eec357e11222"  # 부동산 실거래가
DB_EVAL_RESULT     = "31a64e13bb46802c91e1f5502631a154"  # 자산평가 결과

KST = timezone(timedelta(hours=9))

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

NOTION_CALL_INTERVAL = 0.4  # 초당 3회 제한 대응


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
    rows     = query_db(DB_ASSET_HOLDINGS)
    holdings = []
    for row in rows:
        category = get_prop(row, "자산분류")
        if category not in ("한국주식", "미국주식"):
            continue

        name           = get_prop(row, "자산명")
        ticker         = get_prop(row, "티커/코드") or ""
        quantity       = get_prop(row, "수량") or 0
        unit_price_buy = get_prop(row, "금액")

        if not ticker.strip():
            print(f"  [SKIP] {name} — 티커/코드 미입력")
            continue

        if category == "한국주식" and not ticker.upper().endswith(".KS"):
            ticker = ticker + ".KS"

        holdings.append({
            "name":           name,
            "ticker":         ticker.strip(),
            "quantity":       quantity,
            "category":       category,
            "unit_price_buy": unit_price_buy,
        })
    return holdings


# ── 직전평가액 조회 ───────────────────────────────────────────────────────────
def get_prev_eval_amount(asset_name: str, run_date: str) -> float | None:
    """
    자산평가 결과 DB에서 해당 자산의 직전 레코드 평가액을 반환.
    - 평가일자(Title)가 run_date(실행일) 미만인 레코드 중 가장 최근 것 사용
    - 첫 등록 종목이면 None 반환
    """
    rows = query_db(
        DB_EVAL_RESULT,
        filter_body={
            "property": "자산명", "rich_text": {"equals": asset_name}
        },
        sorts=[{"property": "평가일자", "direction": "descending"}],
    )

    for row in rows:
        row_date = get_prop(row, "평가일자")   # Title 컬럼 (YYYY-MM-DD)
        if row_date and row_date < run_date:
            prev_amount = get_prop(row, "평가액")
            if prev_amount is not None:
                print(f"     직전평가액: {prev_amount:,.0f}원 ({row_date})")
                return float(prev_amount)

    print(f"     직전평가액: 없음 (첫 등록)")
    return None


# ── 자산평가 결과 DB 저장 ─────────────────────────────────────────────────────
def upsert_eval_result(
    asset_name:       str,
    category:         str,
    quantity:         float,
    unit_price_krw:   float,
    eval_amount_krw:  float,
    purchase_amount:  float | None,
    prev_eval_amount: float | None,
    run_date:         str,           # ← 실행일(KST 토요일) 기준으로 통일
) -> None:
    """
    자산평가 결과 DB에 실행일 기준 레코드 UPSERT
    평가일자 = run_date (스크립트 실행 KST 날짜, 실제 거래일과 무관)
    """
    existing = query_db(
        DB_EVAL_RESULT,
        filter_body={
            "and": [
                {"property": "자산명",  "rich_text": {"equals": asset_name}},
                {"property": "평가일자", "title":     {"equals": run_date}},
            ]
        },
    )

    props = {
        "자산명":     {"rich_text": [{"text": {"content": asset_name}}]},
        "자산분류":   {"select":    {"name": category}},
        "수량":       {"number": quantity},
        "금액":       {"number": purchase_amount},
        "현재가":     {"number": round(unit_price_krw)},
        "평가액":     {"number": round(eval_amount_krw)},
        "직전평가액": {"number": round(prev_eval_amount) if prev_eval_amount is not None else None},
    }

    if existing:
        page_id = existing[0]["id"]
        notion_request("PATCH", f"/pages/{page_id}", {"properties": props})
        print(f"  [업데이트] {asset_name}: {eval_amount_krw:,.0f}원")
    else:
        props["평가일자"] = {"title": [{"text": {"content": run_date}}]}
        notion_request(
            "POST",
            "/pages",
            {"parent": {"database_id": DB_EVAL_RESULT}, "properties": props},
        )
        print(f"  [신규생성] {asset_name}: {eval_amount_krw:,.0f}원")


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    run_date = datetime.now(KST).strftime("%Y-%m-%d")   # 실행일 = 평가일자 기준 (토요일)
    print(f"\n{'='*55}")
    print(f"  주식 시세 자동화 실행 — {run_date} (KST)")
    print(f"{'='*55}")

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
        print(f"  - {h['category']} / {h['name']} / {h['quantity']}주")

    # 3) 주가 조회 및 평가 결과 저장
    print("\n[3] 주가 조회 및 노션 저장")
    summary = []

    for holding in holdings:
        name           = holding["name"]
        ticker         = holding["ticker"]
        qty            = holding["quantity"]
        category       = holding["category"]
        unit_price_buy = holding["unit_price_buy"]

        print(f"\n  >> {name} ({ticker})")
        try:
            stock = fetch_stock_price(ticker)
        except Exception as e:
            print(f"  [ERROR] 주가 조회 실패: {e}")
            continue

        price           = stock["price"]
        currency        = stock["currency"]
        last_trade_date = stock["last_trade_date"]  # 로그 출력용

        # 평가일자는 run_date(실행일)로 통일, 실제 거래일은 로그에만 표시
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

        # 직전평가액 조회 (run_date 기준 이전 데이터)
        prev_eval = get_prev_eval_amount(name, run_date)

        upsert_eval_result(
            asset_name=name,
            category=category,
            quantity=qty,
            unit_price_krw=unit_price_krw,
            eval_amount_krw=eval_amount,
            purchase_amount=round(buy_eval) if buy_eval is not None else None,
            prev_eval_amount=prev_eval,
            run_date=run_date,           # ← last_trade_date 대신 run_date 사용
        )

        summary.append({
            "name":            name,
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
        print(f"  {s['name']:15s}  {s['eval_amount']:>15,.0f} 원  (거래일: {s['last_trade_date']})")
        total += s["eval_amount"]
    print(f"  {'합계':15s}  {total:>15,.0f} 원")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
