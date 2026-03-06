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

# ※ 티커/코드는 자산보유현황 DB의 "티커/코드" 컬럼에서 직접 읽어옴
#   → 종목 추가 시 코드 수정 불필요, 노션에서만 관리

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# 노션 API rate limit 대응
# 공식 제한: 초당 3회 평균 → 호출 간 0.4초 간격으로 여유 확보
# 429 응답 시 Retry-After 헤더 값만큼 대기 후 1회 재시도
NOTION_CALL_INTERVAL = 0.4  # 초


# ── Notion API 헬퍼 ───────────────────────────────────────────────────────────
def notion_request(method: str, path: str, body: dict = None) -> dict:
    """
    노션 API 호출 with rate limit 대응
    - 매 호출 후 NOTION_CALL_INTERVAL 초 대기 (초당 3회 제한 준수)
    - 429 응답 시 Retry-After 헤더 값만큼 대기 후 1회 재시도
    """
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

    time.sleep(NOTION_CALL_INTERVAL)  # 다음 호출 전 인터벌 확보
    return result


def query_db(db_id: str, filter_body: dict = None, sorts: list = None) -> list:
    """DB 전체 페이지 조회 (페이지네이션 처리)"""
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
    """페이지 프로퍼티 값 추출"""
    prop = page.get("properties", {}).get(name, {})
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
        "price": float,          # 최근 거래일 종가
        "currency": str,         # KRW / USD
        "last_trade_date": str,  # 실제 거래일 (YYYY-MM-DD)
        "market_state": str,     # CLOSED / PRE / REGULAR / POST
    }
    """
    # range=5d 로 최근 5거래일 데이터를 요청 → 마지막 봉이 항상 최근 거래일 종가
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

    result   = data["chart"]["result"][0]
    meta     = result["meta"]
    currency = meta.get("currency", "")
    market_state = meta.get("marketState", "")

    # 종가 배열에서 마지막 유효값 = 최근 거래일 종가
    closes     = result["indicators"]["quote"][0].get("close", [])
    timestamps = result.get("timestamp", [])

    # None 제거 후 마지막 값 사용
    valid_pairs = [
        (ts, c) for ts, c in zip(timestamps, closes) if c is not None
    ]
    if not valid_pairs:
        # fallback: meta의 previousClose
        price = meta.get("previousClose") or meta.get("regularMarketPrice")
        last_trade_date = "unknown"
    else:
        last_ts, price = valid_pairs[-1]
        # timestamp → KST 날짜 (한국·미국 모두 UTC 기준 변환)
        last_trade_date = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d")

    return {
        "price":           price,
        "currency":        currency,
        "last_trade_date": last_trade_date,
        "market_state":    market_state,
    }


# ── 환율 조회 (환율정보 DB 최신 레코드) ───────────────────────────────────────
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
    주식 보유 내역만 반환 (자산분류 = 한국주식 | 미국주식)

    이 함수가 반환하는 목록이 해당 주 토요일 평가의 전부.
    - 자산보유현황에 있는 종목만 평가 대상 → 매수 종목 자동 포함
    - 자산보유현황에서 삭제된 종목은 조회되지 않음 → 매도 종목 자동 제외
    - 티커는 노션 "티커/코드" 컬럼에서 직접 읽음 → 코드 수정 없이 종목 추가 가능
    - 티커/코드가 비어있는 종목은 SKIP (로그 출력)
    """
    rows = query_db(DB_ASSET_HOLDINGS)
    holdings = []
    for row in rows:
        category = get_prop(row, "자산분류")
        if category not in ("한국주식", "미국주식"):
            continue

        name            = get_prop(row, "자산명")
        ticker          = get_prop(row, "티커/코드") or ""
        quantity        = get_prop(row, "수량") or 0
        unit_price_buy  = get_prop(row, "금액")   # 매수 당시 단가 (KRW 또는 USD)

        if not ticker.strip():
            print(f"  [SKIP] {name} — 티커/코드 미입력")
            continue

        # 한국주식은 야후 파이낸스 티커에 .KS 접미사 필요
        # 노션에 005930 으로 입력해도 자동으로 005930.KS 로 변환
        if category == "한국주식" and not ticker.upper().endswith(".KS"):
            ticker = ticker + ".KS"

        holdings.append({
            "name":           name,
            "ticker":         ticker.strip(),
            "quantity":       quantity,
            "category":       category,
            "unit_price_buy": unit_price_buy,  # 매수 단가 (없으면 None)
        })
    return holdings


# ── 직전평가액 조회 ───────────────────────────────────────────────────────────
def get_prev_eval_amount(asset_name: str, current_trade_date: str) -> float | None:
    """
    자산평가 결과 DB에서 해당 자산의 직전 레코드 평가액을 반환.
    - 현재 거래일보다 이전인 레코드 중 가장 최근 것을 사용
    - 첫 등록 종목(이력 없음)이면 None 반환 → 직전평가액 컬럼 비워둠
    """
    rows = query_db(
        DB_EVAL_RESULT,
        filter_body={
            "property": "자산명", "rich_text": {"equals": asset_name}
        },
        sorts=[{"property": "평가일자", "direction": "descending"}],
    )

    for row in rows:
        row_date = get_prop(row, "평가일자")  # Title 컬럼
        if row_date and row_date < current_trade_date:
            prev_amount = get_prop(row, "평가액")
            if prev_amount is not None:
                print(f"     직전평가액: {prev_amount:,.0f}원 ({row_date})")
                return float(prev_amount)

    print(f"     직전평가액: 없음 (첫 등록)")
    return None


# ── 자산평가 결과 DB 저장 ─────────────────────────────────────────────────────
def upsert_eval_result(
    asset_name: str,
    category: str,
    quantity: float,
    unit_price_krw: float,
    eval_amount_krw: float,
    purchase_amount: float | None,   # 매수원가 (자산보유현황의 금액 컬럼)
    prev_eval_amount: float | None,  # 직전 주 평가액 (없으면 None)
    trade_date: str,                 # 실제 마지막 거래일 (YYYY-MM-DD)
) -> None:
    """
    자산평가 결과 DB에 거래일 기준 레코드 UPSERT
    - 동일 (자산명 + 평가일자) 레코드가 있으면 업데이트, 없으면 신규 생성

    노션 DB 컬럼 구성 (실제 확인 기준):
      평가일자 (Title) / 자산명 (Text) / 자산분류 (Select)
      수량 (Number) / 금액 (Number, 매수원가) / 현재가 (Number)
      평가액 (Number) / 직전평가액 (Number) / 변동액 (수식) / 변동율 (수식)
    """
    existing = query_db(
        DB_EVAL_RESULT,
        filter_body={
            "and": [
                {"property": "자산명",  "rich_text": {"equals": asset_name}},
                {"property": "평가일자", "title":     {"equals": trade_date}},
            ]
        },
    )

    props = {
        "자산명":     {"rich_text": [{"text": {"content": asset_name}}]},
        "자산분류":   {"select":    {"name": category}},
        "수량":       {"number": quantity},
        "금액":       {"number": purchase_amount},           # 매수원가
        "현재가":     {"number": round(unit_price_krw)},
        "평가액":     {"number": round(eval_amount_krw)},
        "직전평가액": {"number": round(prev_eval_amount) if prev_eval_amount is not None else None},
    }

    if existing:
        page_id = existing[0]["id"]
        notion_request("PATCH", f"/pages/{page_id}", {"properties": props})
        print(f"  [업데이트] {asset_name}: {eval_amount_krw:,.0f}원")
    else:
        props["평가일자"] = {"title": [{"text": {"content": trade_date}}]}
        notion_request(
            "POST",
            "/pages",
            {"parent": {"database_id": DB_EVAL_RESULT}, "properties": props},
        )
        print(f"  [신규생성] {asset_name}: {eval_amount_krw:,.0f}원")


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    run_date = datetime.now(KST).strftime("%Y-%m-%d")  # 스크립트 실행일 (토요일)
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
        unit_price_buy = holding["unit_price_buy"]  # 매수 단가

        print(f"\n  >> {name} ({ticker})")
        try:
            stock = fetch_stock_price(ticker)
        except Exception as e:
            print(f"  [ERROR] 주가 조회 실패: {e}")
            continue

        price            = stock["price"]
        currency         = stock["currency"]
        last_trade_date  = stock["last_trade_date"]

        print(f"     기준거래일: {last_trade_date}  (실행일: {run_date})")
        print(f"     종가: {price} {currency}  (시장상태: {stock['market_state']})")

        if currency == "KRW":
            unit_price_krw = price
            # 매수원가: 단가(KRW) × 수량
            buy_eval = (unit_price_buy * qty) if unit_price_buy is not None else None
        else:
            unit_price_krw = price * usd_krw
            print(f"     원화환산: {unit_price_krw:,.0f}원 (×{usd_krw:,.2f})")
            # 매수원가: 단가(USD) × 수량 × 현재환율 (매수 당시 환율 미보존 → 현재 환율 적용)
            buy_eval = (unit_price_buy * qty * usd_krw) if unit_price_buy is not None else None

        if buy_eval is not None:
            print(f"     매수원가: {buy_eval:,.0f}원 ({qty}주 × {unit_price_buy})")

        eval_amount = unit_price_krw * qty
        print(f"     평가금액: {eval_amount:,.0f}원 ({qty}주)")

        # 직전평가액 조회 (첫 등록이면 None)
        prev_eval = get_prev_eval_amount(name, last_trade_date)

        upsert_eval_result(
            asset_name=name,
            category=category,
            quantity=qty,
            unit_price_krw=unit_price_krw,
            eval_amount_krw=eval_amount,
            purchase_amount=round(buy_eval) if buy_eval is not None else None,
            prev_eval_amount=prev_eval,
            trade_date=last_trade_date,
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
        print(f"  {s['name']:15s}  {s['eval_amount']:>15,.0f} 원  ({s['last_trade_date']} 종가)")
        total += s["eval_amount"]
    print(f"  {'합계':15s}  {total:>15,.0f} 원")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
