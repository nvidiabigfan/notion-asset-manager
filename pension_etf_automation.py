"""
pension_etf_automation.py
Phase 6 - 연금ETF/펀드 현재가 조회 및 자산평가결과 DB 저장

[변경이력]
  v2 - KOFIA freesis API → dis.kofia.or.kr XML API 교체
       pykrx get_fund_ohlcv_by_date (미존재 함수) 제거
       빈 응답/파싱 오류 방어 로직 강화

[티커/코드 입력 규칙]
  ETF  (거래소 상장) : 숫자 6자리   예) 360750, 465580
  펀드 (비상장 수익증권): A로 시작  예) A0040Y0, A441800

[현재가 조회 방법]
  ETF  → 야후파이낸스 (ticker.KS)
  펀드 → dis.kofia.or.kr XML POST API
         └ 기준가(NAV): 전일 장 마감 기준 T+1 공시
         └ 실패시 fund.kofia.or.kr REST API fallback

[네이버 금융 API 사용 불가 이유]
  GitHub Actions IP(Azure 미국 대역)를 네이버가 봇으로 차단함
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

# dis.kofia.or.kr XML API 설정
KOFIA_DIS_URL = "https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/"
KOFIA_DIS_HEADERS = {
    "Content-Type": "text/xml; charset=utf-8",
    "User-Agent":   "Mozilla/5.0",
    "Referer":      "https://dis.kofia.or.kr/",
    "Origin":       "https://dis.kofia.or.kr",
}

# fund.kofia.or.kr fallback 설정
KOFIA_FUND_URL = "https://www.kofia.or.kr/biz/fund/sttus/fundNetAssetPriceList.do"


def get_run_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def is_fund_code(ticker: str) -> bool:
    """A로 시작하는 7자리 → 비상장 펀드 (수익증권 표준코드)"""
    return bool(re.match(r'^[Aa][0-9A-Za-z]{6}$', ticker))


def get_recent_business_days(n: int = 7) -> list[str]:
    """오늘부터 최대 n일 전까지 영업일(평일) 목록 반환 (YYYYMMDD 형식)"""
    kst_now = datetime.now(KST)
    result = []
    for days_ago in range(n * 2):  # 여유있게 탐색
        d = kst_now - timedelta(days=days_ago)
        if d.weekday() < 5:  # 월~금
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


# ── 2-B. KOFIA dis XML API (펀드, A코드) — 메인 ──────────
def fetch_kofia_dis_price(ticker: str) -> float | None:
    """
    금융투자협회 공시시스템 XML API
    URL : https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/
    방식: POST, Content-Type: text/xml
    서비스: COMPCode008 / fundInfoList  (펀드 기준가 조회)

    응답 예시:
      <standardPrices>
        <standardPrice>
          <standardCd>A0040Y0</standardCd>
          <standardDt>20260307</standardDt>
          <uOriginalAmt>1000.00</uOriginalAmt>  ← 기준가(NAV)
        </standardPrice>
      </standardPrices>
    """
    business_days = get_recent_business_days(7)

    for date_str in business_days:
        xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<message>
  <proframeHeader>
    <pfmAppName>FS-DIS</pfmAppName>
    <pfmSvcName>COMPCode008</pfmSvcName>
    <pfmFnName>selFundPrc</pfmFnName>
  </proframeHeader>
  <parameter>
    <standardCd>{ticker.upper()}</standardCd>
    <standardDt>{date_str}</standardDt>
    <reqCnt>1</reqCnt>
  </parameter>
</message>"""

        try:
            res = requests.post(
                KOFIA_DIS_URL,
                data=xml_body.encode("utf-8"),
                headers=KOFIA_DIS_HEADERS,
                timeout=10
            )
            res.raise_for_status()

            # ✅ 빈 응답 방어
            body = res.text.strip()
            if not body:
                print(f"[KOFIA-DIS] ⚠️  {ticker} 빈 응답 ({date_str}) - 다음 날짜 시도")
                time.sleep(0.3)
                continue

            # XML 파싱
            root = ET.fromstring(body)

            # 기준가 필드 탐색 (uOriginalAmt 또는 standardPrice)
            price_node = (
                root.find(".//uOriginalAmt")
                or root.find(".//standardPrice")
                or root.find(".//nav")
                or root.find(".//stdPrc")
            )

            if price_node is not None and price_node.text:
                price_text = price_node.text.replace(",", "").strip()
                if price_text and price_text not in ("0", "0.00"):
                    price = float(price_text)
                    print(f"[KOFIA-DIS] ✅ {ticker} 기준가({date_str}) → {price:,.2f}원")
                    return price

            # 에러 메시지 여부 확인
            err_node = root.find(".//errorMessage") or root.find(".//error")
            if err_node is not None:
                print(f"[KOFIA-DIS] ❌ {ticker} API 오류 ({date_str}): {err_node.text}")
                return None

            print(f"[KOFIA-DIS] ⚠️  {ticker} 기준가 필드 없음 ({date_str}) - 다음 날짜 시도")

        except ET.ParseError as e:
            print(f"[KOFIA-DIS] ⚠️  {ticker} XML 파싱 오류 ({date_str}): {e}")
            # XML 파싱 실패시 응답 앞부분 출력 (디버깅용)
            try:
                preview = res.text[:200] if res.text else "(빈 응답)"
                print(f"[KOFIA-DIS]    응답 미리보기: {preview}")
            except Exception:
                pass
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            if status == 404:
                print(f"[KOFIA-DIS] ❌ {ticker} 펀드코드 미존재")
                return None
            print(f"[KOFIA-DIS] ⚠️  {ticker} HTTP {status} ({date_str})")
        except Exception as e:
            print(f"[KOFIA-DIS] ⚠️  {ticker} 오류 ({date_str}): {e}")

        time.sleep(0.3)

    # dis API 실패 → fund.kofia.or.kr fallback
    print(f"[KOFIA-DIS] ⚠️  {ticker} 모든 날짜 실패 → fund.kofia.or.kr fallback 시도")
    return fetch_kofia_fund_price(ticker)


# ── 2-C. fund.kofia.or.kr fallback ───────────────────────
def fetch_kofia_fund_price(ticker: str) -> float | None:
    """
    금융투자협회 펀드공시 포털 fallback
    URL: https://www.kofia.or.kr/biz/fund/sttus/fundNetAssetPriceList.do
    방식: GET, 파라미터로 펀드코드 전달
    """
    business_days = get_recent_business_days(5)

    for date_str in business_days:
        params = {
            "standardCd": ticker.upper(),
            "standardDt": date_str,
            "pageIndex":  "1",
            "pageSize":   "1",
        }
        try:
            res = requests.get(
                KOFIA_FUND_URL,
                params=params,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer":    "https://www.kofia.or.kr/",
                },
                timeout=10
            )
            res.raise_for_status()

            body = res.text.strip()
            if not body:
                continue

            # JSON 응답 시도
            try:
                data = res.json()
                items = (
                    data.get("list")
                    or data.get("data")
                    or data.get("result", {}).get("list", [])
                    or []
                )
                if items:
                    price_raw = (
                        items[0].get("standardPrice")
                        or items[0].get("nav")
                        or items[0].get("stdPrc")
                    )
                    if price_raw:
                        price = float(str(price_raw).replace(",", "").strip())
                        print(f"[KOFIA-Fund] ✅ {ticker} 기준가({date_str}) → {price:,.2f}원")
                        return price
            except ValueError:
                pass  # JSON 아님 - XML 시도
                try:
                    root = ET.fromstring(body)
                    price_node = root.find(".//standardPrice") or root.find(".//nav")
                    if price_node is not None and price_node.text:
                        price = float(price_node.text.replace(",", "").strip())
                        print(f"[KOFIA-Fund] ✅ {ticker} 기준가({date_str}) → {price:,.2f}원")
                        return price
                except ET.ParseError:
                    pass

        except Exception as e:
            print(f"[KOFIA-Fund] ⚠️  {ticker} 오류 ({date_str}): {e}")

        time.sleep(0.3)

    print(f"[KOFIA-Fund] ❌ {ticker} 모든 fallback 실패 - 가격 없음")
    return None


# ── 2. 가격 조회 통합 (ETF/펀드 분기) ────────────────────
def fetch_price(holding: dict) -> float | None:
    ticker = holding["ticker"]
    if holding["is_fund"]:
        price = fetch_kofia_dis_price(ticker)
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
