"""
eval_result.py — 자산평가 결과 DB 단일 기록 경로

■ 왜 이 모듈이 생겼나
  stock / crypto / real_estate 세 스크립트가 각자 upsert를 구현했고,
  키가 전부 '자산명 + 평가일자'였다. 같은 종목을 두 계좌가 보유하면
  뒤에 처리된 쪽이 앞을 PATCH로 덮어써서 앞선 평가액이 사라졌다.

  2026-09-19 실행 로그 실측: 쓰기 61건 / 고유 자산명 44개.
  10개 자산명이 중복돼 326,429,140원이 소실되고 370,090,855원만 남았다.
  2026-09-26 실행에서도 같은 10개 종목, 342,321,976원이 동일하게 소실됐다.
  소실분 수익률이 +73.30%로 잔존분(+32.28%)의 2배여서, 성과가 좋은
  보유분일수록 더 많이 지워지는 방향으로 편향돼 있었다.

■ 키를 무엇으로 잡는가
  자산보유현황 DB의 '행 자체'가 하나의 보유 포지션이다.
  팔란티어는 900주 / 73.368384주 / 51주 / 20주 네 행으로 존재하고,
  소수점 수량이 섞인 것으로 보아 증권사가 다른 계좌 단위다.
  따라서 보유자로도, 티커로도 유일해지지 않는다.

  키 = 평가일자 + 보유ID(= 자산보유현황 행의 Notion page id)
  page id는 이름 변경('리커젼'/'리커전')이나 계좌 추가와 무관하게 안정적이다.

■ 이 모듈을 쓰는 쪽이 지켜야 할 것
  - 보유현황을 읽을 때 page["id"]를 함께 들고 올 것
  - 에러는 삼키되 ErrorTracker에 기록하고, main 끝에서 exit_if_any()를 부를 것
"""

import os
import sys
import time

import requests

NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
DB_EVAL_RESULT = os.environ["DB_EVAL_RESULT"]

HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

API = "https://api.notion.com/v1"

# Notion 공식 제한은 평균 초당 3회. 여유를 둔다.
CALL_INTERVAL = 0.4
MAX_RETRY     = 3

# 자산평가 결과 DB에 반드시 있어야 하는 속성 (없으면 실행 중단)
REQUIRED_PROPS = {
    "보유ID": "rich_text",   # 자산보유현황 행의 page id — 중복 방지 키
    "보유자": "rich_text",   # 대시보드 승민/민경 카드의 입력
}

# 있으면 쓰고 없으면 건너뛰는 속성
OPTIONAL_PROPS = {
    "티커":       "rich_text",
    "시가미반영": "checkbox",   # 실거래가 조회 실패 → 매수가 폴백 표시
}


# ── HTTP ──────────────────────────────────────────────────────────────────────
def notion_request(method: str, path: str, body: dict | None = None) -> dict:
    """429를 존중하며 재시도한다. 마지막 시도까지 실패하면 예외를 올린다."""
    url = f"{API}{path}"
    last_exc = None

    for attempt in range(1, MAX_RETRY + 1):
        res = requests.request(method, url, headers=HEADERS, json=body, timeout=30)

        if res.status_code == 429:
            wait = float(res.headers.get("Retry-After", 5))
            print(f"    [RATE LIMIT] {wait}초 대기 후 재시도 ({attempt}/{MAX_RETRY})")
            time.sleep(wait)
            last_exc = RuntimeError(f"429 Too Many Requests ({path})")
            continue

        if res.status_code >= 500:
            wait = 2 ** attempt
            print(f"    [{res.status_code}] {wait}초 대기 후 재시도 ({attempt}/{MAX_RETRY})")
            time.sleep(wait)
            last_exc = RuntimeError(f"{res.status_code} ({path})")
            continue

        if not res.ok:
            # 4xx는 재시도해도 같은 결과다. 본문을 살려서 올린다.
            raise RuntimeError(f"{res.status_code} {path} — {res.text[:300]}")

        time.sleep(CALL_INTERVAL)
        return res.json()

    raise last_exc if last_exc else RuntimeError(f"요청 실패: {path}")


def query_db(db_id: str, filter_body: dict | None = None,
             sorts: list | None = None) -> list:
    """페이지네이션을 끝까지 따라간다."""
    results: list = []
    body: dict = {}
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


# ── 스키마 검증 ────────────────────────────────────────────────────────────────
_schema_cache: dict[str, str] | None = None


def get_schema() -> dict[str, str]:
    """자산평가 결과 DB의 {속성명: 타입}."""
    global _schema_cache
    if _schema_cache is None:
        db = notion_request("GET", f"/databases/{DB_EVAL_RESULT}")
        _schema_cache = {
            name: prop.get("type") for name, prop in db.get("properties", {}).items()
        }
    return _schema_cache


def require_schema() -> dict[str, bool]:
    """
    필수 속성이 없으면 무엇을 어떻게 추가해야 하는지 찍고 종료한다.
    조용히 틀린 값을 쓰는 것보다 멈추는 쪽이 낫다.

    반환: 선택 속성의 사용 가능 여부 {"티커": bool, "시가미반영": bool}
    """
    schema  = get_schema()
    missing = []

    for name, want_type in REQUIRED_PROPS.items():
        actual = schema.get(name)
        if actual is None:
            missing.append(f"  - '{name}' 속성이 없습니다. 타입 {want_type}로 추가하세요.")
        elif actual != want_type:
            missing.append(
                f"  - '{name}' 속성의 타입이 {actual}입니다. {want_type}여야 합니다."
            )

    if missing:
        print("\n" + "=" * 62)
        print("  자산평가 결과 DB 스키마가 맞지 않아 중단합니다.")
        print("  노션에서 아래를 먼저 반영하세요.")
        print("=" * 62)
        print("\n".join(missing))
        print("=" * 62 + "\n")
        sys.exit(1)

    available = {}
    for name, want_type in OPTIONAL_PROPS.items():
        ok = schema.get(name) == want_type
        available[name] = ok
        if not ok:
            print(f"  [알림] 선택 속성 '{name}'({want_type}) 없음 — 기록을 건너뜁니다.")
    return available


# ── 읽기 ──────────────────────────────────────────────────────────────────────
def _rich_text(page: dict, name: str) -> str:
    items = page.get("properties", {}).get(name, {}).get("rich_text", [])
    return items[0]["plain_text"] if items else ""


def _title(page: dict, name: str) -> str:
    items = page.get("properties", {}).get(name, {}).get("title", [])
    return items[0]["plain_text"] if items else ""


def fetch_prev_eval(holding_id: str, run_date: str) -> float | None:
    """
    같은 보유 포지션의 직전 평가액.

    평가일자는 title 타입이라 date 필터를 걸 수 없다(400이 난다).
    rich_text 필터로 보유ID만 좁히고, 날짜 비교는 파이썬에서 한다.
    """
    rows = query_db(
        DB_EVAL_RESULT,
        filter_body={"property": "보유ID", "rich_text": {"equals": holding_id}},
        sorts=[{"property": "평가일자", "direction": "descending"}],
    )
    for row in rows:
        row_date = _title(row, "평가일자")
        if row_date and row_date < run_date:
            amount = row.get("properties", {}).get("평가액", {}).get("number")
            if amount is not None:
                return float(amount)
    return None


# ── 쓰기 ──────────────────────────────────────────────────────────────────────
def upsert(
    *,
    holding_id:       str,
    owner:            str,
    asset_name:       str,
    category:         str,
    run_date:         str,
    quantity:         float | None = None,
    unit_price:       float | None = None,
    eval_amount:      float | None = None,
    purchase_amount:  float | None = None,
    prev_eval_amount: float | None = None,
    ticker:           str = "",
    stale_price:      bool = False,
    available:        dict[str, bool] | None = None,
) -> str:
    """
    평가일자 + 보유ID로 upsert. 같은 주에 같은 포지션은 한 행만 남는다.

    반환: "신규생성" | "업데이트"
    """
    if not holding_id:
        raise ValueError(f"보유ID 없음 — {asset_name}")

    available = available or {}

    existing = query_db(
        DB_EVAL_RESULT,
        filter_body={
            "and": [
                {"property": "평가일자", "rich_text": {"equals": run_date}},
                {"property": "보유ID",   "rich_text": {"equals": holding_id}},
            ]
        },
    )

    props: dict = {
        "자산명":  {"rich_text": [{"text": {"content": asset_name}}]},
        "자산분류": {"select":    {"name": category}},
        "보유ID":  {"rich_text": [{"text": {"content": holding_id}}]},
        "보유자":  {"rich_text": [{"text": {"content": owner or "미분류"}}]},
    }

    if quantity is not None:
        props["수량"] = {"number": quantity}
    if purchase_amount is not None:
        props["금액"] = {"number": round(purchase_amount)}
    if unit_price is not None:
        props["현재가"] = {"number": round(unit_price)}
    if eval_amount is not None:
        props["평가액"] = {"number": round(eval_amount)}
    props["직전평가액"] = (
        {"number": round(prev_eval_amount)} if prev_eval_amount is not None
        else {"number": None}
    )

    if available.get("티커") and ticker:
        props["티커"] = {"rich_text": [{"text": {"content": ticker}}]}
    if available.get("시가미반영"):
        props["시가미반영"] = {"checkbox": bool(stale_price)}

    if existing:
        notion_request("PATCH", f"/pages/{existing[0]['id']}", {"properties": props})
        if len(existing) > 1:
            print(f"    [경고] {asset_name} — 같은 키의 행이 {len(existing)}개입니다. "
                  f"첫 행만 갱신했습니다. 노션에서 중복 행을 정리하세요.")
        return "업데이트"

    props["평가일자"] = {"title": [{"text": {"content": run_date}}]}
    notion_request(
        "POST", "/pages",
        {"parent": {"database_id": DB_EVAL_RESULT}, "properties": props},
    )
    return "신규생성"


# ── 실패 집계 ──────────────────────────────────────────────────────────────────
class ErrorTracker:
    """
    개별 항목 실패는 건너뛰되, 한 건이라도 있으면 워크플로우를 실패시킨다.
    지금까지는 전부 try/except로 삼켜져 20주 연속 success로 찍혔다.
    """

    def __init__(self, step: str):
        self.step   = step
        self.errors: list[tuple[str, str]] = []

    def record(self, context: str, exc: BaseException | str) -> None:
        msg = str(exc)
        self.errors.append((context, msg))
        print(f"  [ERROR] {context}: {msg}")

    def exit_if_any(self) -> None:
        if not self.errors:
            print(f"\n[{self.step}] 실패 항목 없음")
            return
        print(f"\n{'=' * 62}")
        print(f"  [{self.step}] 실패 {len(self.errors)}건 — 워크플로우를 실패로 종료합니다")
        print(f"{'=' * 62}")
        for context, msg in self.errors:
            print(f"  - {context}: {msg}")
        print(f"{'=' * 62}\n")
        sys.exit(1)
