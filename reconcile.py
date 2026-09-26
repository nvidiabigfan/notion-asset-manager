"""
reconcile.py — 실행 결과 정합성 점검 (워크플로우 마지막 단계)

■ 왜 필요한가
  2026-05 ~ 09, 20주 동안 워크플로우는 전부 success로 끝났지만
  실제로는 매주 자산의 1/4이 덮어써져 사라지고 있었다.
  개별 스크립트는 자기가 처리한 것만 알기 때문에,
  '보유는 61건인데 기록은 44건'이라는 사실을 아무도 보지 못했다.

■ 이 스크립트의 위치
  대시보드 배포까지 끝난 뒤 마지막에 돈다.
  따라서 여기서 실패해도 데이터 적재와 대시보드 발행은 이미 끝나 있다.
  목적은 차단이 아니라 '빨간 실행'을 만들어 알림이 가게 하는 것이다.
  경고를 로그에만 남기면 아무도 보지 않는다 — 20주가 그 증거다.

■ 점검 항목
  [1] 보유 건수 = 기록 건수인가          → 어긋나면 exit 1
  [2] 지난주 대비 총평가액 변동이 ±15% 이내인가 → 넘으면 exit 1
  [3] 부동산이 3주 연속 매수가 폴백인가   → 해당하면 exit 1

  [2]는 실제 급락장에서도 걸릴 수 있다. 그때는 로그를 확인하고
  TOTAL_SWING_PCT 환경변수를 조정하거나 그 주는 무시하면 된다.
  '조용히 지나가는 것'보다 '가끔 헛경보'가 낫다는 판단이다.
"""

import os
import sys

import eval_result

DB_ASSET_HOLDINGS = os.environ["DB_ASSET_HOLDINGS"]
DB_EVAL_RESULT    = os.environ["DB_EVAL_RESULT"]

# writer가 실제로 처리하는 자산분류. 이 밖의 분류는 애초에 평가 대상이 아니다.
COVERED = {"한국주식", "미국주식", "연금", "암호화폐", "부동산"}

SWING_PCT   = float(os.environ.get("TOTAL_SWING_PCT", "15"))
STALE_WEEKS = int(os.environ.get("STALE_ALERT_WEEKS", "3"))


# ── 속성 읽기 ─────────────────────────────────────────────────────────────────
def prop_text(row: dict, name: str) -> str:
    p = row.get("properties", {}).get(name, {})
    items = p.get("rich_text") or p.get("title") or []
    return items[0].get("plain_text", "").strip() if items else ""


def prop_num(row: dict, name: str):
    return row.get("properties", {}).get(name, {}).get("number")


def prop_select(row: dict, name: str) -> str:
    sel = row.get("properties", {}).get(name, {}).get("select")
    return sel.get("name", "") if sel else ""


def prop_check(row: dict, name: str) -> bool:
    return bool(row.get("properties", {}).get(name, {}).get("checkbox", False))


# ── 조회 ──────────────────────────────────────────────────────────────────────
def load_holdings() -> list[dict]:
    rows = eval_result.query_db(DB_ASSET_HOLDINGS)
    return [{
        "id":       row["id"],
        "name":     prop_text(row, "자산명"),
        "category": prop_select(row, "자산분류"),
        "ticker":   prop_text(row, "티커/코드"),
        "quantity": prop_num(row, "수량") or 0,
        "area":     prop_num(row, "전용면적") or 0,
        "owner":    prop_text(row, "보유자"),
    } for row in rows]


def eval_rows_on(date: str) -> list[dict]:
    return eval_result.query_db(
        DB_EVAL_RESULT,
        filter_body={"property": "평가일자", "rich_text": {"equals": date}},
    )


def recent_dates(want: int = 4) -> list[str]:
    """
    최근 평가일자를 최신순으로 모은다.

    전체 스캔(20주 × 61행)을 피하려고 페이지를 제한한다.
    한 페이지가 100행이고 한 주가 60여 행이므로 3페이지면 4주가 덮인다.
    평가일자가 YYYY-MM-DD 문자열이라 사전순 정렬 = 시간순 정렬이다.
    """
    body = {
        "sorts":     [{"property": "평가일자", "direction": "descending"}],
        "page_size": 100,
    }
    dates: list[str] = []
    for _ in range(4):
        resp = eval_result.notion_request(
            "POST", f"/databases/{DB_EVAL_RESULT}/query", body)
        for row in resp.get("results", []):
            d = prop_text(row, "평가일자")
            if d and d not in dates:
                dates.append(d)
        if len(dates) >= want or not resp.get("has_more"):
            break
        body["start_cursor"] = resp["next_cursor"]
    return sorted(dates, reverse=True)[:want]


# ── 점검 로직 (순수 함수 — 테스트에서 직접 호출한다) ──────────────────────────
def skip_reason(holding: dict) -> str | None:
    """
    각 writer가 실제로 건너뛰는 조건을 그대로 옮긴 것.
    writer의 조건을 바꾸면 여기도 같이 바꿔야 한다.
    """
    cat = holding["category"]
    if cat not in COVERED:
        return f"미지원 분류({cat or '분류없음'})"
    if not holding["name"]:
        return "자산명 없음"
    if cat in ("한국주식", "미국주식", "연금"):
        if not holding["ticker"]:
            return "티커/코드 미입력"
    elif cat == "암호화폐":
        if not holding["ticker"]:
            return "티커/코드 미입력"
        if holding["quantity"] <= 0:
            return "수량 0"
    elif cat == "부동산":
        if holding["area"] <= 0:
            return "전용면적 미입력"
    return None


def check_coverage(holdings: list[dict], rows: list[dict]) -> list[str]:
    """[1] 평가 대상 보유 행이 전부 기록됐는가. 중복 기록은 없는가."""
    expected = {h["id"]: h for h in holdings if skip_reason(h) is None}
    skipped  = [(h, skip_reason(h)) for h in holdings if skip_reason(h) is not None]

    seen: dict[str, int] = {}
    for row in rows:
        hid = prop_text(row, "보유ID")
        seen[hid] = seen.get(hid, 0) + 1

    print(f"  보유 {len(holdings)}건 "
          f"(평가대상 {len(expected)} / 제외 {len(skipped)}) → 기록 {len(rows)}건")

    if skipped:
        print("  제외된 행:")
        for h, why in skipped:
            print(f"    - {h['owner'] or '미분류'}/{h['name'] or '(이름없음)'}: {why}")

    problems = []

    missing = [h for hid, h in expected.items() if hid not in seen]
    for h in missing:
        problems.append(
            f"기록 누락: {h['owner'] or '미분류'}/{h['name']} ({h['category']})")

    dupes = [hid for hid, n in seen.items() if n > 1]
    for hid in dupes:
        h = expected.get(hid)
        label = f"{h['owner'] or '미분류'}/{h['name']}" if h else f"보유ID {hid[:8]}"
        problems.append(f"같은 키로 {seen[hid]}행 중복: {label}")

    orphan = [hid for hid in seen
              if hid and hid not in expected
              and hid not in {h["id"] for h in holdings}]
    for hid in orphan:
        print(f"  [알림] 보유현황에 없는 보유ID가 기록돼 있습니다: {hid[:8]}…")

    no_id = seen.get("", 0)
    if no_id:
        problems.append(f"보유ID가 빈 기록 {no_id}행 — 구 버전 코드가 쓴 행입니다")

    return problems


def total_of(rows: list[dict]) -> float:
    return sum(prop_num(r, "평가액") or 0 for r in rows)


def check_swing(cur_date: str, cur_rows: list[dict],
                prev_date: str | None, prev_rows: list[dict]) -> list[str]:
    """[2] 지난주 대비 총평가액이 비정상적으로 튀지 않았는가."""
    cur = total_of(cur_rows)
    print(f"  {cur_date} 총평가액: {cur:,.0f}원")

    if not prev_date or not prev_rows:
        print("  직전 평가일자 없음 — 변동 점검 건너뜀 (첫 실행)")
        return []

    prev = total_of(prev_rows)
    print(f"  {prev_date} 총평가액: {prev:,.0f}원")
    if prev <= 0:
        print("  직전 총액이 0 — 변동 점검 건너뜀")
        return []

    pct = (cur - prev) / prev * 100
    print(f"  변동: {cur - prev:+,.0f}원 ({pct:+.2f}%)  임계 ±{SWING_PCT:.0f}%")

    if abs(pct) > SWING_PCT:
        return [f"총평가액이 한 주 만에 {pct:+.2f}% 움직였습니다 "
                f"({prev:,.0f} → {cur:,.0f}). 시장 변동이 맞는지, "
                f"아니면 누락·중복인지 확인하세요"]
    return []


def check_stale_streak(dates: list[str],
                       rows_by_date: dict[str, list[dict]]) -> list[str]:
    """[3] 부동산이 STALE_WEEKS주 연속 매수가 폴백인가."""
    if len(dates) < STALE_WEEKS:
        print(f"  누적 {len(dates)}주 — {STALE_WEEKS}주 연속 판정 불가, 건너뜀")
        return []

    window = dates[:STALE_WEEKS]
    streak: dict[str, dict] = {}

    for date in window:
        for row in rows_by_date.get(date, []):
            if prop_select(row, "자산분류") != "부동산":
                continue
            hid = prop_text(row, "보유ID")
            if not hid:
                continue
            entry = streak.setdefault(
                hid, {"name": prop_text(row, "자산명"), "stale": 0, "seen": 0})
            entry["seen"] += 1
            if prop_check(row, "시가미반영"):
                entry["stale"] += 1

    problems = []
    for hid, e in streak.items():
        if e["seen"] == STALE_WEEKS and e["stale"] == STALE_WEEKS:
            problems.append(
                f"부동산 '{e['name']}'이 {STALE_WEEKS}주 연속 매수가로 기록됐습니다 "
                f"({', '.join(window)}). 국토부 API가 막혔거나 해당 면적의 "
                f"실거래가 없습니다 — 수동시세 입력을 검토하세요")
        else:
            mark = "시가미반영" if e["stale"] else "실거래"
            print(f"  {e['name']}: 최근 {e['seen']}주 중 미반영 {e['stale']}주 ({mark})")

    return problems


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 62)
    print("  실행 결과 정합성 점검")
    print("=" * 62)

    available = eval_result.require_schema()

    dates = recent_dates(want=max(STALE_WEEKS, 2))
    if not dates:
        print("  자산평가 결과 DB가 비어 있습니다 — 점검할 것이 없습니다")
        return 0

    cur_date = dates[0]
    rows_by_date = {d: eval_rows_on(d) for d in dates}
    cur_rows = rows_by_date[cur_date]

    problems: list[str] = []

    print(f"\n[1] 보유 건수 대 기록 건수 — {cur_date}")
    holdings = load_holdings()
    problems += check_coverage(holdings, cur_rows)

    print("\n[2] 지난주 대비 총평가액 변동")
    prev_date = dates[1] if len(dates) > 1 else None
    problems += check_swing(cur_date, cur_rows,
                            prev_date, rows_by_date.get(prev_date, []))

    print(f"\n[3] 부동산 {STALE_WEEKS}주 연속 매수가 폴백")
    if not available.get("시가미반영"):
        print("  '시가미반영' 속성이 없어 판정할 수 없습니다 — 건너뜀")
    else:
        problems += check_stale_streak(dates, rows_by_date)

    print("\n" + "=" * 62)
    if not problems:
        print("  정합성 점검 통과")
        print("=" * 62 + "\n")
        return 0

    print(f"  정합성 문제 {len(problems)}건 — 워크플로우를 실패로 종료합니다")
    print("=" * 62)
    for p in problems:
        print(f"  - {p}")
    print("=" * 62 + "\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
