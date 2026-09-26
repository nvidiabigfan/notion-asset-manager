"""
정합성 점검(reconcile.py) 회귀 테스트.

실제 2026-09-26 사고 상황(보유 61건 / 기록 44건)을 축소 재현해
[1] 건수 점검이 그것을 잡아내는지 확인한다.

함께 확인하는 것: [2] 총액 변동 점검은 이 사고를 잡지 못한다.
덮어쓰기가 매주 똑같이 일어났으므로 주간 변동률은 정상으로 보였기 때문이다.
셋을 다 두는 이유가 여기 있다 — 각자 잡는 고장이 다르다.

실행: python3 tests/test_reconcile.py   (노션 접속 없음, 전부 인메모리)
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("NOTION_TOKEN", "test-token")
os.environ.setdefault("DB_EVAL_RESULT", "test-eval-db")
os.environ.setdefault("DB_ASSET_HOLDINGS", "test-holdings-db")

import reconcile


# ── 행 만들기 ─────────────────────────────────────────────────────────────────
def holding(hid, name, category="미국주식", ticker="TICK",
            qty=1, area=0, owner="승민"):
    return {"id": hid, "name": name, "category": category, "ticker": ticker,
            "quantity": qty, "area": area, "owner": owner}


def eval_row(hid, name, amount, category="미국주식",
             stale=False, date="2026-09-26"):
    return {"properties": {
        "평가일자":   {"title":     [{"plain_text": date}]},
        "자산명":     {"rich_text": [{"plain_text": name}]},
        "자산분류":   {"select":    {"name": category}},
        "평가액":     {"number":    amount},
        "보유ID":     {"rich_text": [{"plain_text": hid}] if hid else []},
        "시가미반영": {"checkbox":  stale},
    }}


def check(label, got, want):
    ok = got == want
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}: {got}"
          + ("" if ok else f"  (기대 {want})"))
    return ok


# ── 2026-09-26 실제 구성 축소판 ───────────────────────────────────────────────
# 팔란티어 4계좌 / 테슬라 4계좌 / 삼성전자 2계좌 = 보유 10행, 고유 자산명 3개
POSITIONS = [
    ("h00", "팔란티어", 221_730_106), ("h01", "팔란티어", 18_075_533),
    ("h02", "팔란티어", 12_564_706),  ("h03", "팔란티어",  4_927_336),
    ("h04", "테슬라",    65_676_218), ("h05", "테슬라",    15_156_050),
    ("h06", "테슬라",     8_588_429), ("h07", "테슬라",     3_163_130),
    ("h08", "삼성전자",   9_135_000), ("h09", "삼성전자",   5_220_000),
]
HOLDINGS  = [holding(hid, name) for hid, name, _ in POSITIONS]
FULL_ROWS = [eval_row(hid, name, amt) for hid, name, amt in POSITIONS]


def main():
    results = []

    print("\n[1] 전부 기록됐으면 통과한다")
    results.append(check("문제 건수",
                         len(reconcile.check_coverage(HOLDINGS, FULL_ROWS)), 0))

    print("\n[2] 2026-09-26 사고 재현 — 자산명이 겹쳐 3행만 남은 경우")
    # 구 키(자산명+평가일자)에서는 같은 자산명이 한 행으로 뭉갰다
    collapsed = {}
    for hid, name, amt in POSITIONS:
        collapsed[name] = (hid, amt)
    bug_rows = [eval_row(hid, name, amt) for name, (hid, amt) in collapsed.items()]
    problems = reconcile.check_coverage(HOLDINGS, bug_rows)
    results.append(check("누락으로 잡힌 건수", len(problems), 7))
    results.append(check("전부 '기록 누락'인가",
                         all(p.startswith("기록 누락") for p in problems), True))

    print("\n[3] 같은 보유ID가 두 행이면 중복으로 잡는다")
    dupe_rows = FULL_ROWS + [eval_row("h00", "팔란티어", 221_730_106)]
    problems = reconcile.check_coverage(HOLDINGS, dupe_rows)
    results.append(check("중복으로 잡힌 건수",
                         sum(1 for p in problems if "중복" in p), 1))

    print("\n[4] 보유ID가 빈 행(구 버전 코드가 쓴 행)을 잡는다")
    legacy_rows = FULL_ROWS + [eval_row("", "정체불명", 1_000_000)]
    problems = reconcile.check_coverage(HOLDINGS, legacy_rows)
    results.append(check("빈 보유ID로 잡힌 건수",
                         sum(1 for p in problems if "보유ID가 빈" in p), 1))

    print("\n[5] 평가 대상이 아닌 행은 누락으로 치지 않는다")
    extra = HOLDINGS + [
        holding("h10", "티커없는종목", ticker=""),          # 주식 writer가 건너뜀
        holding("h11", "예금",   category="예금"),           # 아무도 처리 안 함
        holding("h12", "면적없음", category="부동산", area=0),
        holding("h13", "수량0",   category="암호화폐", qty=0),
    ]
    results.append(check("문제 건수",
                         len(reconcile.check_coverage(extra, FULL_ROWS)), 0))

    print("\n[6] 총액 변동 — 임계 안이면 통과, 넘으면 잡는다")
    reconcile.SWING_PCT = 15.0
    prev = [eval_row(h, n, a, date="2026-09-19") for h, n, a in POSITIONS]
    results.append(check("5% 상승 시 문제 건수",
        len(reconcile.check_swing(
            "2026-09-26",
            [eval_row(h, n, a * 1.05) for h, n, a in POSITIONS],
            "2026-09-19", prev)), 0))
    results.append(check("22% 하락 시 문제 건수",
        len(reconcile.check_swing(
            "2026-09-26",
            [eval_row(h, n, a * 0.78) for h, n, a in POSITIONS],
            "2026-09-19", prev)), 1))

    print("\n[7] 변동 점검만으로는 이번 사고를 못 잡는다 (건수 점검이 필요한 이유)")
    # 덮어쓰기가 매주 똑같이 일어났으므로 주간 변동률은 멀쩡해 보였다
    bug_prev = [eval_row(hid, name, amt, date="2026-09-19")
                for name, (hid, amt) in collapsed.items()]
    swing = reconcile.check_swing("2026-09-26", bug_rows, "2026-09-19", bug_prev)
    results.append(check("사고 상태인데 변동 점검 통과함", len(swing), 0))

    print("\n[8] 부동산 3주 연속 매수가 폴백을 잡는다")
    reconcile.STALE_WEEKS = 3
    dates = ["2026-09-26", "2026-09-19", "2026-09-12"]
    always_stale = {
        d: [eval_row("re1", "○○동 아파트", 1_300_000_000, "부동산", True, d)]
        for d in dates
    }
    results.append(check("3주 연속 미반영 → 문제 건수",
                         len(reconcile.check_stale_streak(dates, always_stale)), 1))

    recovered = dict(always_stale)
    recovered["2026-09-26"] = [
        eval_row("re1", "○○동 아파트", 1_400_000_000, "부동산", False, "2026-09-26")]
    results.append(check("이번 주 실거래가 잡힘 → 문제 건수",
                         len(reconcile.check_stale_streak(dates, recovered)), 0))

    print("\n" + "=" * 58)
    if all(results):
        print(f"  전체 통과 ({len(results)}/{len(results)})")
        print("=" * 58 + "\n")
        return 0
    print(f"  실패 {results.count(False)}건 / {len(results)}건")
    print("=" * 58 + "\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
