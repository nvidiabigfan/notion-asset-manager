"""
자산평가 결과 DB 키 회귀 테스트.

2026-09-19 / 09-26 실행에서 같은 자산명을 가진 서로 다른 계좌의 보유분이
덮어써져 각각 326,429,140원 / 342,321,976원이 사라졌다.
그 상황을 실제 보유 구성으로 재현하고, 새 키로 재발하지 않음을 확인한다.

실행: python3 tests/test_eval_result_key.py   (노션 접속 없음, 전부 인메모리)
"""

import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("NOTION_TOKEN", "test-token")
os.environ.setdefault("DB_EVAL_RESULT", "test-db")

import eval_result


# ── 인메모리 가짜 노션 ─────────────────────────────────────────────────────────
class FakeNotion:
    def __init__(self):
        self.pages: dict[str, dict] = {}
        self.calls = 0

    def request(self, method, path, body=None):
        self.calls += 1

        if method == "GET" and path.startswith("/databases/"):
            return {"properties": {
                "평가일자":   {"type": "title"},
                "자산명":     {"type": "rich_text"},
                "자산분류":   {"type": "select"},
                "수량":       {"type": "number"},
                "금액":       {"type": "number"},
                "현재가":     {"type": "number"},
                "평가액":     {"type": "number"},
                "직전평가액": {"type": "number"},
                "보유ID":     {"type": "rich_text"},
                "보유자":     {"type": "rich_text"},
                "티커":       {"type": "rich_text"},
                "시가미반영": {"type": "checkbox"},
            }}

        if method == "POST" and path.endswith("/query"):
            return {"results": self._query(body.get("filter")), "has_more": False}

        if method == "POST" and path == "/pages":
            pid = str(uuid.uuid4())
            self.pages[pid] = {"id": pid, "properties": self._echo(body["properties"])}
            return self.pages[pid]

        if method == "PATCH" and path.startswith("/pages/"):
            pid = path.split("/pages/")[1]
            self.pages[pid]["properties"].update(self._echo(body["properties"]))
            return self.pages[pid]

        raise AssertionError(f"예상치 못한 호출: {method} {path}")

    @staticmethod
    def _echo(props):
        """노션은 rich_text/title 항목에 plain_text를 함께 돌려준다. 그 형태를 맞춘다."""
        out = {}
        for name, value in props.items():
            value = dict(value)
            for kind in ("rich_text", "title"):
                if kind in value and value[kind]:
                    value[kind] = [
                        {**item, "plain_text": item["text"]["content"]}
                        for item in value[kind]
                    ]
            out[name] = value
        return out

    # 이 테스트가 쓰는 필터(rich_text equals의 and)만 해석한다
    def _query(self, flt):
        if not flt:
            return list(self.pages.values())
        conds = flt.get("and", [flt])
        out = []
        for page in self.pages.values():
            if all(self._match(page, c) for c in conds):
                out.append(page)
        return out

    @staticmethod
    def _match(page, cond):
        name = cond["property"]
        want = cond["rich_text"]["equals"]
        prop = page["properties"].get(name, {})
        items = prop.get("rich_text") or prop.get("title") or []
        got = items[0]["plain_text"] if items else ""
        return got == want


def install(fake):
    eval_result.notion_request = fake.request
    eval_result._schema_cache = None


# ── 실제 2026-09-26 보유 구성 (자산명, 보유자, 수량, 평가액) ────────────────────
# 로그에서 중복으로 확인된 10개 자산명 중 대표 3개를 그대로 사용한다.
POSITIONS = [
    ("팔란티어", "승민", 900,        221_730_106),
    ("팔란티어", "승민", 73.368384,   18_075_533),
    ("팔란티어", "민경", 51,          12_564_706),
    ("팔란티어", "민경", 20,           4_927_336),
    ("테슬라",   "승민", 130,         65_676_218),
    ("테슬라",   "승민", 30,          15_156_050),
    ("테슬라",   "민경", 17,           8_588_429),
    ("테슬라",   "민경", 6.261124,     3_163_130),
    ("삼성전자", "승민", 35,           9_135_000),
    ("삼성전자", "민경", 20,           5_220_000),
]
EXPECTED_TOTAL = sum(p[3] for p in POSITIONS)

RUN_DATE = "2026-09-26"


def write_all(fake, run_date, holding_ids):
    available = eval_result.require_schema()
    for (name, owner, qty, amount), hid in zip(POSITIONS, holding_ids):
        eval_result.upsert(
            holding_id=hid, owner=owner, asset_name=name, category="미국주식",
            run_date=run_date, quantity=qty, eval_amount=amount,
            purchase_amount=amount * 0.6, available=available,
        )


def stored_total(fake, run_date):
    total = 0
    for page in fake.pages.values():
        props = page["properties"]
        date = props["평가일자"]["title"][0]["text"]["content"]
        if date == run_date:
            total += props["평가액"]["number"]
    return total


def rows_on(fake, run_date):
    return sum(
        1 for p in fake.pages.values()
        if p["properties"]["평가일자"]["title"][0]["text"]["content"] == run_date
    )


def check(label, got, want):
    ok = "PASS" if got == want else "FAIL"
    print(f"  [{ok}] {label}: {got:,}" + ("" if got == want else f"  (기대 {want:,})"))
    return got == want


def main():
    results = []

    print("\n[1] 보유 행 단위 키 — 같은 종목 4계좌가 서로 덮어쓰지 않는다")
    fake = FakeNotion(); install(fake)
    hids = [f"hold-{i:02d}" for i in range(len(POSITIONS))]
    write_all(fake, RUN_DATE, hids)
    results.append(check("저장된 행 수", rows_on(fake, RUN_DATE), len(POSITIONS)))
    results.append(check("평가액 합계", stored_total(fake, RUN_DATE), EXPECTED_TOTAL))

    print("\n[2] 재실행해도 행이 늘지 않는다 (upsert 멱등)")
    write_all(fake, RUN_DATE, hids)
    results.append(check("재실행 후 행 수", rows_on(fake, RUN_DATE), len(POSITIONS)))
    results.append(check("재실행 후 합계", stored_total(fake, RUN_DATE), EXPECTED_TOTAL))

    print("\n[3] 직전평가액이 같은 계좌의 것만 따라온다")
    fake2 = FakeNotion(); install(fake2)
    write_all(fake2, "2026-09-19", hids)
    prev = eval_result.fetch_prev_eval("hold-00", RUN_DATE)
    results.append(check("hold-00(팔란티어 900주)의 직전평가액", int(prev), 221_730_106))
    prev3 = eval_result.fetch_prev_eval("hold-03", RUN_DATE)
    results.append(check("hold-03(팔란티어 20주)의 직전평가액", int(prev3), 4_927_336))

    print("\n[4] 구 키(자산명+평가일자) 재현 — 덮어쓰기가 실제로 일어났는가")
    collapsed = {}
    for name, owner, qty, amount in POSITIONS:
        collapsed[name] = amount          # 뒤에 온 값이 앞을 덮는다
    old_total = sum(collapsed.values())
    print(f"  구 키 저장 합계: {old_total:,}원 / 실제: {EXPECTED_TOTAL:,}원")
    print(f"  소실: {EXPECTED_TOTAL - old_total:,}원")
    results.append(check("구 키 행 수(=고유 자산명 수)", len(collapsed), 3))

    print("\n[5] 필수 속성이 없으면 조용히 쓰지 않고 멈춘다")
    class NoSchema(FakeNotion):
        def request(self, method, path, body=None):
            if method == "GET" and path.startswith("/databases/"):
                return {"properties": {"평가일자": {"type": "title"},
                                       "자산명":   {"type": "rich_text"}}}
            return super().request(method, path, body)

    install(NoSchema())
    try:
        eval_result.require_schema()
        print("  [FAIL] 속성이 없는데도 통과했다")
        results.append(False)
    except SystemExit as e:
        ok = e.code == 1
        print(f"  [{'PASS' if ok else 'FAIL'}] exit code {e.code}로 중단")
        results.append(ok)

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
