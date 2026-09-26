"""
국토부 실거래가 API 오류 구분 테스트.

지금까지 부동산 13억은 매주 매수가에 고정돼 있었는데,
'API가 막혔다'와 '해당 면적의 실거래가 없다'가 똑같이 빈 리스트여서
어느 쪽인지 알 수 없었다. 그 둘이 구분되는지 확인한다.

실행: python3 tests/test_real_estate_api.py   (네트워크 접속 없음)
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("NOTION_TOKEN", "test-token")
os.environ.setdefault("PUBLIC_DATA_API_KEY", "test-key")
os.environ.setdefault("DB_EVAL_RESULT", "test-eval-db")

import real_estate_automation as re_auto


# ── requests.get 대역 ─────────────────────────────────────────────────────────
class FakeResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass


def install(text):
    """다음 호출이 돌려줄 응답 본문을 정하고, 실제 호출 횟수를 센다."""
    calls = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        calls["n"] += 1
        return FakeResponse(text)

    re_auto.requests.get = fake_get
    re_auto._api_blocked = None
    return calls


OK_WITH_ITEM = """<response><header><resultCode>00</resultCode>
<resultMsg>NORMAL SERVICE.</resultMsg></header><body><items><item>
<excluUseAr>84.97</excluUseAr><dealAmount>130,000</dealAmount>
<dealYear>2026</dealYear><dealMonth>8</dealMonth><dealDay>14</dealDay>
<umdNm>역삼동</umdNm><aptNm>테스트아파트</aptNm><floor>12</floor>
</item></items></body></response>"""

OK_NO_ITEM = """<response><header><resultCode>00</resultCode>
<resultMsg>NORMAL SERVICE.</resultMsg></header>
<body><items></items></body></response>"""

BAD_KEY = """<response><header><resultCode>30</resultCode>
<resultMsg>SERVICE_KEY_IS_NOT_REGISTERED_ERROR</resultMsg></header></response>"""

CMM_ENVELOPE = """<OpenAPI_ServiceResponse><cmmMsgHeader>
<returnReasonCode>30</returnReasonCode>
<returnAuthMsg>SERVICE_KEY_IS_NOT_REGISTERED_ERROR</returnAuthMsg>
</cmmMsgHeader></OpenAPI_ServiceResponse>"""

HTML_ERROR = ("<html><body>SERVICE_KEY_IS_NOT_REGISTERED_ERROR</body></html>")

TRANSIENT = """<response><header><resultCode>99</resultCode>
<resultMsg>APPLICATION_ERROR</resultMsg></header></response>"""


def check(label, got, want):
    ok = got == want
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}: {got}"
          + ("" if ok else f"  (기대 {want})"))
    return ok


def main():
    results = []

    print("\n[1] 정상 응답 — 거래가 나오고 오류는 없다")
    install(OK_WITH_ITEM)
    trades, err = re_auto.fetch_apt_trades("11680", "202608")
    results.append(check("거래 건수", len(trades), 1))
    results.append(check("오류 사유", err, None))
    results.append(check("금액(원)", trades[0]["price_won"], 1_300_000_000))

    print("\n[2] 조회는 됐는데 거래가 없다 — 오류가 아니다")
    install(OK_NO_ITEM)
    trades, err = re_auto.fetch_apt_trades("11680", "202608")
    results.append(check("거래 건수", len(trades), 0))
    results.append(check("오류 사유 없음", err is None, True))

    print("\n[3] 서비스키 미등록 — 오류로 구분되고 이후 호출이 차단된다")
    calls = install(BAD_KEY)
    trades, err = re_auto.fetch_apt_trades("11680", "202608")
    results.append(check("오류 사유가 잡힘", err is not None and "인증 오류" in err, True))
    results.append(check("HTTP 호출 1회", calls["n"], 1))
    re_auto.fetch_apt_trades("11680", "202607")
    re_auto.fetch_apt_trades("11680", "202606")
    results.append(check("차단 후 추가 호출 없음", calls["n"], 1))

    print("\n[4] cmmMsgHeader 형식 오류 응답도 인증 오류로 읽는다")
    install(CMM_ENVELOPE)
    _, err = re_auto.fetch_apt_trades("11680", "202608")
    results.append(check("인증 오류로 분류", err is not None and "인증 오류" in err, True))

    print("\n[5] XML이 아닌 HTML 오류 페이지도 인증 오류로 읽는다")
    install(HTML_ERROR)
    _, err = re_auto.fetch_apt_trades("11680", "202608")
    results.append(check("인증 오류로 분류", err is not None and "인증 오류" in err, True))

    print("\n[6] 일시적 오류(99)는 차단하지 않는다 — 다음 달 조회를 계속한다")
    calls = install(TRANSIENT)
    _, err = re_auto.fetch_apt_trades("11680", "202608")
    results.append(check("오류 사유는 남김", err is not None, True))
    results.append(check("차단되지 않음", re_auto._api_blocked, None))
    re_auto.fetch_apt_trades("11680", "202607")
    results.append(check("다음 호출이 실제로 나감", calls["n"], 2))

    print("\n[7] 차단되면 get_recent_trades가 24개월을 돌지 않는다")
    calls = install(BAD_KEY)
    matched, errors = re_auto.get_recent_trades("11680", "역삼동", 84.97, 5)
    results.append(check("매칭 거래 없음", len(matched), 0))
    results.append(check("오류 사유 전달됨", len(errors) >= 1, True))
    results.append(check("HTTP 호출 1회로 끝남", calls["n"], 1))

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
