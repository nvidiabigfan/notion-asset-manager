"""
generate_dashboard.py
자산평가결과 DB + 주간자산요약 DB → docs/index.html (Plotly 대시보드)

charts:
  1. 요약 카드   : 총 평가액 / 총 손익 / 수익률 / 주간 변동
  2. 보유자 카드 : 승민 / 민경 개별 평가액·수익률
  3. 보유자 파이 : 승민·민경 자산구성 (나란히)
  4. 주간 변동률 : 자산분류별 WoW % change 바 차트
  5. 정규화 추이 : 자산분류별 100 기준 상대 성과
  6. 절대 추이   : 자산분류별 총평가액 (백만원)
  7. 수익률 바   : 종목별 수익률 %
  8. 평가액 바   : 종목별 현재 평가액 vs 매수원가
"""

import os
import json
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── 환경 변수 ─────────────────────────────────────────────
NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
DB_EVAL_RESULT    = os.environ["DB_EVAL_RESULT"]
DB_WEEKLY_SUMMARY = os.environ["DB_WEEKLY_SUMMARY"]

KST = timezone(timedelta(hours=9))

HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type":   "application/json",
}

CATEGORY_COLOR = {
    "한국주식": "#4C9BE8",
    "미국주식": "#F4A261",
    "암호화폐": "#A8DADC",
    "부동산":   "#E76F51",
    "연금":     "#81B29A",
    "예적금":   "#9B89C4",
}

OWNER_COLOR = {
    "승민": "#7B68EE",
    "민경": "#FF7F7F",
}

NOTION_CALL_INTERVAL = 0.4


# ── Notion API 헬퍼 ───────────────────────────────────────
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
            print(f"  [RATE LIMIT] {retry_after}초 대기...")
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


# ── 1. 최신 평가일자 조회 ─────────────────────────────────
def get_latest_eval_date() -> str:
    rows = query_db(
        DB_EVAL_RESULT,
        sorts=[{"property": "평가일자", "direction": "descending"}],
    )
    if not rows:
        raise ValueError("DB_EVAL_RESULT 데이터 없음")
    return get_prop(rows[0], "평가일자")


# ── 2. 최신 종목별 데이터 조회 ───────────────────────────
def fetch_latest_holdings(eval_date: str) -> list[dict]:
    rows = query_db(
        DB_EVAL_RESULT,
        filter_body={"property": "평가일자", "title": {"equals": eval_date}},
    )
    holdings = []
    for row in rows:
        category    = get_prop(row, "자산분류")
        name        = get_prop(row, "자산명")
        eval_amount = get_prop(row, "평가액")
        buy_amount  = get_prop(row, "금액")
        current_price = get_prop(row, "현재가")
        quantity    = get_prop(row, "수량")
        owner       = get_prop(row, "보유자")

        if not name or eval_amount is None:
            continue

        holdings.append({
            "name":          name,
            "category":      category or "기타",
            "eval_amount":   float(eval_amount),
            "buy_amount":    float(buy_amount) if buy_amount is not None else None,
            "current_price": float(current_price) if current_price is not None else None,
            "quantity":      float(quantity) if quantity is not None else None,
            "owner":         owner or "미분류",
        })
    return holdings


# ── 3. 주간 히스토리 조회 ─────────────────────────────────
def fetch_weekly_history() -> list[dict]:
    rows = query_db(
        DB_WEEKLY_SUMMARY,
        sorts=[{"property": "평가일자", "direction": "ascending"}],
    )
    history = []
    for row in rows:
        date     = get_prop(row, "평가일자")
        category = get_prop(row, "자산분류")
        total    = get_prop(row, "총평가액")
        change_r = get_prop(row, "변동율")

        if not date or not category or total is None:
            continue
        if category == "전체":
            continue

        history.append({
            "date":        date,
            "category":    category,
            "total":       float(total) * 1e8,
            "change_rate": float(change_r) if change_r is not None else None,
        })
    return history


# ── 4. HTML 생성 ──────────────────────────────────────────
def build_html(eval_date: str, holdings: list[dict], history: list[dict]) -> str:

    # ── 전체 집계 ──────────────────────────────────────────
    total_eval = sum(h["eval_amount"] for h in holdings)
    total_buy  = sum(h["buy_amount"]  for h in holdings if h["buy_amount"] is not None)
    total_pnl  = total_eval - total_buy if total_buy else None
    total_rate = (total_pnl / total_buy * 100) if total_buy else None

    cat_totals = defaultdict(float)
    for h in holdings:
        cat_totals[h["category"]] += h["eval_amount"]

    weekly_change = None
    for row in sorted(history, key=lambda x: x["date"], reverse=True):
        if row.get("change_rate") is not None:
            weekly_change = row["change_rate"]
            break

    # ── 보유자별 집계 ──────────────────────────────────────
    owner_eval = defaultdict(float)
    owner_buy  = defaultdict(float)
    owner_cats = defaultdict(lambda: defaultdict(float))

    for h in holdings:
        o = h["owner"]
        owner_eval[o] += h["eval_amount"]
        if h["buy_amount"]:
            owner_buy[o] += h["buy_amount"]
        owner_cats[o][h["category"]] += h["eval_amount"]

    # ── 히스토리 (자산분류별) ──────────────────────────────
    hist_by_cat: dict[str, dict] = defaultdict(lambda: {"dates": [], "totals": []})
    for row in history:
        hist_by_cat[row["category"]]["dates"].append(row["date"])
        hist_by_cat[row["category"]]["totals"].append(round(row["total"] / 1e6, 1))

    # ── WoW 주간 변동률 계산 ───────────────────────────────
    wow_by_cat: dict[str, list] = {}
    for cat, data in hist_by_cat.items():
        dates  = data["dates"]
        totals = data["totals"]
        wows   = []
        for i in range(1, len(totals)):
            prev = totals[i - 1]
            if prev and prev != 0:
                pct = round((totals[i] - prev) / prev * 100, 2)
            else:
                pct = 0.0
            wows.append({"date": dates[i], "pct": pct})
        wow_by_cat[cat] = wows

    # ── 정규화 추이 (첫 주 = 100) ──────────────────────────
    norm_traces = []
    for cat, data in hist_by_cat.items():
        dates  = data["dates"]
        totals = data["totals"]
        if not totals or totals[0] == 0:
            continue
        base   = totals[0]
        normed = [round(v / base * 100, 2) for v in totals]
        color  = CATEGORY_COLOR.get(cat, "#cccccc")
        norm_traces.append({
            "type": "scatter", "mode": "lines+markers",
            "name": cat, "x": dates, "y": normed,
            "line": {"color": color, "width": 2},
            "hovertemplate": "%{x}<br>" + cat + ": %{y:.1f}<extra></extra>",
        })

    # ── 숫자 포맷 ──────────────────────────────────────────
    def fmt_won(v):
        if v is None:
            return "-"
        if abs(v) >= 1e8:
            return f"{v/1e8:,.1f}억"
        if abs(v) >= 1e4:
            return f"{v/1e4:,.0f}만"
        return f"{v:,.0f}"

    def fmt_pct(v, sign=False):
        if v is None:
            return "-"
        prefix = "+" if sign and v > 0 else ""
        return f"{prefix}{v:.2f}%"

    pnl_color    = "#2ECC71" if total_pnl  and total_pnl  >= 0 else "#E74C3C"
    rate_color   = "#2ECC71" if total_rate and total_rate >= 0 else "#E74C3C"
    weekly_color = "#2ECC71" if weekly_change and weekly_change >= 0 else "#E74C3C"
    now_str      = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")

    # ── 보유자 카드 HTML ───────────────────────────────────
    owner_cards_html = ""
    for owner in ["승민", "민경"]:
        o_eval = owner_eval.get(owner, 0)
        o_buy  = owner_buy.get(owner, 0)
        o_pnl  = o_eval - o_buy if o_buy else None
        o_rate = (o_pnl / o_buy * 100) if o_buy else None
        o_color = OWNER_COLOR.get(owner, "#aaa")
        pnl_c  = "#2ECC71" if o_pnl and o_pnl >= 0 else "#E74C3C"
        rate_c = "#2ECC71" if o_rate and o_rate >= 0 else "#E74C3C"
        owner_cards_html += f"""
  <div class="card owner-card" style="border-color:{o_color}55">
    <div class="card-label" style="color:{o_color}">{owner}</div>
    <div class="card-value" style="color:{o_color}">{fmt_won(o_eval)}</div>
    <div class="card-sub">손익 <span style="color:{pnl_c}">{fmt_won(o_pnl)}</span>
      &nbsp;|&nbsp; 수익률 <span style="color:{rate_c}">{fmt_pct(o_rate, sign=True)}</span></div>
  </div>"""

    # ── Plotly JSON 생성 ───────────────────────────────────

    # 보유자별 파이 데이터
    def make_owner_pie(owner: str) -> str:
        cats   = owner_cats.get(owner, {})
        labels = list(cats.keys())
        values = [cats[k] / 1e6 for k in labels]
        colors = [CATEGORY_COLOR.get(k, "#cccccc") for k in labels]
        return json.dumps({
            "data": [{
                "type": "pie", "labels": labels, "values": values,
                "marker": {"colors": colors},
                "textinfo": "label+percent",
                "hovertemplate": "%{label}<br>%{value:,.0f}백만원<br>%{percent}<extra></extra>",
            }],
            "layout": {
                "margin": {"t": 10, "b": 10, "l": 10, "r": 10},
                "showlegend": True, "legend": {"orientation": "h", "y": -0.15},
                "paper_bgcolor": "#1a1a2e", "font": {"color": "#e0e0e0"},
            }
        })

    pie_sm = make_owner_pie("승민")
    pie_mk = make_owner_pie("민경")

    # 주간 변동률 바 차트
    wow_traces_json = []
    cat_order = ["미국주식", "한국주식", "암호화폐", "연금", "부동산"]
    for cat in cat_order + [c for c in wow_by_cat if c not in cat_order]:
        wows = wow_by_cat.get(cat, [])
        if not wows:
            continue
        bar_colors = ["#2ECC71" if w["pct"] >= 0 else "#E74C3C" for w in wows]
        wow_traces_json.append({
            "type": "bar",
            "name": cat,
            "x": [w["date"] for w in wows],
            "y": [w["pct"] for w in wows],
            "marker": {"color": bar_colors, "opacity": 0.85},
            "hovertemplate": "%{x}<br>" + cat + " WoW: %{y:.2f}%<extra></extra>",
            "visible": True if cat in ("미국주식", "한국주식", "암호화폐") else "legendonly",
        })

    wow_data = json.dumps({
        "data": wow_traces_json,
        "layout": {
            "barmode": "group",
            "margin": {"t": 10, "b": 50, "l": 60, "r": 20},
            "xaxis": {"title": "", "tickangle": -30},
            "yaxis": {"title": "변동률 (%)", "zeroline": True, "zerolinecolor": "#555",
                      "zerolinewidth": 2},
            "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
            "font": {"color": "#e0e0e0"},
            "legend": {"orientation": "h", "y": -0.3},
        }
    })

    # 정규화 추이
    norm_data = json.dumps({
        "data": norm_traces,
        "layout": {
            "margin": {"t": 10, "b": 50, "l": 60, "r": 20},
            "xaxis": {"title": ""},
            "yaxis": {"title": "기준 = 100", "zeroline": False},
            "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
            "font": {"color": "#e0e0e0"},
            "legend": {"orientation": "h", "y": -0.25},
            "shapes": [{
                "type": "line", "x0": 0, "x1": 1, "xref": "paper",
                "y0": 100, "y1": 100,
                "line": {"color": "#666", "width": 1, "dash": "dot"},
            }],
        }
    })

    # 절대 추이 라인
    hist_traces_json = []
    for cat, data in hist_by_cat.items():
        color = CATEGORY_COLOR.get(cat, "#cccccc")
        hist_traces_json.append({
            "type": "scatter", "mode": "lines+markers",
            "name": cat, "x": data["dates"], "y": data["totals"],
            "line": {"color": color, "width": 2},
            "hovertemplate": "%{x}<br>%{y:,.0f}백만원<extra>" + cat + "</extra>",
        })

    hist_data = json.dumps({
        "data": hist_traces_json,
        "layout": {
            "margin": {"t": 10, "b": 40, "l": 70, "r": 20},
            "xaxis": {"title": ""},
            "yaxis": {"title": "백만원"},
            "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
            "font": {"color": "#e0e0e0"},
            "legend": {"orientation": "h", "y": -0.25},
        }
    })

    # 수익률 바
    rate_items = [h for h in holdings if h["buy_amount"] and h["buy_amount"] > 0]
    rate_items.sort(key=lambda x: (x["eval_amount"] - x["buy_amount"]) / x["buy_amount"])
    rate_names  = [h["name"] for h in rate_items]
    rate_values = [round((h["eval_amount"] - h["buy_amount"]) / h["buy_amount"] * 100, 2) for h in rate_items]
    rate_colors = ["#E74C3C" if v < 0 else "#2ECC71" for v in rate_values]

    rate_data = json.dumps({
        "data": [{
            "type": "bar", "x": rate_values, "y": rate_names,
            "orientation": "h", "marker": {"color": rate_colors},
            "text": [fmt_pct(v, sign=True) for v in rate_values],
            "textposition": "outside",
            "hovertemplate": "%{y}<br>%{x:.2f}%<extra></extra>",
        }],
        "layout": {
            "margin": {"t": 10, "b": 30, "l": 120, "r": 80},
            "xaxis": {"title": "", "zeroline": True, "zerolinecolor": "#555"},
            "yaxis": {"automargin": True},
            "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
            "font": {"color": "#e0e0e0"},
        }
    })

    # 평가액 vs 매수원가
    eval_items  = sorted(holdings, key=lambda x: x["eval_amount"], reverse=True)
    eval_names  = [h["name"] for h in eval_items]
    eval_values = [round(h["eval_amount"] / 1e4) for h in eval_items]
    buy_values  = [round(h["buy_amount"] / 1e4) if h["buy_amount"] else 0 for h in eval_items]

    eval_data = json.dumps({
        "data": [
            {"type": "bar", "name": "매수원가", "x": eval_names, "y": buy_values,
             "marker": {"color": "#555577"},
             "hovertemplate": "%{x}<br>매수원가: %{y:,.0f}만원<extra></extra>"},
            {"type": "bar", "name": "평가액",   "x": eval_names, "y": eval_values,
             "marker": {"color": "#4C9BE8"},
             "hovertemplate": "%{x}<br>평가액: %{y:,.0f}만원<extra></extra>"},
        ],
        "layout": {
            "barmode": "group",
            "margin": {"t": 10, "b": 80, "l": 60, "r": 20},
            "xaxis": {"tickangle": -35, "automargin": True},
            "yaxis": {"title": "만원"},
            "paper_bgcolor": "#1a1a2e", "plot_bgcolor": "#16213e",
            "font": {"color": "#e0e0e0"},
            "legend": {"orientation": "h", "y": -0.35},
        }
    })

    # ── HTML ──────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>자산 대시보드</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: #0f0f23;
    color: #e0e0e0;
    font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", sans-serif;
    padding: 20px;
  }}
  h1 {{ font-size: 1.4rem; font-weight: 600; margin-bottom: 4px; color: #a0c4ff; }}
  .subtitle {{ font-size: 0.8rem; color: #777; margin-bottom: 16px; }}
  .cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
    margin-bottom: 16px;
  }}
  .card {{
    background: #1a1a2e;
    border-radius: 10px;
    padding: 16px;
    border: 1px solid #2a2a4a;
  }}
  .owner-card {{ border-width: 2px; }}
  .card-label {{ font-size: 0.75rem; color: #888; margin-bottom: 6px; }}
  .card-value {{ font-size: 1.5rem; font-weight: 700; }}
  .card-sub {{ font-size: 0.78rem; color: #999; margin-top: 4px; }}
  .section {{ margin-bottom: 20px; }}
  .charts {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
  }}
  .chart-box {{
    background: #1a1a2e;
    border-radius: 10px;
    padding: 14px;
    border: 1px solid #2a2a4a;
  }}
  .chart-title {{ font-size: 0.85rem; color: #aaa; margin-bottom: 6px; font-weight: 600; }}
  .chart-hint  {{ font-size: 0.72rem; color: #666; margin-bottom: 8px; }}
  .full-width  {{ grid-column: 1 / -1; }}
  @media (max-width: 700px) {{ .charts {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>

<h1>자산 포트폴리오 대시보드</h1>
<div class="subtitle">기준일: {eval_date} &nbsp;|&nbsp; 갱신: {now_str}</div>

<!-- ① 전체 요약 카드 -->
<div class="cards">
  <div class="card">
    <div class="card-label">총 평가액</div>
    <div class="card-value" style="color:#a0c4ff">{fmt_won(total_eval)}</div>
    <div class="card-sub">{total_eval:,.0f}원</div>
  </div>
  <div class="card">
    <div class="card-label">총 평가손익</div>
    <div class="card-value" style="color:{pnl_color}">{fmt_won(total_pnl)}</div>
    <div class="card-sub">매수원가 {fmt_won(total_buy)}</div>
  </div>
  <div class="card">
    <div class="card-label">전체 수익률</div>
    <div class="card-value" style="color:{rate_color}">{fmt_pct(total_rate, sign=True)}</div>
    <div class="card-sub">&nbsp;</div>
  </div>
  <div class="card">
    <div class="card-label">주간 변동율</div>
    <div class="card-value" style="color:{weekly_color}">{fmt_pct(weekly_change, sign=True)}</div>
    <div class="card-sub">직전 대비</div>
  </div>
</div>

<!-- ② 보유자별 카드 -->
<div class="cards">
{owner_cards_html}
</div>

<!-- ③ 보유자별 자산구성 -->
<div class="section">
  <div class="charts">
    <div class="chart-box">
      <div class="chart-title">승민 자산구성</div>
      <div id="pie_sm" style="height:260px"></div>
    </div>
    <div class="chart-box">
      <div class="chart-title">민경 자산구성</div>
      <div id="pie_mk" style="height:260px"></div>
    </div>
  </div>
</div>

<!-- ④ 주간 변동률 (WoW) -->
<div class="section">
  <div class="chart-box">
    <div class="chart-title">주차별 자산 변동률 (Week-over-Week %)</div>
    <div class="chart-hint">▶ 미국주식·한국주식·암호화폐 기본 표시 / 레전드 클릭으로 부동산·연금 토글</div>
    <div id="wow" style="height:320px"></div>
  </div>
</div>

<!-- ⑤ 정규화 성과 추이 -->
<div class="section">
  <div class="chart-box">
    <div class="chart-title">자산분류별 상대 성과 (첫 기준일 = 100)</div>
    <div class="chart-hint">▶ 부동산 규모 차이 제거 — 상대 수익률만 비교</div>
    <div id="norm" style="height:280px"></div>
  </div>
</div>

<!-- ⑥ 절대 추이 -->
<div class="section">
  <div class="chart-box">
    <div class="chart-title">자산분류별 평가액 추이 (백만원)</div>
    <div id="hist" style="height:280px"></div>
  </div>
</div>

<!-- ⑦⑧ 종목별 차트 -->
<div class="section">
  <div class="charts">
    <div class="chart-box">
      <div class="chart-title">종목별 수익률</div>
      <div id="rate" style="height:340px"></div>
    </div>
    <div class="chart-box full-width">
      <div class="chart-title">종목별 평가액 vs 매수원가 (만원)</div>
      <div id="eval" style="height:340px"></div>
    </div>
  </div>
</div>

<script>
const cfg = {{responsive: true, displayModeBar: false}};
Plotly.newPlot('pie_sm', {pie_sm}.data,  {pie_sm}.layout,  cfg);
Plotly.newPlot('pie_mk', {pie_mk}.data,  {pie_mk}.layout,  cfg);
Plotly.newPlot('wow',    {wow_data}.data, {wow_data}.layout, cfg);
Plotly.newPlot('norm',   {norm_data}.data, {norm_data}.layout, cfg);
Plotly.newPlot('hist',   {hist_data}.data, {hist_data}.layout, cfg);
Plotly.newPlot('rate',   {rate_data}.data, {rate_data}.layout, cfg);
Plotly.newPlot('eval',   {eval_data}.data, {eval_data}.layout, cfg);
</script>
</body>
</html>"""
    return html


# ── MAIN ──────────────────────────────────────────────────
def main():
    print("\n[Dashboard] 대시보드 생성 시작")

    print("[Dashboard] 최신 평가일자 조회...")
    eval_date = get_latest_eval_date()
    print(f"[Dashboard] 기준일: {eval_date}")

    print("[Dashboard] 종목별 데이터 조회...")
    holdings = fetch_latest_holdings(eval_date)
    print(f"[Dashboard] {len(holdings)}종목 로드")

    print("[Dashboard] 주간 히스토리 조회...")
    history = fetch_weekly_history()
    print(f"[Dashboard] 히스토리 {len(history)}행 로드")

    print("[Dashboard] HTML 생성...")
    html = build_html(eval_date, holdings, history)

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("[Dashboard] docs/index.html 저장 완료")


if __name__ == "__main__":
    main()
