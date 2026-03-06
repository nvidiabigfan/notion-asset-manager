import os
import requests
from datetime import datetime, timedelta
import pytz

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DB_ID = "31a64e13bb4680a491b8c1c2ca7770bc"
NOTION_VERSION = "2022-06-28"
KST = pytz.timezone("Asia/Seoul")


def log(msg):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


def get_exchange_rate():
    url = "https://open.er-api.com/v6/latest/USD"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    if data.get("result") == "success":
        rate = data["rates"]["KRW"]
        today = datetime.now(KST).strftime("%Y-%m-%d")
        log(f"환율 조회 성공: USD/KRW {rate:,.2f}")
        return rate, today
    raise Exception("환율 조회 실패")


def save_to_notion(rate, rate_date):
    today = datetime.now(KST).strftime("%Y-%m-%d")
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }
    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "조회일자": {"title": [{"text": {"content": today}}]},
            "USD/KRW 환율": {"number": rate},
            "기준일자": {"rich_text": [{"text": {"content": rate_date}}]},
            "출처": {"rich_text": [{"text": {"content": "exchangerate-api.com"}}]},
        },
    }
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers,
        json=payload,
        timeout=10
    )
    if r.status_code == 200:
        log(f"노션 저장 완료: {rate:,.2f}")
    else:
        raise Exception(f"Notion 오류 {r.status_code}: {r.text}")


def main():
    log("===== 환율 자동화 시작 =====")
    if not NOTION_TOKEN:
        raise Exception("NOTION_TOKEN 없음")
    rate, rate_date = get_exchange_rate()
    save_to_notion(rate, rate_date)
    log("===== 완료 =====")


if __name__ == "__main__":
    main()
