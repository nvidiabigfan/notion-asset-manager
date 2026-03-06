"""
환율 자동화 스크립트 v1.0
==========================
- 한국은행 ECOS API에서 USD/KRW 환율 조회
- 노션 환율정보 DB에 저장
- 매주 토요일 12:00 (KST) GitHub Actions에서 실행

노션 환율정보 DB 컬럼:
  - 조회일자 (title): 스크립트 실행일 (예: 2026-03-07)
  - USD/KRW 환율 (number): 환율 수치 (예: 1450.50)
  - 기준일자 (text): 한국은행 데이터 기준일 (예: 2026-03-06, 토→금)
  - 출처 (text): "한국은행 OpenAPI (ECOS)"
"""

import os
import requests
import json
from datetime import datetime, timedelta
import pytz

# ============================
# 설정
# ============================
BOK_API_KEY = os.environ.get("BOK_API_KEY")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DB_ID = "31a64e13bb4680a491b8c1c2ca7770bc"  # 환율정보 DB

NOTION_API_VERSION = "2022-06-28"
KST = pytz.timezone("Asia/Seoul")


def log(msg):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


# ============================
# 1. 한국은행 API에서 환율 조회
# ============================
def get_exchange_rate():
    """
    한국은행 OpenAPI에서 USD/KRW 환율 조회
    토요일은 장이 없으므로 직전 평일(금요일) 기준으로 조회
    """
    today = datetime.now(KST)

    # 토요일(5) → 금요일, 일요일(6) → 금요일로 조정
    # 평일도 당일 데이터가 없을 수 있으므로 최대 7일 전까지 시도
    for days_back in range(1, 8):
        target_date = today - timedelta(days=days_back)
        date_str = target_date.strftime("%Y%m%d")

        url = (
            f"https://ecos.bok.or.kr/api/StatisticSearch/"
            f"{BOK_API_KEY}/json/kr/1/1/F001/D/{date_str}/{date_str}/H010"
        )

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "StatisticSearch" in data and "row" in data["StatisticSearch"]:
                rows = data["StatisticSearch"]["row"]
                if rows:
                    rate = float(rows[0]["DATA_VALUE"])
                    actual_date = rows[0]["TIME"]
                    formatted_date = f"{actual_date[:4]}-{actual_date[4:6]}-{actual_date[6:]}"
                    log(f"✅ 환율 조회 성공: USD/KRW {rate:,.2f} (기준일: {formatted_date})")
                    return rate, formatted_date

        except Exception as e:
            log(f"⚠️  {date_str} 환율 조회 실패: {e}")
            continue

    raise Exception("❌ 7일 이내 환율 데이터를 가져오지 못했습니다.")


# ============================
# 2. 노션 DB에 저장
# ============================
def save_to_notion(rate, rate_date):
    """
    환율 데이터를 노션 환율정보 DB에 저장
    """
    today_kst = datetime.now(KST).strftime("%Y-%m-%d")

    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_API_VERSION,
    }

    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "조회일자": {
                "title": [
                    {
                        "text": {
                            "content": today_kst
                        }
                    }
                ]
            },
            "USD/KRW 환율": {
                "number": rate
            },
            "기준일자": {
                "rich_text": [
                    {
                        "text": {
                            "content": rate_date
                        }
                    }
                ]
            },
            "출처": {
                "rich_text": [
                    {
                        "text": {
                            "content": "한국은행 OpenAPI (ECOS)"
                        }
                    }
                ]
            }
        }
    }

    response = requests.post(url, headers=headers, json=payload, timeout=10)

    if response.status_code == 200:
        log(f"✅ 노션 DB 저장 성공: USD/KRW {rate:,.2f} → {today_kst}")
    else:
        log(f"❌ 노션 저장 실패 (HTTP {response.status_code}): {response.text}")
        raise Exception(f"노션 API 오류: {response.status_code}")


# ============================
# 메인 실행
# ============================
def main():
    log("=" * 60)
    log("🚀 환율 자동화 시작")
    log("=" * 60)

    # 환경변수 확인
    if not BOK_API_KEY:
        raise Exception("❌ BOK_API_KEY 환경변수가 설정되지 않았습니다.")
    if not NOTION_TOKEN:
        raise Exception("❌ NOTION_TOKEN 환경변수가 설정되지 않았습니다.")

    # 1. 환율 조회
    log("📡 한국은행 API에서 환율 조회 중...")
    rate, rate_date = get_exchange_rate()

    # 2. 노션 저장
    log(f"📝 노션 DB에 저장 중...")
    save_to_notion(rate, rate_date)

    log("=" * 60)
    log("✅ 환율 자동화 완료")
    log("=" * 60)


if __name__ == "__main__":
    main()
