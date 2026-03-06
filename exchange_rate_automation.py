"""
환율 자동화 스크립트 v2.0
==========================
- exchangerate-api.com (무료, 인증 불필요)에서 USD/KRW 환율 조회
- 한국은행 ECOS API를 백업으로 사용
- 노션 환율정보 DB에 저장
- 매주 토요일 12:00 (KST) GitHub Actions에서 실행
"""

import os
import requests
from datetime import datetime, timedelta
import pytz 

# ============================
# 설정
# ============================
BOK_API_KEY   = os.environ.get("BOK_API_KEY")
NOTION_TOKEN  = os.environ.get("NOTION_TOKEN")
NOTION_DB_ID  = "31a64e13bb4680a491b8c1c2ca7770bc"
NOTION_VERSION = "2022-06-28"
KST = pytz.timezone("Asia/Seoul")


def log(msg):
      now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
      print(f"[{now}] {msg}")


# ============================
# STEP 1. 환율 조회 (메인: exchangerate-api)
# ============================
def get_exchange_rate_primary():
      """
          exchangerate-api.com 무료 엔드포인트로 USD/KRW 조회
              가입 불필요, 일 1500회 무료
                  """
      url = "https://open.er-api"""
  환율 자동화 스크립트 v2.0
==========================
- exchangerate-api.com (무료, 인증 불필요)에서 USD/KRW 환율 조회
- 한국은행 ECOS API를 백업으로 사용
- 노션 환율정보 DB에 저장
- 매주 토요일 12:00 (KST) GitHub Actions에서 실행
"""

import os
import requests
from datetime import datetime, timedelta
import pytz

# ============================
# 설정
# ============================
BOK_API_KEY   = os.environ.get("BOK_API_KEY")
NOTION_TOKEN  = os.environ.get("NOTION_TOKEN")
NOTION_DB_ID  = "31a64e13bb4680a491b8c1c2ca7770bc"
NOTION_VERSION = "2022-06-28"
KST = pytz.timezone("Asia/Seoul")


def log(msg):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] {msg}")


        # ============================
        # STEP 1. 환율 조회 (메인: exchangerate-api)
        # ============================
        def get_exchange_rate_primary():
    """
    exchangerate-api.com 무료 엔드포인트로 USD/KRW 조회
   
