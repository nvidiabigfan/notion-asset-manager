#!/usr/bin/env python3
"""
노션 자산관리 자동화 시스템
- 매주 토요일 12:00 (한국시간) 실행
- 부동산 실거래가, 환율정보, 자산평가 결과 자동 업데이트
"""

import requests
import json
import os
from datetime import datetime
from typing import Dict, List, Optional

class NotionAssetManager:
    def __init__(self, config_path: str = "config.json"):
        """설정 파일에서 필요한 정보 로드"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.token = self.config['notion_token']
        self.dbs = self.config['databases']
        self.api_version = self.config['api_version']
        
        # HTTP 헤더 설정
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": self.api_version
        }
        
        self.base_url = "https://api.notion.com/v1"
        
    def log(self, message: str):
        """로그 출력"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    # ============================================================
    # 1. 부동산 실거래가 DB 관리
    # ============================================================
    
    def add_real_estate_transaction(self, address: str, transaction_date: str, 
                                   amount: int, source: str = "국토부", note: str = ""):
        """부동산 실거래가 항목 추가"""
        db_id = self.dbs['real_estate']
        
        payload = {
            "parent": {"database_id": db_id},
            "properties": {
                "지번/주소": {
                    "title": [{"text": {"content": address}}]
                },
                "거래일자": {
                    "date": {"start": transaction_date}
                },
                "거래금액": {
                    "number": amount
                },
                "출처": {
                    "rich_text": [{"text": {"content": source}}]
                },
                "비고": {
                    "rich_text": [{"text": {"content": note}}]
                }
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/pages",
                json=payload,
                headers=self.headers
            )
            
            if response.status_code == 200:
                self.log(f"✅ 부동산 거래 추가: {address} ({transaction_date})")
                return True
            else:
                self.log(f"❌ 부동산 거래 추가 실패: {response.text}")
                return False
                
        except Exception as e:
            self.log(f"❌ 오류: {str(e)}")
            return False
    
    # ============================================================
    # 2. 환율정보 DB 관리
    # ============================================================
    
    def get_exchange_rate(self) -> Optional[float]:
        """한국은행 API에서 현재 환율 조회"""
        try:
            # 한국은행 OpenAPI (API KEY 필요)
            # 이 예제는 고정값으로 처리, 실제 사용시 API KEY 추가 필요
            
            # 임시: 고정값 반환 (실제 운영시 한국은행 API 연동)
            # rate = self._fetch_from_bok_api()
            
            self.log("⚠️  환율 조회: 실제 API 연동 필요 (한국은행 OpenAPI)")
            return None
            
        except Exception as e:
            self.log(f"❌ 환율 조회 오류: {str(e)}")
            return None
    
    def add_exchange_rate(self, rate: float, source: str = "한국은행"):
        """환율정보 추가"""
        db_id = self.dbs['exchange_rate']
        today = datetime.now().strftime("%Y-%m-%d")
        
        payload = {
            "parent": {"database_id": db_id},
            "properties": {
                "조회일자": {
                    "title": [{"text": {"content": today}}]
                },
                "USD/KRW 환율": {
                    "number": rate
                },
                "출처": {
                    "rich_text": [{"text": {"content": source}}]
                }
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/pages",
                json=payload,
                headers=self.headers
            )
            
            if response.status_code == 200:
                self.log(f"✅ 환율정보 추가: USD/KRW {rate}")
                return True
            else:
                self.log(f"❌ 환율정보 추가 실패: {response.text}")
                return False
                
        except Exception as e:
            self.log(f"❌ 오류: {str(e)}")
            return False
    
    # ============================================================
    # 3. 자산보유현황 DB 조회
    # ============================================================
    
    def get_asset_holdings(self) -> List[Dict]:
        """자산보유현황 DB의 모든 자산 조회"""
        db_id = self.dbs['asset_holdings']
        
        payload = {
            "database_id": db_id,
            "sorts": [{"property": "자산분류", "direction": "ascending"}]
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/databases/{db_id}/query",
                json=payload,
                headers=self.headers
            )
            
            if response.status_code == 200:
                results = response.json().get('results', [])
                self.log(f"✅ 자산보유현황 조회: {len(results)}개 자산")
                return results
            else:
                self.log(f"❌ 자산보유현황 조회 실패: {response.text}")
                return []
                
        except Exception as e:
            self.log(f"❌ 오류: {str(e)}")
            return []
    
    # ============================================================
    # 4. 자산평가 결과 DB 관리
    # ============================================================
    
    def add_valuation_result(self, evaluation_date: str, asset_name: str, 
                            asset_type: str, quantity: float, current_price: float,
                            evaluation_amount: float, reference_value: str = ""):
        """자산평가 결과 추가"""
        db_id = self.dbs['asset_valuation']
        
        payload = {
            "parent": {"database_id": db_id},
            "properties": {
                "평가일자": {
                    "title": [{"text": {"content": evaluation_date}}]
                },
                "자산명": {
                    "rich_text": [{"text": {"content": asset_name}}]
                },
                "자산분류": {
                    "select": {"name": asset_type}
                },
                "수량/금액": {
                    "number": quantity
                },
                "현재가": {
                    "number": current_price
                },
                "평가액": {
                    "number": evaluation_amount
                },
                "참고값": {
                    "rich_text": [{"text": {"content": reference_value}}]
                }
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/pages",
                json=payload,
                headers=self.headers
            )
            
            if response.status_code == 200:
                self.log(f"✅ 자산평가 결과 추가: {asset_name} ({asset_type})")
                return True
            else:
                self.log(f"❌ 자산평가 결과 추가 실패: {response.text}")
                return False
                
        except Exception as e:
            self.log(f"❌ 오류: {str(e)}")
            return False
    
    # ============================================================
    # 5. 메인 실행 함수
    # ============================================================
    
    def run_daily_update(self):
        """매일 실행되는 업데이트 함수"""
        self.log("=" * 70)
        self.log("🚀 노션 자산관리 자동화 시작")
        self.log("=" * 70)
        
        # 1. 현재 자산 조회
        assets = self.get_asset_holdings()
        
        # 2. 환율 정보 조회 (필요시)
        # exchange_rate = self.get_exchange_rate()
        
        # 3. 자산평가 결과 생성
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 예제: 자산보유현황에서 조회한 자산들을 평가
        if assets:
            for asset in assets:
                # 자산 정보 추출 (필드명은 당신의 DB 구조에 맞게 수정)
                try:
                    props = asset['properties']
                    # 실제 구현: 각 필드에서 값 추출 후 평가액 계산
                    self.log(f"📊 자산 처리: {props}")
                except Exception as e:
                    self.log(f"⚠️  자산 처리 오류: {str(e)}")
        
        self.log("=" * 70)
        self.log("✅ 자동화 완료")
        self.log("=" * 70)


def main():
    """메인 실행 함수"""
    try:
        manager = NotionAssetManager('config.json')
        manager.run_daily_update()
        
    except FileNotFoundError:
        print("❌ config.json 파일을 찾을 수 없습니다.")
        print("config.json 파일을 생성하고 필요한 정보를 입력하세요.")
        exit(1)
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
