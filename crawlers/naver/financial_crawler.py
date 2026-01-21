"""
재무 데이터 크롤러 (네이버 금융)
- 매출, 영업이익 조회
- 분기 1회 실행
"""
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import time
import re


class FinancialCrawler:
    """재무 데이터 크롤러 (네이버 금융)"""

    # 요청 간 대기 시간 (초)
    REQUEST_INTERVAL = 0.3

    # 네이버 금융 재무정보 URL
    BASE_URL = "https://finance.naver.com/item/main.naver?code={code}"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    def get_financial_data(self, stock_code: str) -> Optional[Dict]:
        """
        종목 재무 데이터 조회

        Args:
            stock_code: 종목코드 (6자리)

        Returns:
            {
                "revenue": 79000000000000,        # 매출액 (원)
                "operating_profit": 9180000000000  # 영업이익 (원)
            }
        """
        url = self.BASE_URL.format(code=stock_code)

        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # 재무정보 테이블 찾기
            # 네이버 금융 메인 페이지의 주요 재무정보 영역
            table = soup.select_one("div.section.cop_analysis table")

            if not table:
                return None

            # 매출액, 영업이익 행 찾기
            revenue = None
            operating_profit = None

            rows = table.select("tr")
            for row in rows:
                th = row.select_one("th")
                if not th:
                    continue

                label = th.get_text(strip=True)
                tds = row.select("td")

                if not tds:
                    continue

                # 최근 분기 데이터 (첫 번째 td)
                value_text = tds[0].get_text(strip=True)
                value = self._parse_value(value_text)

                if "매출액" in label:
                    revenue = value
                elif "영업이익" in label:
                    operating_profit = value

            if revenue is not None or operating_profit is not None:
                return {
                    "revenue": revenue or 0,
                    "operating_profit": operating_profit or 0
                }

            return None

        except Exception as e:
            print(f"종목 {stock_code} 재무 조회 에러: {e}")
            return None

    def _parse_value(self, text: str) -> int:
        """
        금액 문자열 파싱 (억원 단위 -> 원 단위)

        Args:
            text: "79,000" (억원)

        Returns:
            79000000000000 (원)
        """
        try:
            # 쉼표 제거, 숫자만 추출
            cleaned = re.sub(r"[^\d\-.]", "", text)
            if not cleaned or cleaned == "-":
                return 0

            # 억원 단위를 원 단위로 변환
            value = float(cleaned) * 100000000
            return int(value)
        except ValueError:
            return 0

    def crawl_stocks(self, stock_codes: List[str]) -> Dict[str, Dict]:
        """
        여러 종목 재무 데이터 크롤링

        Args:
            stock_codes: 종목코드 리스트

        Returns:
            {
                "005930": {"revenue": ..., "operating_profit": ...},
                ...
            }
        """
        result = {}
        total = len(stock_codes)

        for i, code in enumerate(stock_codes):
            if (i + 1) % 100 == 0:
                print(f"[{i + 1}/{total}] 재무 데이터 조회 중...")

            data = self.get_financial_data(code)
            if data:
                result[code] = data

            time.sleep(self.REQUEST_INTERVAL)

        print(f"재무 데이터 크롤링 완료: {len(result)}개 종목")
        return result

    def get_current_quarter(self) -> str:
        """현재 분기 반환 (예: "2024-Q3")"""
        from datetime import datetime
        now = datetime.now()
        quarter = (now.month - 1) // 3 + 1
        # 실적 발표 지연 고려 (1분기 지연)
        if quarter == 1:
            return f"{now.year - 1}-Q4"
        else:
            return f"{now.year}-Q{quarter - 1}"


if __name__ == "__main__":
    # 테스트
    crawler = FinancialCrawler()

    # 삼성전자 테스트
    data = crawler.get_financial_data("005930")
    if data:
        print("\n삼성전자 재무 데이터:")
        print(f"  매출액: {data['revenue']:,}원")
        print(f"  영업이익: {data['operating_profit']:,}원")
    else:
        print("데이터 조회 실패")

    print(f"\n현재 분기: {crawler.get_current_quarter()}")
