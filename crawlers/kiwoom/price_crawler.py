"""
일봉 데이터 크롤러
- 종목별 일봉 조회 (OPT10081)
- 매일 장 마감 후 실행
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from .api import KiwoomAPI
import time


class PriceCrawler:
    """일봉 데이터 크롤러"""

    # TR 요청 간 대기 시간 (초) - 키움 API 제한: 조회TR 3.6초/1회
    REQUEST_INTERVAL = 3.7

    def __init__(self, api: KiwoomAPI):
        self.api = api

    def get_daily_price(
        self,
        stock_code: str,
        start_date: Optional[str] = None,
        count: int = 100
    ) -> List[Dict]:
        """
        종목 일봉 데이터 조회

        Args:
            stock_code: 종목코드 (6자리)
            start_date: 조회 시작일 (YYYYMMDD), 기본값 오늘
            count: 조회할 일수

        Returns:
            [{
                "date": "2024-01-15",
                "open": 10000,
                "high": 10500,
                "low": 9800,
                "close": 10200,
                "volume": 1234567,
                "trading_value": 12345678900
            }, ...]
        """
        if start_date is None:
            start_date = datetime.now().strftime("%Y%m%d")

        prices = []

        def handler(tr_code, rq_name):
            result = []
            data_count = min(self.api._get_repeat_cnt(tr_code, rq_name), count)

            for i in range(data_count):
                date_str = self.api._get_comm_data(tr_code, rq_name, i, "일자")
                if not date_str:
                    continue
                # YYYYMMDD -> YYYY-MM-DD
                date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                price = {
                    "date": date_formatted,
                    "open": abs(int(self.api._get_comm_data(tr_code, rq_name, i, "시가") or 0)),
                    "high": abs(int(self.api._get_comm_data(tr_code, rq_name, i, "고가") or 0)),
                    "low": abs(int(self.api._get_comm_data(tr_code, rq_name, i, "저가") or 0)),
                    "close": abs(int(self.api._get_comm_data(tr_code, rq_name, i, "현재가") or 0)),
                    "volume": abs(int(self.api._get_comm_data(tr_code, rq_name, i, "거래량") or 0)),
                    "trading_value": abs(int(self.api._get_comm_data(tr_code, rq_name, i, "거래대금") or 0)) * 1000000,  # 백만원 단위 -> 원
                }
                result.append(price)
            return result

        self.api.set_input_value("종목코드", stock_code)
        self.api.set_input_value("기준일자", start_date)
        self.api.set_input_value("수정주가구분", "1")  # 수정주가 적용

        result = self.api.comm_rq_data("일봉조회", "OPT10081", 0, "2000", handler=handler)

        if result and "result" in result:
            prices = result["result"]

        return prices

    def crawl_stocks(
        self,
        stock_codes: List[str],
        days: int = 70  # 9주 + 여유
    ) -> Dict[str, List[Dict]]:
        """
        여러 종목 일봉 데이터 크롤링

        Args:
            stock_codes: 종목코드 리스트
            days: 조회할 일수 (기본 70일 = 약 10주)

        Returns:
            {"종목코드": [일봉 데이터 리스트], ...}
        """
        result = {}
        total = len(stock_codes)

        for i, code in enumerate(stock_codes):
            if (i + 1) % 100 == 0:
                print(f"[{i + 1}/{total}] 일봉 조회 중...")

            prices = self.get_daily_price(code, count=days)
            result[code] = prices

            # API 요청 제한 준수
            time.sleep(self.REQUEST_INTERVAL)

        print(f"일봉 크롤링 완료: {total}개 종목")
        return result

    def crawl_today(self, stock_codes: List[str]) -> Dict[str, Dict]:
        """
        오늘 데이터만 크롤링 (일일 갱신용)

        Args:
            stock_codes: 종목코드 리스트

        Returns:
            {"종목코드": {오늘 일봉 데이터}, ...}
        """
        result = {}
        total = len(stock_codes)

        for i, code in enumerate(stock_codes):
            if (i + 1) % 100 == 0:
                print(f"[{i + 1}/{total}] 진행 중...")

            prices = self.get_daily_price(code, count=1)
            if prices:
                result[code] = prices[0]

            time.sleep(self.REQUEST_INTERVAL)

        print(f"오늘 일봉 크롤링 완료: {len(result)}개 종목")
        return result


if __name__ == "__main__":
    # 테스트
    api = KiwoomAPI()

    if api.login():
        crawler = PriceCrawler(api)

        # 삼성전자 일봉 테스트
        prices = crawler.get_daily_price("005930", count=10)
        print("\n삼성전자 최근 10일 일봉:")
        for p in prices[:5]:
            print(f"  {p['date']}: 종가 {p['close']:,}원, 거래대금 {p['trading_value']:,}원")
    else:
        print("로그인 실패")
