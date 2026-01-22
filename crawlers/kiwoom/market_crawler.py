"""
시장 데이터 크롤러
- 시총, 주식수, PER, PBR 조회 (OPT10001)
- 매일 장 마감 후 실행
"""
from typing import List, Dict, Optional
from .api import KiwoomAPI
import time


class MarketCrawler:
    """시장 데이터 크롤러 (시총, 주식수, PER, PBR)"""

    # TR 요청 간 대기 시간 (초) - 키움 API 제한: 조회TR 3.6초/1회
    REQUEST_INTERVAL = 3.7

    def __init__(self, api: KiwoomAPI):
        self.api = api

    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        """
        종목 기본정보 조회 (OPT10001)

        Args:
            stock_code: 종목코드 (6자리)

        Returns:
            {
                "market_cap": 420000000000000,  # 시가총액 (원)
                "shares": 5969782550,            # 상장주식수
                "per": 12.5,
                "pbr": 1.2
            }
        """
        def handler(tr_code, rq_name):
            try:
                # 시가총액 (억원 단위로 반환됨 -> 원으로 변환)
                market_cap_str = self.api._get_comm_data(tr_code, rq_name, 0, "시가총액")
                market_cap = int(market_cap_str or 0) * 100000000  # 억원 -> 원

                # 상장주식수
                shares_str = self.api._get_comm_data(tr_code, rq_name, 0, "상장주식")
                shares = int(shares_str.replace(",", "") or 0)

                # PER
                per_str = self.api._get_comm_data(tr_code, rq_name, 0, "PER")
                per = float(per_str or 0)

                # PBR
                pbr_str = self.api._get_comm_data(tr_code, rq_name, 0, "PBR")
                pbr = float(pbr_str or 0)

                return {
                    "market_cap": market_cap,
                    "shares": shares,
                    "per": per,
                    "pbr": pbr
                }
            except (ValueError, AttributeError) as e:
                print(f"종목 {stock_code} 파싱 에러: {e}")
                return None

        self.api.set_input_value("종목코드", stock_code)
        result = self.api.comm_rq_data("주식기본정보요청", "OPT10001", 0, "0101", handler=handler)

        if result and "result" in result:
            return result["result"]
        return None

    def crawl_stocks(self, stock_codes: List[str]) -> Dict[str, Dict]:
        """
        여러 종목 시장 데이터 크롤링

        Args:
            stock_codes: 종목코드 리스트

        Returns:
            {
                "005930": {"market_cap": ..., "shares": ..., "per": ..., "pbr": ...},
                ...
            }
        """
        result = {}
        total = len(stock_codes)

        for i, code in enumerate(stock_codes):
            if (i + 1) % 100 == 0:
                print(f"[{i + 1}/{total}] 시장 데이터 조회 중...")

            info = self.get_stock_info(code)
            if info:
                result[code] = info

            # API 요청 제한 준수
            time.sleep(self.REQUEST_INTERVAL)

        print(f"시장 데이터 크롤링 완료: {len(result)}개 종목")
        return result


if __name__ == "__main__":
    # 테스트
    api = KiwoomAPI()

    if api.login():
        crawler = MarketCrawler(api)

        # 삼성전자 테스트
        info = crawler.get_stock_info("005930")
        if info:
            print("\n삼성전자 시장 데이터:")
            print(f"  시가총액: {info['market_cap']:,}원")
            print(f"  상장주식수: {info['shares']:,}주")
            print(f"  PER: {info['per']}")
            print(f"  PBR: {info['pbr']}")
    else:
        print("로그인 실패")
