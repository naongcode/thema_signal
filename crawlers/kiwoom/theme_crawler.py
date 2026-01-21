"""
테마/종목 크롤러
- GetThemeGroupList / GetThemeGroupCode 사용
- 주 1회 실행 (주말)
"""
from typing import List, Dict
from .api import KiwoomAPI
import time


class ThemeCrawler:
    """테마/종목 데이터 크롤러"""

    # TR 요청 간 대기 시간 (초) - 키움 API 제한
    REQUEST_INTERVAL = 0.5

    def __init__(self, api: KiwoomAPI):
        self.api = api

    def get_theme_list(self) -> List[Dict]:
        """
        전체 테마 목록 조회 (GetThemeGroupList 사용)

        Returns:
            [{"code": "141", "name": "2차전지_소재(양극화물질등)"}, ...]
        """
        themes = []

        # nType=1: 테마코드|테마명;테마코드|테마명;... 형식으로 반환
        result = self.api.ocx.dynamicCall("GetThemeGroupList(int)", 1)

        if not result:
            print("테마 목록 조회 실패")
            return themes

        # 파싱: "141|2차전지_소재;140|2차전지_완제품;..."
        theme_items = result.split(";")
        for item in theme_items:
            if "|" in item:
                code, name = item.split("|", 1)
                themes.append({
                    "code": code.strip(),
                    "name": name.strip()
                })

        print(f"테마 {len(themes)}개 조회 완료")
        return themes

    def get_theme_stocks(self, theme_code: str) -> List[Dict]:
        """
        테마별 종목 목록 조회 (GetThemeGroupCode 사용)

        Args:
            theme_code: 테마 코드

        Returns:
            [{"code": "373220", "name": "LG에너지솔루션", "market": "KOSPI"}, ...]
        """
        stocks = []

        # 테마코드로 종목코드 목록 조회
        result = self.api.ocx.dynamicCall("GetThemeGroupCode(QString)", theme_code)

        if not result:
            return stocks

        # 파싱: "A005930;A000660;..." 형식
        stock_codes = result.split(";")

        for code in stock_codes:
            if not code:
                continue

            # 종목코드 앞자리로 시장 구분 (A: KOSPI, J/Q: KOSDAQ)
            market = "KOSDAQ" if code.startswith("J") or code.startswith("Q") else "KOSPI"
            # 알파벳 접두사 제거
            clean_code = code.lstrip("AJQ")

            if clean_code:
                # 종목명 조회
                name = self.api.ocx.dynamicCall(
                    "GetMasterCodeName(QString)", clean_code
                )

                stocks.append({
                    "code": clean_code,
                    "name": name.strip() if name else "",
                    "market": market
                })

        return stocks

    def crawl_all(self) -> Dict:
        """
        전체 테마/종목 데이터 크롤링

        Returns:
            {
                "themes": [{"code": "141", "name": "2차전지_소재"}, ...],
                "theme_stocks": {"141": [{"code": "373220", ...}, ...], ...}
            }
        """
        data = {
            "themes": [],
            "theme_stocks": {}
        }

        # 1. 테마 목록 조회
        themes = self.get_theme_list()
        data["themes"] = themes

        # 2. 각 테마별 종목 조회
        for i, theme in enumerate(themes):
            theme_code = theme["code"]
            print(f"[{i + 1}/{len(themes)}] {theme['name']} 종목 조회 중...")

            stocks = self.get_theme_stocks(theme_code)
            data["theme_stocks"][theme_code] = stocks

            # API 요청 제한 준수
            time.sleep(self.REQUEST_INTERVAL)

        print(f"크롤링 완료: 테마 {len(themes)}개")
        return data


if __name__ == "__main__":
    # 테스트
    api = KiwoomAPI()

    if api.login():
        crawler = ThemeCrawler(api)
        data = crawler.crawl_all()

        # 결과 확인
        print(f"\n총 테마 수: {len(data['themes'])}")
        for theme in data["themes"][:5]:
            stocks = data["theme_stocks"].get(theme["code"], [])
            print(f"  - {theme['name']}: {len(stocks)}개 종목")
            if stocks:
                print(f"    예: {stocks[0]['name']} ({stocks[0]['code']})")
    else:
        print("로그인 실패")
