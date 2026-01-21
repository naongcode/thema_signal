"""
크롤러 스케줄러
- 매일 15:40: 일봉 데이터 + 시장 데이터 수집
- 매주 토요일 10:00: 테마/종목 매핑 갱신
- 분기 1회: 재무 데이터 갱신
"""
import schedule
import time
from datetime import datetime
from kiwoom.api import KiwoomAPI
from kiwoom.theme_crawler import ThemeCrawler
from kiwoom.price_crawler import PriceCrawler
from kiwoom.market_crawler import MarketCrawler
from naver.financial_crawler import FinancialCrawler
import storage


def get_all_stock_codes() -> list:
    """저장된 종목 코드 목록 반환"""
    stocks = storage.load_stocks()
    return list(stocks.keys())


def run_daily_crawler():
    """매일 실행: 일봉 + 시장 데이터 수집"""
    print(f"\n[{datetime.now()}] 일별 크롤러 시작")
    today = datetime.now().strftime("%Y-%m-%d")

    api = KiwoomAPI()
    if not api.login():
        print("로그인 실패 - 크롤러 종료")
        return

    try:
        stock_codes = get_all_stock_codes()

        if not stock_codes:
            print("조회할 종목 없음 - 먼저 init 실행 필요")
            return

        # 1. 일봉 데이터 수집
        print(f"\n[1/2] 일봉 데이터 수집 ({len(stock_codes)}개 종목)")
        price_crawler = PriceCrawler(api)
        price_result = price_crawler.crawl_today(stock_codes)

        # 종가 + 거래대금만 추출하여 저장
        prices_to_save = {}
        for code, data in price_result.items():
            prices_to_save[code] = {
                "close": data["close"],
                "value": data["trading_value"]
            }
        storage.add_daily_prices(today, prices_to_save)

        # 2. 시장 데이터 수집 (시총, 주식수, PER, PBR)
        print(f"\n[2/2] 시장 데이터 수집 ({len(stock_codes)}개 종목)")
        market_crawler = MarketCrawler(api)
        market_result = market_crawler.crawl_stocks(stock_codes)
        storage.save_market(today, market_result)

        print(f"\n일별 크롤링 완료: 가격 {len(prices_to_save)}개, 시장 {len(market_result)}개")

    except Exception as e:
        print(f"일별 크롤러 에러: {e}")
    finally:
        api.disconnect()

    print(f"[{datetime.now()}] 일별 크롤러 종료")


def run_weekly_crawler():
    """주 1회 실행: 테마/종목 매핑 갱신"""
    print(f"\n[{datetime.now()}] 주간 크롤러 시작")

    api = KiwoomAPI()
    if not api.login():
        print("로그인 실패 - 크롤러 종료")
        return

    try:
        crawler = ThemeCrawler(api)
        data = crawler.crawl_all()

        # 1. 종목 기본정보 저장 (stocks.json)
        stocks = {}
        for theme_stocks in data["theme_stocks"].values():
            for stock in theme_stocks:
                stocks[stock["code"]] = {
                    "name": stock["name"],
                    "market": stock["market"]
                }
        storage.save_stocks(stocks)

        # 2. 테마 매핑 저장 (themes.json)
        themes = []
        for theme in data["themes"]:
            theme_code = theme["code"]
            stock_codes = [s["code"] for s in data["theme_stocks"].get(theme_code, [])]
            themes.append({
                "id": theme_code,
                "name": theme["name"],
                "stocks": stock_codes
            })
        storage.save_themes(themes)

        print(f"주간 크롤링 완료: 종목 {len(stocks)}개, 테마 {len(themes)}개")

    except Exception as e:
        print(f"주간 크롤러 에러: {e}")
    finally:
        api.disconnect()

    print(f"[{datetime.now()}] 주간 크롤러 종료")


def run_quarterly_crawler():
    """분기 1회 실행: 재무 데이터 갱신"""
    print(f"\n[{datetime.now()}] 분기 크롤러 시작")

    try:
        stock_codes = get_all_stock_codes()

        if not stock_codes:
            print("조회할 종목 없음")
            return

        crawler = FinancialCrawler()
        quarter = crawler.get_current_quarter()
        result = crawler.crawl_stocks(stock_codes)

        storage.save_financial(quarter, result)
        print(f"분기 크롤링 완료: {quarter}, {len(result)}개 종목")

    except Exception as e:
        print(f"분기 크롤러 에러: {e}")

    print(f"[{datetime.now()}] 분기 크롤러 종료")


def run_initial_crawl():
    """초기 데이터 수집 (최초 1회)"""
    print(f"\n[{datetime.now()}] 초기 데이터 수집 시작")

    # 데이터 디렉토리 초기화
    storage.init_data_directory()

    api = KiwoomAPI()
    if not api.login():
        print("로그인 실패")
        return

    try:
        # 1. 테마/종목 수집
        print("\n[1/5] 테마/종목 데이터 수집")
        theme_crawler = ThemeCrawler(api)
        theme_data = theme_crawler.crawl_all()

        # 종목 기본정보 저장 (stocks.json)
        stocks = {}
        for theme_stocks in theme_data["theme_stocks"].values():
            for stock in theme_stocks:
                stocks[stock["code"]] = {
                    "name": stock["name"],
                    "market": stock["market"]
                }
        storage.save_stocks(stocks)

        # 테마 매핑 저장 (themes.json)
        themes = []
        for theme in theme_data["themes"]:
            theme_code = theme["code"]
            stock_codes = [s["code"] for s in theme_data["theme_stocks"].get(theme_code, [])]
            themes.append({
                "id": theme_code,
                "name": theme["name"],
                "stocks": stock_codes
            })
        storage.save_themes(themes)

        # 2. 전체 종목 코드 추출
        stock_codes = list(stocks.keys())
        print(f"\n총 {len(stock_codes)}개 종목")

        # 3. 일봉 데이터 수집 (9주 + 여유 = 70일)
        print("\n[2/5] 일봉 데이터 수집 (70일)")
        price_crawler = PriceCrawler(api)
        price_data = price_crawler.crawl_stocks(stock_codes, days=70)

        # 월별로 분리하여 저장
        monthly_prices = {}  # {"2025-01": {"005930": {"2025-01-20": {...}}}}
        for code, daily_list in price_data.items():
            for day in daily_list:
                date = day["date"]
                year_month = date[:7]

                if year_month not in monthly_prices:
                    monthly_prices[year_month] = {}
                if code not in monthly_prices[year_month]:
                    monthly_prices[year_month][code] = {}

                monthly_prices[year_month][code][date] = {
                    "close": day["close"],
                    "value": day["trading_value"]
                }

        for year_month, data in monthly_prices.items():
            storage.save_prices(year_month, data)

        # 4. 시장 데이터 수집
        print("\n[3/5] 시장 데이터 수집")
        today = datetime.now().strftime("%Y-%m-%d")
        market_crawler = MarketCrawler(api)
        market_data = market_crawler.crawl_stocks(stock_codes)
        storage.save_market(today, market_data)

        api.disconnect()

        # 5. 재무 데이터 수집 (네이버 - API 불필요)
        print("\n[4/5] 재무 데이터 수집")
        financial_crawler = FinancialCrawler()
        quarter = financial_crawler.get_current_quarter()
        financial_data = financial_crawler.crawl_stocks(stock_codes)
        storage.save_financial(quarter, financial_data)

        print("\n[5/5] 초기 데이터 수집 완료!")
        print(f"  - 종목: {len(stocks)}개")
        print(f"  - 테마: {len(themes)}개")
        print(f"  - 가격: {len(monthly_prices)}개월")
        print(f"  - 시장: {len(market_data)}개")
        print(f"  - 재무: {len(financial_data)}개")

    except Exception as e:
        print(f"초기 수집 에러: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if api:
            api.disconnect()


def start_scheduler():
    """스케줄러 시작"""
    # 매일 15:40 - 일봉 + 시장 데이터 수집 (장 마감 후)
    schedule.every().day.at("15:40").do(run_daily_crawler)

    # 매주 토요일 10:00 - 테마/종목 갱신
    schedule.every().saturday.at("10:00").do(run_weekly_crawler)

    # 매 분기 첫째 주 월요일 - 재무 데이터 갱신 (수동 실행 권장)
    # schedule.every().monday.at("09:00").do(run_quarterly_crawler)

    print("스케줄러 시작")
    print("  - 매일 15:40: 일봉 + 시장 데이터 수집")
    print("  - 매주 토요일 10:00: 테마/종목 갱신")
    print("  - 분기 1회: 재무 데이터 (수동 실행: python scheduler.py quarterly)")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "init":
            run_initial_crawl()
        elif cmd == "daily":
            run_daily_crawler()
        elif cmd == "weekly":
            run_weekly_crawler()
        elif cmd == "quarterly":
            run_quarterly_crawler()
        else:
            print("사용법: python scheduler.py [init|daily|weekly|quarterly]")
            print("  init      - 초기 데이터 수집 (최초 1회)")
            print("  daily     - 일별 데이터 수집 (가격, 시장)")
            print("  weekly    - 주간 데이터 수집 (테마, 종목)")
            print("  quarterly - 분기 데이터 수집 (재무)")
    else:
        start_scheduler()