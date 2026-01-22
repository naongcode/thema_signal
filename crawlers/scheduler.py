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


def is_excluded_stock(name: str) -> bool:
    """ETF/ETN/스팩/우선주 등 제외 대상인지 확인"""
    # 제외 패턴
    exclude_patterns = [
        '스팩', 'SPAC', '리츠', 'ETN', 'ETF',
        '인버스', '레버리지', '선물', '채권',
        # ETF 브랜드
        'KODEX', 'TIGER', 'ACE', 'ARIRANG', 'KBSTAR',
        'HANARO', 'SOL', 'PLUS', 'RISE', 'KOSEF',
        'KINDEX', 'SMART', 'FOCUS', 'TIMEFOLIO'
    ]

    # 패턴 매칭
    if any(pattern in name for pattern in exclude_patterns):
        return True

    # 우선주 제외 (이름 끝에 "우", "우B", "우C" 등)
    if name.endswith('우') or name.endswith('우B') or name.endswith('우C'):
        return True

    return False


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


def run_update_crawler():
    """수동 실행: 마지막 저장 날짜 이후 ~ 오늘까지 데이터 수집"""
    print(f"\n[{datetime.now()}] 업데이트 크롤러 시작")

    # 마지막 저장 날짜 확인
    last_date = storage.get_last_price_date()
    today = datetime.now().strftime("%Y-%m-%d")

    if last_date is None:
        print("저장된 가격 데이터 없음 - init 먼저 실행 필요")
        return

    if last_date >= today:
        print(f"이미 최신 데이터 ({last_date})까지 저장됨")
        return

    # 거래일 수 계산 (주말 제외 대략 계산)
    from datetime import datetime as dt
    last_dt = dt.strptime(last_date, "%Y-%m-%d")
    today_dt = dt.strptime(today, "%Y-%m-%d")
    calendar_days = (today_dt - last_dt).days
    trading_days = int(calendar_days * 5 / 7) + 5  # 여유 있게

    print(f"마지막 저장: {last_date}, 오늘: {today}")
    print(f"가져올 일수: 약 {trading_days}일")

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
        print(f"\n[1/2] 일봉 데이터 수집 ({len(stock_codes)}개 종목, {trading_days}일)")
        price_crawler = PriceCrawler(api)
        price_data = price_crawler.crawl_stocks(stock_codes, days=trading_days)

        # 월별로 모아서 저장
        monthly_prices = {}
        new_count = 0
        for code, daily_list in price_data.items():
            for day in daily_list:
                date = day["date"]
                # 마지막 저장 날짜 이후 데이터만 저장
                if date <= last_date:
                    continue
                year_month = date[:7]
                if year_month not in monthly_prices:
                    monthly_prices[year_month] = {}
                if code not in monthly_prices[year_month]:
                    monthly_prices[year_month][code] = {}
                monthly_prices[year_month][code][date] = {
                    "close": day["close"],
                    "value": day["trading_value"]
                }
                new_count += 1

        # 월별로 기존 데이터와 병합 후 저장
        for year_month, new_data in monthly_prices.items():
            existing = storage.load_prices(year_month)
            for code, dates in new_data.items():
                if code not in existing:
                    existing[code] = {}
                existing[code].update(dates)
            storage.save_prices(year_month, existing)
            print(f"  {year_month} 저장 완료")

        # 2. 시장 데이터 수집
        print(f"\n[2/2] 시장 데이터 수집")
        market_crawler = MarketCrawler(api)
        market_result = market_crawler.crawl_stocks(stock_codes)
        storage.save_market(today, market_result)

        print(f"\n업데이트 완료: 새 가격 데이터 {new_count}건")

    except Exception as e:
        print(f"업데이트 크롤러 에러: {e}")
        import traceback
        traceback.print_exc()
    finally:
        api.disconnect()

    print(f"[{datetime.now()}] 업데이트 크롤러 종료")


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

        # 종목 기본정보 저장 (stocks.json) - ETF/스팩/우선주 필터링
        stocks = {}
        filtered_count = 0
        for theme_stocks in theme_data["theme_stocks"].values():
            for stock in theme_stocks:
                name = stock["name"]
                if is_excluded_stock(name):
                    filtered_count += 1
                    continue
                stocks[stock["code"]] = {
                    "name": name,
                    "market": stock["market"]
                }
        print(f"  테마 종목: {len(stocks)}개 (필터링 제외: {filtered_count}개)")

        # 전체 시장 종목 추가 (테마 없는 종목 포함)
        print("\n  전체 시장 종목 조회 중...")
        kospi_raw = api.ocx.dynamicCall("GetCodeListByMarket(QString)", "0")
        kospi_codes = set(kospi_raw.split(";")) if kospi_raw else set()
        kospi_codes.discard("")

        kosdaq_raw = api.ocx.dynamicCall("GetCodeListByMarket(QString)", "10")
        kosdaq_codes = set(kosdaq_raw.split(";")) if kosdaq_raw else set()
        kosdaq_codes.discard("")

        # ETF 제외
        etf_raw = api.ocx.dynamicCall("GetCodeListByMarket(QString)", "8")
        etf_codes = set(etf_raw.split(";")) if etf_raw else set()
        kospi_codes = kospi_codes - etf_codes
        kosdaq_codes = kosdaq_codes - etf_codes

        all_market_codes = kospi_codes | kosdaq_codes
        new_codes = all_market_codes - set(stocks.keys())

        print(f"  KOSPI: {len(kospi_codes)}개, KOSDAQ: {len(kosdaq_codes)}개")
        print(f"  테마 외 종목: {len(new_codes)}개 추가 조회 중...")

        added_count = 0
        market_filtered = 0
        for code in new_codes:
            name = api.ocx.dynamicCall("GetMasterCodeName(QString)", code)
            if not name:
                continue
            name = name.strip()
            if is_excluded_stock(name):
                market_filtered += 1
                continue
            market = "KOSDAQ" if code in kosdaq_codes else "KOSPI"
            stocks[code] = {"name": name, "market": market}
            added_count += 1

        storage.save_stocks(stocks)
        print(f"  추가됨: {added_count}개 (필터링 제외: {market_filtered}개)")
        print(f"  총 종목: {len(stocks)}개")

        # 테마 매핑 저장 (themes.json) - 필터링된 종목만 포함
        themes = []
        for theme in theme_data["themes"]:
            theme_code = theme["code"]
            # 필터링된 종목만 포함
            stock_codes = [
                s["code"] for s in theme_data["theme_stocks"].get(theme_code, [])
                if not is_excluded_stock(s["name"])
            ]
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


def run_kosdaq_crawl():
    """코스닥 종목만 크롤링하여 기존 데이터에 추가"""
    print(f"\n[{datetime.now()}] 코스닥 크롤링 시작")

    api = KiwoomAPI()
    if not api.login():
        print("로그인 실패")
        return

    try:
        # 1. 코스닥 테마/종목 수집
        print("\n[1/4] 코스닥 테마/종목 수집")
        theme_crawler = ThemeCrawler(api)
        kosdaq_data = theme_crawler.crawl_kosdaq_only()

        # 기존 stocks.json 로드 후 병합
        existing_stocks = storage.load_stocks()
        new_stocks = {}
        for theme_stocks in kosdaq_data["theme_stocks"].values():
            for stock in theme_stocks:
                new_stocks[stock["code"]] = {
                    "name": stock["name"],
                    "market": stock["market"]
                }
        existing_stocks.update(new_stocks)
        storage.save_stocks(existing_stocks)

        # 기존 themes.json 로드 후 병합
        existing_themes = storage.load_themes()
        theme_dict = {t["id"]: t for t in existing_themes}
        for theme in kosdaq_data["themes"]:
            theme_code = theme["code"]
            kosdaq_stock_codes = [s["code"] for s in kosdaq_data["theme_stocks"].get(theme_code, [])]
            if kosdaq_stock_codes:
                if theme_code in theme_dict:
                    # 기존 테마에 코스닥 종목 추가
                    existing_codes = set(theme_dict[theme_code]["stocks"])
                    existing_codes.update(kosdaq_stock_codes)
                    theme_dict[theme_code]["stocks"] = list(existing_codes)
                else:
                    # 새 테마 추가
                    theme_dict[theme_code] = {
                        "id": theme_code,
                        "name": theme["name"],
                        "stocks": kosdaq_stock_codes
                    }
        storage.save_themes(list(theme_dict.values()))

        # 코스닥 종목 코드 목록
        kosdaq_codes = list(new_stocks.keys())
        print(f"\n코스닥 종목 {len(kosdaq_codes)}개 추가됨")

        # 2. 일봉 데이터 수집 (70일)
        print("\n[2/4] 일봉 데이터 수집 (70일)")
        price_crawler = PriceCrawler(api)
        price_data = price_crawler.crawl_stocks(kosdaq_codes, days=70)

        # 월별로 모아서 한번에 저장 (최적화)
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

        # 월별로 기존 데이터와 병합 후 저장
        for year_month, new_data in monthly_prices.items():
            existing = storage.load_prices(year_month)
            for code, dates in new_data.items():
                if code not in existing:
                    existing[code] = {}
                existing[code].update(dates)
            storage.save_prices(year_month, existing)
            print(f"  {year_month} 저장 완료")

        # 3. 시장 데이터 수집
        print("\n[3/4] 시장 데이터 수집")
        today = datetime.now().strftime("%Y-%m-%d")
        market_crawler = MarketCrawler(api)
        market_data = market_crawler.crawl_stocks(kosdaq_codes)

        # 기존 market.json에 병합
        existing_market = storage.load_market()
        existing_market["data"].update(market_data)
        storage.save_market(today, existing_market["data"])

        api.disconnect()

        # 4. 재무 데이터 수집 (네이버)
        print("\n[4/4] 재무 데이터 수집")
        financial_crawler = FinancialCrawler()
        quarter = financial_crawler.get_current_quarter()
        financial_data = financial_crawler.crawl_stocks(kosdaq_codes)

        # 기존 financial.json에 병합
        existing_financial = storage.load_financial()
        existing_financial["data"].update(financial_data)
        storage.save_financial(quarter, existing_financial["data"])

        print(f"\n코스닥 크롤링 완료!")
        print(f"  - 종목: {len(kosdaq_codes)}개 추가")
        print(f"  - 가격: {len(price_data)}개")
        print(f"  - 시장: {len(market_data)}개")
        print(f"  - 재무: {len(financial_data)}개")

    except Exception as e:
        print(f"코스닥 크롤러 에러: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if api:
            api.disconnect()

    print(f"[{datetime.now()}] 코스닥 크롤링 종료")


def run_add_stocks(stock_codes: list):
    """개별 종목 추가: 테마 없이 종목 데이터만 수집"""
    print(f"\n[{datetime.now()}] 종목 추가 시작: {stock_codes}")

    if not stock_codes:
        print("추가할 종목코드를 입력하세요")
        print("사용법: python scheduler.py add 005930 000660 ...")
        return

    api = KiwoomAPI()
    if not api.login():
        print("로그인 실패 - 크롤러 종료")
        return

    try:
        # 1. 종목 기본정보 조회 및 저장
        print("\n[1/4] 종목 기본정보 조회")
        existing_stocks = storage.load_stocks()
        new_stocks = {}

        # 코스닥 코드 목록 로드 (시장 구분용)
        kosdaq_codes = set()
        kosdaq_raw = api.ocx.dynamicCall("GetCodeListByMarket(QString)", "10")
        if kosdaq_raw:
            kosdaq_codes = set(kosdaq_raw.split(";"))

        for code in stock_codes:
            code = code.strip()
            if len(code) != 6:
                print(f"  {code}: 잘못된 종목코드 (6자리 필요)")
                continue

            if code in existing_stocks:
                print(f"  {code}: 이미 등록된 종목 ({existing_stocks[code]['name']})")
                continue

            # 종목명 조회
            name = api.ocx.dynamicCall("GetMasterCodeName(QString)", code)
            if not name:
                print(f"  {code}: 종목을 찾을 수 없음")
                continue

            market = "KOSDAQ" if code in kosdaq_codes else "KOSPI"
            new_stocks[code] = {"name": name.strip(), "market": market}
            print(f"  {code}: {name.strip()} ({market})")

        if not new_stocks:
            print("추가할 새 종목이 없습니다")
            api.disconnect()
            return

        # stocks.json에 저장
        existing_stocks.update(new_stocks)
        storage.save_stocks(existing_stocks)
        print(f"종목 {len(new_stocks)}개 추가됨")

        new_codes = list(new_stocks.keys())

        # 2. 일봉 데이터 수집 (70일)
        print(f"\n[2/4] 일봉 데이터 수집 ({len(new_codes)}개 종목)")
        price_crawler = PriceCrawler(api)
        price_data = price_crawler.crawl_stocks(new_codes, days=70)

        # 월별로 저장
        monthly_prices = {}
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

        for year_month, new_data in monthly_prices.items():
            existing = storage.load_prices(year_month)
            for code, dates in new_data.items():
                if code not in existing:
                    existing[code] = {}
                existing[code].update(dates)
            storage.save_prices(year_month, existing)
            print(f"  {year_month} 저장 완료")

        # 3. 시장 데이터 수집
        print(f"\n[3/4] 시장 데이터 수집")
        today = datetime.now().strftime("%Y-%m-%d")
        market_crawler = MarketCrawler(api)
        market_data = market_crawler.crawl_stocks(new_codes)

        existing_market = storage.load_market()
        existing_market["data"].update(market_data)
        storage.save_market(today, existing_market["data"])

        api.disconnect()

        # 4. 재무 데이터 수집 (네이버)
        print(f"\n[4/4] 재무 데이터 수집")
        financial_crawler = FinancialCrawler()
        quarter = financial_crawler.get_current_quarter()
        financial_data = financial_crawler.crawl_stocks(new_codes)

        existing_financial = storage.load_financial()
        existing_financial["data"].update(financial_data)
        storage.save_financial(quarter, existing_financial["data"])

        print(f"\n종목 추가 완료!")
        print(f"  - 종목: {len(new_codes)}개")
        print(f"  - 가격: {len(price_data)}개")
        print(f"  - 시장: {len(market_data)}개")
        print(f"  - 재무: {len(financial_data)}개")

    except Exception as e:
        print(f"종목 추가 에러: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if api:
            api.disconnect()

    print(f"[{datetime.now()}] 종목 추가 종료")


def run_all_stocks():
    """전체 시장 종목 수집 (KOSPI + KOSDAQ, 기존 종목 제외)"""
    print(f"\n[{datetime.now()}] 전체 종목 수집 시작")

    api = KiwoomAPI()
    if not api.login():
        print("로그인 실패 - 크롤러 종료")
        return

    try:
        # 기존 종목 로드
        existing_stocks = storage.load_stocks()
        existing_codes = set(existing_stocks.keys())
        print(f"기존 종목: {len(existing_codes)}개")

        # 1. 전체 종목 코드 조회
        print("\n[1/5] 전체 종목 코드 조회")

        # KOSPI (시장코드 0)
        kospi_raw = api.ocx.dynamicCall("GetCodeListByMarket(QString)", "0")
        kospi_codes = set(kospi_raw.split(";")) if kospi_raw else set()
        kospi_codes.discard("")

        # KOSDAQ (시장코드 10)
        kosdaq_raw = api.ocx.dynamicCall("GetCodeListByMarket(QString)", "10")
        kosdaq_codes = set(kosdaq_raw.split(";")) if kosdaq_raw else set()
        kosdaq_codes.discard("")

        # ETF 제외 (시장코드 8)
        etf_raw = api.ocx.dynamicCall("GetCodeListByMarket(QString)", "8")
        etf_codes = set(etf_raw.split(";")) if etf_raw else set()

        kospi_codes = kospi_codes - etf_codes
        kosdaq_codes = kosdaq_codes - etf_codes

        all_codes = kospi_codes | kosdaq_codes
        new_codes = all_codes - existing_codes

        print(f"  KOSPI: {len(kospi_codes)}개")
        print(f"  KOSDAQ: {len(kosdaq_codes)}개")
        print(f"  전체: {len(all_codes)}개")
        print(f"  신규 (추가 대상): {len(new_codes)}개")

        if not new_codes:
            print("추가할 새 종목이 없습니다")
            api.disconnect()
            return

        # 2. 종목 기본정보 조회 (보통주만 필터링)
        print(f"\n[2/5] 종목 기본정보 조회 및 필터링")
        new_stocks = {}
        filtered_count = 0

        for i, code in enumerate(new_codes):
            if (i + 1) % 500 == 0:
                print(f"  [{i + 1}/{len(new_codes)}] 조회 중...")

            name = api.ocx.dynamicCall("GetMasterCodeName(QString)", code)
            if not name:
                continue

            name = name.strip()

            # 필터링: ETF/ETN/스팩/우선주 등 제외
            if is_excluded_stock(name):
                filtered_count += 1
                continue

            market = "KOSDAQ" if code in kosdaq_codes else "KOSPI"
            new_stocks[code] = {"name": name, "market": market}

        print(f"  필터링 제외: {filtered_count}개 (ETF/ETN/스팩/우선주 등)")

        # stocks.json에 저장
        existing_stocks.update(new_stocks)
        storage.save_stocks(existing_stocks)
        print(f"종목 {len(new_stocks)}개 추가됨")

        new_code_list = list(new_stocks.keys())

        # 3. 일봉 데이터 수집 (70일)
        print(f"\n[3/5] 일봉 데이터 수집 ({len(new_code_list)}개 종목)")
        price_crawler = PriceCrawler(api)
        price_data = price_crawler.crawl_stocks(new_code_list, days=70)

        # 월별로 저장
        monthly_prices = {}
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

        for year_month, new_data in monthly_prices.items():
            existing = storage.load_prices(year_month)
            for code, dates in new_data.items():
                if code not in existing:
                    existing[code] = {}
                existing[code].update(dates)
            storage.save_prices(year_month, existing)
            print(f"  {year_month} 저장 완료")

        # 4. 시장 데이터 수집
        print(f"\n[4/5] 시장 데이터 수집")
        today = datetime.now().strftime("%Y-%m-%d")
        market_crawler = MarketCrawler(api)
        market_data = market_crawler.crawl_stocks(new_code_list)

        existing_market = storage.load_market()
        existing_market["data"].update(market_data)
        storage.save_market(today, existing_market["data"])

        api.disconnect()

        # 5. 재무 데이터 수집 (네이버)
        print(f"\n[5/5] 재무 데이터 수집")
        financial_crawler = FinancialCrawler()
        quarter = financial_crawler.get_current_quarter()
        financial_data = financial_crawler.crawl_stocks(new_code_list)

        existing_financial = storage.load_financial()
        existing_financial["data"].update(financial_data)
        storage.save_financial(quarter, existing_financial["data"])

        print(f"\n전체 종목 수집 완료!")
        print(f"  - 신규 종목: {len(new_stocks)}개")
        print(f"  - 가격 데이터: {len(price_data)}개")
        print(f"  - 시장 데이터: {len(market_data)}개")
        print(f"  - 재무 데이터: {len(financial_data)}개")

    except Exception as e:
        print(f"전체 종목 수집 에러: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if api:
            api.disconnect()

    print(f"[{datetime.now()}] 전체 종목 수집 종료")


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
        elif cmd == "kosdaq":
            run_kosdaq_crawl()
        elif cmd == "update":
            run_update_crawler()
        elif cmd == "add":
            stock_codes = sys.argv[2:]
            run_add_stocks(stock_codes)
        elif cmd == "all":
            run_all_stocks()
        else:
            print("사용법: python scheduler.py [명령어]")
            print("")
            print("명령어:")
            print("  init      - 초기 데이터 수집 (최초 1회)")
            print("  daily     - 일별 데이터 수집 (오늘만)")
            print("  weekly    - 주간 데이터 수집 (테마, 종목)")
            print("  quarterly - 분기 데이터 수집 (재무)")
            print("  kosdaq    - 코스닥 종목만 수집 (기존 데이터에 추가)")
            print("  update    - 마지막 저장일 이후 ~ 오늘까지 데이터 수집")
            print("  add       - 개별 종목 추가 (테마 없이)")
            print("  all       - 전체 종목 수집 (KOSPI+KOSDAQ, 기존 제외)")
            print("")
            print("예시:")
            print("  python scheduler.py add 005930 000660  # 삼성전자, SK하이닉스 추가")
            print("  python scheduler.py all               # 전체 시장 종목 수집")
    else:
        start_scheduler()