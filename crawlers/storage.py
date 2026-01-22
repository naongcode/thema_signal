"""
JSON 파일 저장 유틸리티
- 데이터_정의.md 스키마에 맞게 저장
"""
import json
import os
from datetime import datetime
from typing import Dict, List, Any

# 기본 저장 경로
BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web", "data")


def ensure_dir(path: str):
    """디렉토리가 없으면 생성"""
    os.makedirs(path, exist_ok=True)


def save_json(filepath: str, data: Any):
    """JSON 파일 저장"""
    ensure_dir(os.path.dirname(filepath))
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"저장 완료: {filepath}")


def load_json(filepath: str) -> Any:
    """JSON 파일 로드"""
    if not os.path.exists(filepath):
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return None
        return json.loads(content)


# ============================================
# stocks.json - 종목 기본정보
# ============================================
def save_stocks(stocks: Dict[str, Dict]):
    """
    종목 기본정보 저장

    Args:
        stocks: {
            "005930": {"name": "삼성전자", "market": "KOSPI"},
            ...
        }
    """
    filepath = os.path.join(BASE_PATH, "stocks.json")
    save_json(filepath, stocks)


def load_stocks() -> Dict[str, Dict]:
    """종목 기본정보 로드"""
    filepath = os.path.join(BASE_PATH, "stocks.json")
    return load_json(filepath) or {}


# ============================================
# themes.json - 테마 매핑
# ============================================
def save_themes(themes: List[Dict]):
    """
    테마 매핑 저장

    Args:
        themes: [
            {"id": "T001", "name": "2차전지", "stocks": ["373220", ...]},
            ...
        ]
    """
    filepath = os.path.join(BASE_PATH, "themes.json")
    save_json(filepath, {"themes": themes})


def load_themes() -> List[Dict]:
    """테마 매핑 로드"""
    filepath = os.path.join(BASE_PATH, "themes.json")
    data = load_json(filepath)
    return data.get("themes", []) if data else []


# ============================================
# market.json - 시장 데이터 (시총, 주식수, PER, PBR)
# ============================================
def save_market(date: str, data: Dict[str, Dict]):
    """
    시장 데이터 저장

    Args:
        date: "2025-01-20"
        data: {
            "005930": {"market_cap": 420000000000000, "shares": 5969782550, "per": 12.5, "pbr": 1.2},
            ...
        }
    """
    filepath = os.path.join(BASE_PATH, "market.json")
    save_json(filepath, {"date": date, "data": data})


def load_market() -> Dict:
    """시장 데이터 로드"""
    filepath = os.path.join(BASE_PATH, "market.json")
    return load_json(filepath) or {"date": None, "data": {}}


# ============================================
# financial.json - 재무 데이터 (매출, 영업이익)
# ============================================
def save_financial(quarter: str, data: Dict[str, Dict]):
    """
    재무 데이터 저장

    Args:
        quarter: "2024-Q3"
        data: {
            "005930": {"revenue": 79000000000000, "operating_profit": 9180000000000},
            ...
        }
    """
    filepath = os.path.join(BASE_PATH, "financial.json")
    save_json(filepath, {"quarter": quarter, "data": data})


def load_financial() -> Dict:
    """재무 데이터 로드"""
    filepath = os.path.join(BASE_PATH, "financial.json")
    return load_json(filepath) or {"quarter": None, "data": {}}


# ============================================
# prices/YYYY-MM.json - 월별 가격 데이터
# ============================================
def get_price_filepath(year_month: str) -> str:
    """월별 가격 파일 경로 반환"""
    return os.path.join(BASE_PATH, "prices", f"{year_month}.json")


def save_prices(year_month: str, data: Dict[str, Dict]):
    """
    월별 가격 데이터 저장 (전체 덮어쓰기)

    Args:
        year_month: "2025-01"
        data: {
            "005930": {
                "2025-01-20": {"close": 71000, "value": 850000000000},
                ...
            },
            ...
        }
    """
    filepath = get_price_filepath(year_month)
    save_json(filepath, data)


def load_prices(year_month: str) -> Dict[str, Dict]:
    """월별 가격 데이터 로드"""
    filepath = get_price_filepath(year_month)
    return load_json(filepath) or {}


def add_daily_prices(date: str, prices: Dict[str, Dict]):
    """
    일별 가격 데이터 추가 (기존 데이터에 병합)

    Args:
        date: "2025-01-20"
        prices: {
            "005930": {"close": 71000, "value": 850000000000},
            ...
        }
    """
    year_month = date[:7]  # "2025-01-20" -> "2025-01"

    # 기존 데이터 로드
    existing = load_prices(year_month)

    # 각 종목별로 해당 날짜 데이터 추가
    for stock_code, price_data in prices.items():
        if stock_code not in existing:
            existing[stock_code] = {}
        existing[stock_code][date] = price_data

    # 저장
    save_prices(year_month, existing)
    print(f"{date} 가격 데이터 {len(prices)}개 종목 추가 완료")


def load_prices_range(months: List[str]) -> Dict[str, Dict]:
    """
    여러 월의 가격 데이터 병합 로드

    Args:
        months: ["2025-01", "2024-12", "2024-11"]

    Returns:
        {
            "005930": {
                "2025-01-20": {"close": 71000, "value": ...},
                "2024-12-30": {"close": 70000, "value": ...},
                ...
            },
            ...
        }
    """
    merged = {}

    for month in months:
        data = load_prices(month)
        for stock_code, dates in data.items():
            if stock_code not in merged:
                merged[stock_code] = {}
            merged[stock_code].update(dates)

    return merged


# ============================================
# 유틸리티 함수
# ============================================
def get_recent_months(count: int = 3) -> List[str]:
    """최근 N개월 목록 반환"""
    months = []
    now = datetime.now()
    year, month = now.year, now.month

    for _ in range(count):
        months.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    return months


def get_last_price_date() -> str:
    """저장된 가격 데이터의 마지막 날짜 반환"""
    months = get_recent_months(3)
    last_date = None

    for month in months:
        data = load_prices(month)
        for stock_code, dates in data.items():
            for date in dates.keys():
                if last_date is None or date > last_date:
                    last_date = date

    return last_date


def init_data_directory():
    """데이터 디렉토리 초기화"""
    ensure_dir(BASE_PATH)
    ensure_dir(os.path.join(BASE_PATH, "prices"))
    print(f"데이터 디렉토리 생성: {BASE_PATH}")


if __name__ == "__main__":
    # 테스트
    init_data_directory()

    # stocks.json 테스트
    test_stocks = {
        "005930": {"name": "삼성전자", "market": "KOSPI"},
        "000660": {"name": "SK하이닉스", "market": "KOSPI"},
    }
    save_stocks(test_stocks)
    print(f"로드 테스트: {load_stocks()}")
