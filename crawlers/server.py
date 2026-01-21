"""
크롤링 API 서버
웹 UI에서 버튼 클릭으로 크롤러 실행
"""
from flask import Flask, jsonify
from flask_cors import CORS
import threading
import sys
import os

# 현재 디렉토리를 path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import (
    run_daily_crawler,
    run_weekly_crawler,
    run_quarterly_crawler,
    run_initial_crawl
)

app = Flask(__name__)
CORS(app)  # 웹에서 API 호출 허용

# 크롤링 상태 관리
crawl_status = {
    "running": False,
    "type": None,
    "message": ""
}


def run_crawler_async(crawler_func, crawler_type):
    """비동기로 크롤러 실행"""
    global crawl_status

    crawl_status["running"] = True
    crawl_status["type"] = crawler_type
    crawl_status["message"] = f"{crawler_type} 크롤링 진행 중..."

    try:
        crawler_func()
        crawl_status["message"] = f"{crawler_type} 크롤링 완료!"
    except Exception as e:
        crawl_status["message"] = f"{crawler_type} 크롤링 실패: {str(e)}"
    finally:
        crawl_status["running"] = False
        crawl_status["type"] = None


@app.route("/api/status", methods=["GET"])
def get_status():
    """현재 크롤링 상태 확인"""
    return jsonify(crawl_status)


@app.route("/api/crawl/daily", methods=["POST"])
def crawl_daily():
    """일별 크롤링 실행 (가격 + 시장)"""
    if crawl_status["running"]:
        return jsonify({"error": "이미 크롤링 진행 중", "status": crawl_status}), 400

    thread = threading.Thread(
        target=run_crawler_async,
        args=(run_daily_crawler, "일별")
    )
    thread.start()

    return jsonify({"message": "일별 크롤링 시작", "status": crawl_status})


@app.route("/api/crawl/weekly", methods=["POST"])
def crawl_weekly():
    """주간 크롤링 실행 (테마 + 종목)"""
    if crawl_status["running"]:
        return jsonify({"error": "이미 크롤링 진행 중", "status": crawl_status}), 400

    thread = threading.Thread(
        target=run_crawler_async,
        args=(run_weekly_crawler, "주간")
    )
    thread.start()

    return jsonify({"message": "주간 크롤링 시작", "status": crawl_status})


@app.route("/api/crawl/quarterly", methods=["POST"])
def crawl_quarterly():
    """분기 크롤링 실행 (재무)"""
    if crawl_status["running"]:
        return jsonify({"error": "이미 크롤링 진행 중", "status": crawl_status}), 400

    thread = threading.Thread(
        target=run_crawler_async,
        args=(run_quarterly_crawler, "분기")
    )
    thread.start()

    return jsonify({"message": "분기 크롤링 시작", "status": crawl_status})


@app.route("/api/crawl/init", methods=["POST"])
def crawl_init():
    """초기 크롤링 실행 (전체)"""
    if crawl_status["running"]:
        return jsonify({"error": "이미 크롤링 진행 중", "status": crawl_status}), 400

    thread = threading.Thread(
        target=run_crawler_async,
        args=(run_initial_crawl, "초기")
    )
    thread.start()

    return jsonify({"message": "초기 크롤링 시작", "status": crawl_status})


if __name__ == "__main__":
    print("크롤링 API 서버 시작")
    print("  - GET  /api/status        : 크롤링 상태 확인")
    print("  - POST /api/crawl/daily   : 일별 크롤링 (가격, 시장)")
    print("  - POST /api/crawl/weekly  : 주간 크롤링 (테마, 종목)")
    print("  - POST /api/crawl/quarterly: 분기 크롤링 (재무)")
    print("  - POST /api/crawl/init    : 초기 크롤링 (전체)")
    print()
    app.run(host="0.0.0.0", port=5000, debug=True)
