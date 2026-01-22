# 스케줄러 실행 명령어

```bash
cd crawlers
C:\Python311-32\python.exe scheduler.py [명령어]
```

## 명령어 목록

| 명령어 | 설명 |
|--------|------|
| `init` | 초기 데이터 수집 (최초 1회) |
| `daily` | 일별 데이터 수집 (오늘만) |
| `weekly` | 주간 데이터 수집 (테마, 종목) |
| `quarterly` | 분기 데이터 수집 (재무) |
| `kosdaq` | 코스닥 종목만 수집 (기존 데이터에 추가) |
| `update` | 마지막 저장일 이후 ~ 오늘까지 데이터 수집 |
| `add` | 개별 종목 추가 (테마 없이, 내 테마용) |
| `all` | 전체 종목 수집 (KOSPI+KOSDAQ, 기존 종목 제외) |

## 사용 예시

```bash
# 초기 설정 (최초 1회)
python scheduler.py init

# 매일 업데이트 (수동)
python scheduler.py update

# 개별 종목 추가 (테마 없이)
python scheduler.py add 005930 000660 035720

# 전체 시장 종목 수집 (테마 없는 종목까지 모두)
python scheduler.py all

# 자동 스케줄러 실행
python scheduler.py
```

## 자동 스케줄

인자 없이 실행하면 자동 스케줄러 시작:
- 매일 15:40 - 일봉 + 시장 데이터 수집
- 매주 토요일 10:00 - 테마/종목 갱신
