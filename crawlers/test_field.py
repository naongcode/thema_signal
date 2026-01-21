from kiwoom.api import KiwoomAPI

api = KiwoomAPI()
api.login()

# 방법 1: GetThemeGroupList 시도
print("=== GetThemeGroupList ===")
theme_list = api.ocx.dynamicCall("GetThemeGroupList(int)", 1)
print(f"결과: {theme_list[:500] if theme_list else 'None'}...")

# 방법 2: 테마 코드로 종목 조회 시도
print("\n=== GetThemeGroupCode ===")
theme_code = api.ocx.dynamicCall("GetThemeGroupCode(QString)", "0")
print(f"결과: {theme_code[:500] if theme_code else 'None'}...")
