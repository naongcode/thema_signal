"""
키움 Open API+ 연결 모듈
- 32bit Python 필요
- Open API+ 모듈 설치 필요
"""
import sys
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QEventLoop, QTimer


class KiwoomAPI:
    """키움 Open API+ 래퍼 클래스"""

    def __init__(self):
        self.app = QApplication(sys.argv)
        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.connected = False
        self.login_loop = None
        self.request_loop = None
        self.tr_data = None
        self.tr_handler = None  # TR별 데이터 처리 핸들러

        # 이벤트 연결
        self.ocx.OnEventConnect.connect(self._on_event_connect)
        self.ocx.OnReceiveTrData.connect(self._on_receive_tr_data)

    def login(self, timeout: int = 60) -> bool:
        """
        로그인 (키움 로그인 창 팝업)

        Args:
            timeout: 로그인 타임아웃 (초)

        Returns:
            로그인 성공 여부
        """
        self.ocx.dynamicCall("CommConnect()")

        self.login_loop = QEventLoop()
        QTimer.singleShot(timeout * 1000, self.login_loop.quit)
        self.login_loop.exec_()

        return self.connected

    def _on_event_connect(self, err_code: int):
        """로그인 이벤트 핸들러"""
        if err_code == 0:
            self.connected = True
            print("로그인 성공")
        else:
            self.connected = False
            print(f"로그인 실패: {err_code}")

        if self.login_loop:
            self.login_loop.quit()

    def get_connect_state(self) -> bool:
        """연결 상태 확인"""
        return self.ocx.dynamicCall("GetConnectState()") == 1

    def get_login_info(self, tag: str) -> str:
        """
        로그인 정보 조회

        Args:
            tag: ACCOUNT_CNT, ACCNO, USER_ID, USER_NAME 등
        """
        return self.ocx.dynamicCall("GetLoginInfo(QString)", tag)

    def set_input_value(self, name: str, value: str):
        """TR 입력값 설정"""
        self.ocx.dynamicCall("SetInputValue(QString, QString)", name, value)

    def comm_rq_data(self, rq_name: str, tr_code: str, prev_next: int, screen_no: str, timeout: int = 10, handler=None) -> dict:
        """
        TR 요청

        Args:
            rq_name: 요청명
            tr_code: TR 코드
            prev_next: 연속조회 여부 (0: 처음, 2: 연속)
            screen_no: 화면번호 (4자리)
            timeout: 타임아웃 (초)
            handler: 데이터 처리 핸들러 함수 (이벤트 안에서 호출됨)

        Returns:
            TR 응답 데이터
        """
        self.tr_data = None
        self.tr_handler = handler
        self.ocx.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            rq_name, tr_code, prev_next, screen_no
        )

        self.request_loop = QEventLoop()
        QTimer.singleShot(timeout * 1000, self.request_loop.quit)
        self.request_loop.exec_()

        self.tr_handler = None
        return self.tr_data

    def _on_receive_tr_data(self, screen_no, rq_name, tr_code, record_name, prev_next, *args):
        """TR 데이터 수신 이벤트 핸들러 - 이 안에서 데이터를 읽어야 함"""
        self.tr_data = {
            "screen_no": screen_no,
            "rq_name": rq_name,
            "tr_code": tr_code,
            "record_name": record_name,
            "prev_next": prev_next,
        }

        # TR 핸들러가 있으면 실행 (이벤트 안에서 데이터 읽기)
        if self.tr_handler:
            self.tr_data["result"] = self.tr_handler(tr_code, rq_name)

        if self.request_loop:
            self.request_loop.quit()

    def _get_comm_data(self, tr_code: str, rq_name: str, index: int, item_name: str) -> str:
        """TR 데이터 조회 (이벤트 핸들러 내에서만 호출)"""
        return self.ocx.dynamicCall(
            "GetCommData(QString, QString, int, QString)",
            tr_code, rq_name, index, item_name
        ).strip()

    def _get_repeat_cnt(self, tr_code: str, rq_name: str) -> int:
        """반복 데이터 개수 조회 (이벤트 핸들러 내에서만 호출)"""
        return self.ocx.dynamicCall(
            "GetRepeatCnt(QString, QString)",
            tr_code, rq_name
        )

    def disconnect(self):
        """연결 해제"""
        self.ocx.dynamicCall("CommTerminate()")
        self.connected = False


if __name__ == "__main__":
    # 테스트
    api = KiwoomAPI()

    if api.login():
        print(f"사용자: {api.get_login_info('USER_NAME')}")
        print(f"계좌: {api.get_login_info('ACCNO')}")
    else:
        print("로그인 실패")