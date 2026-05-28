"""
generate_krx_tickers.py
KRX 코스피·코스닥 전종목 티커를 가져와 krx_tickers.json 으로 저장.
GitHub Actions에서 매일 실행 → 리포에 커밋.

필요 환경변수 (GitHub Secrets):
  KRX_ID  - data.krx.co.kr 로그인 ID
  KRX_PW  - data.krx.co.kr 로그인 PW

  KRX 계정이 없으면: https://data.krx.co.kr 에서 무료 회원가입

출력 형식:
{
  "삼성전자": "005930.KS",
  "펄어비스":  "263750.KQ",
  ...
}
"""

import json
import os
import sys
import requests

LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
LOGIN_JSP  = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
LOGIN_URL  = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
API_URL    = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def build_session(login_id: str, login_pw: str) -> requests.Session:
    s = requests.Session()
    s.get(LOGIN_PAGE, headers={"User-Agent": UA}, timeout=15)
    s.get(LOGIN_JSP,  headers={"User-Agent": UA, "Referer": LOGIN_PAGE}, timeout=15)

    payload = {"mbrId": login_id, "pw": login_pw, "mbrNm": "", "telNo": "", "di": "", "certType": ""}
    resp = s.post(LOGIN_URL, data=payload, headers={"User-Agent": UA, "Referer": LOGIN_PAGE}, timeout=15)
    data = resp.json()

    if data.get("_error_code") == "CD011":          # 중복 로그인 처리
        payload["skipDup"] = "Y"
        resp = s.post(LOGIN_URL, data=payload, headers={"User-Agent": UA, "Referer": LOGIN_PAGE}, timeout=15)
        data = resp.json()

    if data.get("_error_code") != "CD001":
        raise RuntimeError(f"KRX 로그인 실패: {data.get('_error_code')} {data.get('_error_message')}")

    print("KRX 로그인 성공")
    return s


def fetch_tickers(session: requests.Session, mkt_id: str) -> list[dict]:
    headers = {
        "User-Agent": UA,
        "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
        "X-Requested-With": "XMLHttpRequest",
    }
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
        "mktId": mkt_id,
        "segTpCd": "ALL",
    }
    resp = session.post(API_URL, headers=headers, data=data, timeout=30)
    resp.raise_for_status()
    return resp.json().get("OutBlock_1", [])


def main():
    login_id = os.getenv("KRX_ID")
    login_pw = os.getenv("KRX_PW")
    if not login_id or not login_pw:
        print("ERROR: KRX_ID, KRX_PW 환경변수를 설정하세요.")
        print("  data.krx.co.kr 에서 무료 회원가입 후 GitHub Secrets에 등록")
        sys.exit(1)

    session = build_session(login_id, login_pw)
    mapping = {}

    for mkt_id, suffix, label in [("STK", ".KS", "KOSPI"), ("KSQ", ".KQ", "KOSDAQ")]:
        print(f"{label} 조회 중...")
        rows = fetch_tickers(session, mkt_id)
        count = 0
        for row in rows:
            # ISU_SRT_CD = 6자리 종목코드, ISU_ABBRV = 약식 종목명
            code = str(row.get("ISU_SRT_CD", "")).strip()
            name = str(row.get("ISU_ABBRV", "")).strip()
            if not code or not name:
                continue
            mapping[name] = f"{code}{suffix}"
            count += 1
        print(f"  → {count}개")

    print(f"합계: {len(mapping)}개")

    with open("krx_tickers.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2, sort_keys=True)
    print("저장 완료: krx_tickers.json")


if __name__ == "__main__":
    main()
