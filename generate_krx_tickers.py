"""
generate_krx_tickers.py
KRX 코스피·코스닥 전종목 티커를 가져와 krx_tickers.json 으로 저장.
GitHub Actions에서 매일 실행 → 리포에 커밋.

출력 형식:
{
  "삼성전자": "005930.KS",
  "펄어비스":  "263750.KQ",
  ...
}
"""

import json
import sys
from pykrx import stock

def fetch_market(market: str, suffix: str) -> dict:
    tickers = stock.get_market_ticker_list(market=market)
    result = {}
    for code in tickers:
        try:
            name = stock.get_market_ticker_name(code)
            if name:
                result[name.strip()] = f"{code}{suffix}"
        except Exception:
            continue
    return result

def main():
    print("KOSPI 조회 중...")
    kospi = fetch_market("KOSPI", ".KS")
    print(f"  → {len(kospi)}개")

    print("KOSDAQ 조회 중...")
    kosdaq = fetch_market("KOSDAQ", ".KQ")
    print(f"  → {len(kosdaq)}개")

    # KOSPI 우선 (동명 종목 있을 경우 KOSPI로 덮어씀)
    merged = {**kosdaq, **kospi}
    print(f"합계: {len(merged)}개 (중복 제거 후)")

    output_path = "krx_tickers.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"저장 완료: {output_path}")

if __name__ == "__main__":
    main()
