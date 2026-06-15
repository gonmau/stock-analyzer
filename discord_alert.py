"""
보유종목 가격 알림 (목표가 / 손절가 / 트레일링 스탑) → Discord Webhook 전송

데이터 소스:
  - positions_snapshot.json : app6.py에서 '현재가 조회' 시 자동 동기화 (종목키, 종목명, 평균단가, 잔고수량, 종목코드6)
  - price_alerts.json       : app6.py '🎯 목표가/손절가/트레일링 스탑 설정'에서 동기화 (target, stoplos, trailing_pct)
  - alert_state.json        : 이 스크립트가 직접 관리 (신고가 추적, 알림 중복 방지)

트레일링 스탑 로직:
  - 현재가 >= 평균단가 (수익 중)  → 매수 이후 최고가(신고가) 대비 -trailing_pct% 하락 시 알림
  - 현재가 <  평균단가 (손실 중)  → 평균단가 대비 -trailing_pct% 도달 시 알림 (= 손절가와 동일 기준)

알림 종류: target(목표가 도달), stoplos(손절가 도달), trailing(트레일링 스탑 발동)
같은 알림은 alert_state.json에 기록되어 같은 트리거에서 중복 발송되지 않음.
가격이 트리거 구간을 벗어나면(예: 다시 회복) 해당 알림 상태는 리셋되어 재발동 가능.
"""

import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

POSITIONS_PATH = "positions_snapshot.json"
ALERTS_PATH    = "price_alerts.json"
STATE_PATH     = "alert_state.json"

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()


def load_json(path, default):
    try:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"⚠️ {path} 로드 실패: {e}")
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fetch_naver_price(code6: str):
    """네이버 모바일 API에서 현재가 조회."""
    url = f"https://m.stock.naver.com/api/stock/{code6}/basic"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
        "Referer": "https://m.finance.naver.com/",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            price = data.get("closePrice") or data.get("currentPrice") or data.get("stockPrice")
            if price:
                return int(str(price).replace(",", ""))
    except Exception as e:
        print(f"⚠️ {code6} 네이버 가격 조회 실패: {e}")
    return None


def fetch_yf_price(code6: str):
    """yfinance 폴백 (KS/KQ 모두 시도)."""
    for suffix in ("KS", "KQ"):
        ticker = f"{code6}.{suffix}"
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            "?range=1d&interval=1m"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
                result = data.get("chart", {}).get("result")
                if not result:
                    continue
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice")
                if price:
                    return int(round(price))
        except Exception:
            continue
    return None


def fetch_price(code6: str):
    price = fetch_naver_price(code6)
    if price is not None:
        return price
    return fetch_yf_price(code6)


def send_discord(messages: list[str]):
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ DISCORD_WEBHOOK_URL 미설정 - 알림 전송 생략")
        for m in messages:
            print("  [알림 미발송]", m)
        return
    if not messages:
        return

    content = "\n\n".join(messages)
    # Discord 메시지 길이 제한(2000자) 대비 분할
    chunks = []
    while content:
        chunks.append(content[:1900])
        content = content[1900:]

    for chunk in chunks:
        body = json.dumps({"content": chunk}).encode("utf-8")
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL, data=body, method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                r.read()
        except urllib.error.HTTPError as e:
          body = e.read().decode("utf-8", errors="ignore")

          print(f"❌ Discord 전송 실패")
          print(f"HTTP Status: {e.code}")
          print("Response:")
          print(body)

          success = False
        except Exception as e:
            print(f"❌ Discord 전송 실패: {e}")


def fmt_won(v):
    return f"{v:,.0f}원"


def main():
    snap  = load_json(POSITIONS_PATH, {})
    pas   = load_json(ALERTS_PATH, {})
    state = load_json(STATE_PATH, {})

    positions = snap.get("positions", [])
    if not positions:
        print("ℹ️ positions_snapshot.json에 보유종목 정보가 없습니다. "
              "app6.py에서 '📡 현재가 조회'를 한 번 실행해야 동기화됩니다.")
        return

    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    messages = []
    state_changed = False

    for pos in positions:
        sk    = pos.get("종목키")
        name  = pos.get("종목명", sk)
        avg   = float(pos.get("평균단가", 0) or 0)
        qty   = float(pos.get("잔고수량", 0) or 0)
        code6 = str(pos.get("종목코드6", "") or "").strip()

        if not sk or qty <= 0 or avg <= 0 or not code6:
            continue

        cur = fetch_price(code6)
        if cur is None:
            print(f"⚠️ {name}({code6}) 현재가 조회 실패 - 스킵")
            continue

        alert_cfg = pas.get(sk, {})
        target   = float(alert_cfg.get("target", 0) or 0)
        stoplos  = float(alert_cfg.get("stoplos", 0) or 0)
        trail_pct = float(alert_cfg.get("trailing_pct", 0) or 0)

        st_entry = state.get(sk, {})
        high = float(st_entry.get("high_since_buy", avg) or avg)
        fired = set(st_entry.get("fired", []))

        pnl_pct = (cur - avg) / avg * 100

        # 신고가 갱신 추적 (수익 중일 때만 의미 있음)
        if cur > high:
            high = cur
            st_entry["high_since_buy"] = high
            state_changed = True

        new_fired = set()

        # 1) 목표가 도달
        if target > 0:
            if cur >= target:
                new_fired.add("target")
                if "target" not in fired:
                    messages.append(
                        f"🎯 **{name}** 목표가 도달!\n"
                        f"현재가 {fmt_won(cur)} ≥ 목표가 {fmt_won(target)}\n"
                        f"평균단가 {fmt_won(avg)} · 수익률 {pnl_pct:+.2f}%"
                    )

        # 2) 손절가 도달
        if stoplos > 0:
            if cur <= stoplos:
                new_fired.add("stoplos")
                if "stoplos" not in fired:
                    messages.append(
                        f"🛑 **{name}** 손절가 도달!\n"
                        f"현재가 {fmt_won(cur)} ≤ 손절가 {fmt_won(stoplos)}\n"
                        f"평균단가 {fmt_won(avg)} · 수익률 {pnl_pct:+.2f}%"
                    )

        # 3) 트레일링 스탑
        if trail_pct > 0:
            if pnl_pct >= 0:
                # 수익 중: 신고가 대비 -trail_pct% 하락 시 발동
                trail_trigger_price = high * (1 - trail_pct / 100)
                if cur <= trail_trigger_price and high > avg:
                    new_fired.add("trailing")
                    if "trailing" not in fired:
                        drawdown = (cur - high) / high * 100
                        messages.append(
                            f"📉 **{name}** 트레일링 스탑 발동! (수익 구간)\n"
                            f"매수 후 신고가 {fmt_won(high)} → 현재가 {fmt_won(cur)} ({drawdown:+.2f}%)\n"
                            f"기준: 신고가 대비 -{trail_pct:.1f}% · 평균단가 {fmt_won(avg)} · 수익률 {pnl_pct:+.2f}%"
                        )
            else:
                # 손실 중: 평균단가 대비 -trail_pct% 도달 시 발동 (손절가와 동일 기준)
                trail_trigger_price = avg * (1 - trail_pct / 100)
                if cur <= trail_trigger_price:
                    new_fired.add("trailing")
                    if "trailing" not in fired:
                        messages.append(
                            f"📉 **{name}** 트레일링 스탑 발동! (손실 구간)\n"
                            f"평균단가 {fmt_won(avg)} 대비 -{trail_pct:.1f}% 도달 → 현재가 {fmt_won(cur)}\n"
                            f"수익률 {pnl_pct:+.2f}%"
                        )

        if new_fired != fired:
            st_entry["fired"] = sorted(new_fired)
            state_changed = True

        st_entry["last_price"] = cur
        st_entry["last_checked"] = now_str
        state[sk] = st_entry

    if messages:
        header = f"📊 주식 알림 ({now_str} KST)\n" + "─" * 20
        send_discord([header] + messages)
        print(f"✅ {len(messages)}건 알림 전송 완료")
    else:
        print("ℹ️ 발동된 알림 없음")

    if state_changed:
        save_json(STATE_PATH, state)
    elif not os.path.exists(STATE_PATH):
        # 최초 실행 등으로 파일이 아직 없으면 빈 상태라도 생성
        save_json(STATE_PATH, state)


if __name__ == "__main__":
    main()
