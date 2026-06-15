"""
보유종목 가격 알림 → Discord Webhook 전송

알림 종류:
  - near_target  : 목표가의 3% 이내 근접 시 사전 알림
  - target       : 목표가 도달
  - stoplos      : 손절가 도달
  - trailing     : 트레일링 스탑 발동
    · 수익 중 → 신고가 대비 -X% 하락 시
    · 손실 중 → 평단 대비 -X% 도달 시

중복 방지: 각 알림은 fired 상태로 기록, 트리거 구간 벗어나면 자동 리셋(재발동 가능)
설정값 변경: 변경된 항목의 fired만 자동 리셋
전송 실패: fired 갱신 보류 → 다음 실행에서 재시도
"""

import json
import os
import urllib.request
import urllib.error
from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

POSITIONS_PATH  = "positions_snapshot.json"
ALERTS_PATH     = "price_alerts.json"
STATE_PATH      = "alert_state.json"
NEAR_TARGET_PCT = 3.0  # 목표가 X% 이내 진입 시 사전 알림

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
    url = f"https://m.stock.naver.com/api/stock/{code6}/basic"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
        "Referer": "https://m.finance.naver.com/",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())

            market_status = data.get("marketStatus", "")  # PREOPEN / OPEN / CLOSE 등

            # NXT 시간외(프리/애프터마켓) 시세 우선 사용
            over = data.get("overMarketPriceInfo", {})
            over_status = over.get("overMarketStatus", "")
            over_price_str = over.get("overPrice", "")

            if over_status == "OPEN" and over_price_str:
                price = int(str(over_price_str).replace(",", ""))
                session = over.get("tradingSessionType", "")
                print(f"  [{code6}] NXT {session} 시세: {price:,}원 (정규장 상태: {market_status})")
                return price

            # NXT 미운영 시간 → KRX 현재가/종가
            price_str = data.get("closePrice") or data.get("currentPrice") or data.get("stockPrice")
            if price_str:
                price = int(str(price_str).replace(",", ""))
                print(f"  [{code6}] KRX 시세: {price:,}원 (장 상태: {market_status})")
                return price

    except Exception as e:
        print(f"⚠️ {code6} 네이버 가격 조회 실패: {e}")
    return None


def fetch_yf_price(code6: str):
    for suffix in ("KS", "KQ"):
        ticker = f"{code6}.{suffix}"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1m"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
                result = data.get("chart", {}).get("result")
                if not result:
                    continue
                price = result[0].get("meta", {}).get("regularMarketPrice")
                if price:
                    return int(round(price))
        except Exception:
            continue
    return None


def fetch_price(code6: str):
    price = fetch_naver_price(code6)
    return price if price is not None else fetch_yf_price(code6)


def send_discord(messages: list[str]) -> bool:
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ DISCORD_WEBHOOK_URL 미설정 - 알림 전송 생략")
        for m in messages:
            print("  [미발송]", m)
        return False
    if not messages:
        return True

    print(f"📤 Discord 전송 시도: {len(messages)}건")
    content = "\n\n".join(messages)
    chunks, all_ok = [], True
    while content:
        chunks.append(content[:1900])
        content = content[1900:]

    for i, chunk in enumerate(chunks):
        body = json.dumps({"content": chunk}).encode("utf-8")
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL, data=body, method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                r.read()
                print(f"✅ chunk {i+1}/{len(chunks)} 전송 성공 (status={r.status})")
        except urllib.error.HTTPError as e:
            print(f"❌ Discord 전송 실패 HTTP {e.code}: {e.read().decode('utf-8', errors='ignore')}")
            all_ok = False
        except Exception as e:
            print(f"❌ Discord 전송 실패: {type(e).__name__}: {e}")
            all_ok = False
    return all_ok


def fmt_won(v):
    return f"{v:,.0f}원"


def check_alerts(name, avg, cur, target, stoplos, trail_pct, fired, high):
    """
    각 알림 조건 체크.
    반환: (messages, new_fired_set)
      - messages: 새로 발생한 알림 메시지 목록
      - new_fired_set: 현재 트리거 상태인 알림 키 집합 (트리거 안 된 건 포함 안 됨)
    """
    msgs = []
    new_fired = set()
    pnl_pct = (cur - avg) / avg * 100

    # 1) 목표가 근처 사전 알림
    if target > 0:
        near_trigger = target * (1 - NEAR_TARGET_PCT / 100)
        if cur >= near_trigger:
            new_fired.add("near_target")
            if "near_target" not in fired and cur < target:
                gap_pct = (target - cur) / cur * 100
                msgs.append(
                    f"🔔 **{name}** 목표가 근접!\n"
                    f"현재가 {fmt_won(cur)} → 목표가 {fmt_won(target)} ({gap_pct:.1f}% 남음)\n"
                    f"평균단가 {fmt_won(avg)} · 수익률 {pnl_pct:+.2f}%"
                )

    # 2) 목표가 도달
    if target > 0:
        if cur >= target:
            new_fired.add("target")
            if "target" not in fired:
                msgs.append(
                    f"🎯 **{name}** 목표가 도달!\n"
                    f"현재가 {fmt_won(cur)} ≥ 목표가 {fmt_won(target)}\n"
                    f"평균단가 {fmt_won(avg)} · 수익률 {pnl_pct:+.2f}%"
                )

    # 3) 손절가 도달
    if stoplos > 0:
        if cur <= stoplos:
            new_fired.add("stoplos")
            if "stoplos" not in fired:
                msgs.append(
                    f"🛑 **{name}** 손절가 도달!\n"
                    f"현재가 {fmt_won(cur)} ≤ 손절가 {fmt_won(stoplos)}\n"
                    f"평균단가 {fmt_won(avg)} · 수익률 {pnl_pct:+.2f}%"
                )

    # 4) 트레일링 스탑
    if trail_pct > 0:
        if pnl_pct >= 0:
            # 수익 중: 신고가 대비 -trail_pct%
            trig = high * (1 - trail_pct / 100)
            print(f"  [트레일링/수익] 신고가={fmt_won(high)} 트리거={fmt_won(trig)} 현재={fmt_won(cur)} 충족={cur<=trig and high>avg}")
            if cur <= trig and high > avg:
                new_fired.add("trailing")
                if "trailing" not in fired:
                    drawdown = (cur - high) / high * 100
                    msgs.append(
                        f"📉 **{name}** 트레일링 스탑! (수익 구간)\n"
                        f"신고가 {fmt_won(high)} → 현재가 {fmt_won(cur)} ({drawdown:+.2f}%)\n"
                        f"기준: 신고가 대비 -{trail_pct:.1f}% · 평균단가 {fmt_won(avg)} · 수익률 {pnl_pct:+.2f}%"
                    )
                else:
                    print(f"  -> 이미 fired")
        else:
            # 손실 중: 평단 대비 -trail_pct%
            trig = avg * (1 - trail_pct / 100)
            print(f"  [트레일링/손실] 트리거={fmt_won(trig)} 현재={fmt_won(cur)} 충족={cur<=trig}")
            if cur <= trig:
                new_fired.add("trailing")
                if "trailing" not in fired:
                    msgs.append(
                        f"📉 **{name}** 트레일링 스탑! (손실 구간)\n"
                        f"평균단가 {fmt_won(avg)} 대비 -{trail_pct:.1f}% 도달 → 현재가 {fmt_won(cur)}\n"
                        f"수익률 {pnl_pct:+.2f}%"
                    )
                else:
                    print(f"  -> 이미 fired")

    return msgs, new_fired


def main():
    snap  = load_json(POSITIONS_PATH, {})
    pas   = load_json(ALERTS_PATH, {})
    state = load_json(STATE_PATH, {})

    positions = snap.get("positions", [])
    if not positions:
        print("ℹ️ positions_snapshot.json 없음. app6.py에서 '📡 현재가 조회' 먼저 실행 필요.")
        return

    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    all_messages = []   # 전송할 메시지
    pending = {}        # sk → new_fired (전송 성공 후 state에 반영)
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
        target    = float(alert_cfg.get("target", 0) or 0)
        stoplos   = float(alert_cfg.get("stoplos", 0) or 0)
        trail_pct = float(alert_cfg.get("trailing_pct", 0) or 0)

        st_entry = state.get(sk, {})
        high  = float(st_entry.get("high_since_buy", avg) or avg)
        fired = set(st_entry.get("fired", []))

        # 설정값 변경 감지 → 변경된 항목의 fired만 리셋
        prev_cfg = st_entry.get("_alert_cfg", {})
        curr_cfg = {"target": target, "stoplos": stoplos, "trailing_pct": trail_pct}
        if prev_cfg != curr_cfg and prev_cfg:
            reset_keys = []
            if prev_cfg.get("target") != target:
                fired -= {"target", "near_target"}
                reset_keys += ["target", "near_target"]
            if prev_cfg.get("stoplos") != stoplos:
                fired -= {"stoplos"}
                reset_keys.append("stoplos")
            if prev_cfg.get("trailing_pct") != trail_pct:
                fired -= {"trailing"}
                reset_keys.append("trailing")
            if reset_keys:
                print(f"⚙️ {name} 설정 변경 ({', '.join(reset_keys)}) → 해당 fired 리셋")
            st_entry["fired"] = sorted(fired)
            st_entry["_alert_cfg"] = curr_cfg
            state_changed = True
        elif not prev_cfg:
            st_entry["_alert_cfg"] = curr_cfg
            state_changed = True

        # 신고가 갱신
        pnl_pct = (cur - avg) / avg * 100
        if cur > high:
            high = cur
            st_entry["high_since_buy"] = high
            state_changed = True

        print(f"\n===== {name} ===== 현재가={fmt_won(cur)} 수익률={pnl_pct:+.2f}%")

        msgs, new_fired = check_alerts(name, avg, cur, target, stoplos, trail_pct, fired, high)

        # 트리거 벗어난 항목은 fired에서 제거 (재발동 가능하도록)
        resettable = {"target", "near_target", "stoplos", "trailing"}
        auto_reset = (fired & resettable) - new_fired
        if auto_reset:
            fired -= auto_reset
            print(f"  🔄 {name} 트리거 해제 → {auto_reset} fired 리셋")
            st_entry["fired"] = sorted(fired - auto_reset)
            state_changed = True

        if msgs:
            all_messages.extend(msgs)
            pending[sk] = new_fired

        st_entry["last_price"] = cur
        st_entry["last_checked"] = now_str
        state[sk] = st_entry

    if all_messages:
        header = f"📊 주식 알림 ({now_str} KST)\n" + "─" * 20
        ok = send_discord([header] + all_messages)
        if ok:
            print(f"\n✅ {len(all_messages)}건 알림 전송 완료")
            for sk2, nf in pending.items():
                state[sk2]["fired"] = sorted(nf)
                state_changed = True
        else:
            print(f"\n❌ 전송 실패 - fired 갱신 보류 (다음 실행 재시도)")
    else:
        print("\nℹ️ 발동된 알림 없음")

    if state_changed or not os.path.exists(STATE_PATH):
        save_json(STATE_PATH, state)


if __name__ == "__main__":
    main()
