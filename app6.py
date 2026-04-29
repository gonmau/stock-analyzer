import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import re
import json
import io
from datetime import date, datetime

st.set_page_config(page_title="주식 종목별 손익 분석기", layout="wide")


# ==========================================
# yfinance 현재가 조회
# ==========================================

# 종목명 → yfinance 티커 수동 매핑 테이블
# 거래내역에 종목코드6이 없을 때 이름으로 fallback
_YF_NAME_MAP: dict[str, str] = {
    # ── KOSPI ──────────────────────────────────────────
    '삼성전자':         '005930.KS',
    '삼성물산':         '028260.KS',
    'SK하이닉스':       '000660.KS',
    '하이닉스':         '000660.KS',
    'LG전자':           '066570.KS',
    'SK':               '034730.KS',
    'SKC':              '011790.KS',
    'KT':               '030200.KS',
    '강원랜드':         '035250.KS',
    '대우건설':         '047040.KS',
    '대웅제약':         '069620.KS',
    '대한전선':         '001440.KS',
    '대우차판매':       '000070.KS',
    '대정화금':         '024680.KS',
    '세아제강':         '003030.KS',
    '조광페인트':       '004910.KS',
    '두산에너빌리티':   '034020.KS',
    '두산중공업':       '034020.KS',
    '현대건설':         '000720.KS',
    '현대자동차':       '005380.KS',
    '현대차':           '005380.KS',
    '현대로템':         '064350.KS',
    '한국전력공사':     '015760.KS',
    '한국항공우주':     '047810.KS',
    '한국항공우주산업': '047810.KS',
    '한국주철관':       '008960.KS',
    '한국주철관공업':   '008960.KS',
    '한국콜마홀딩스':   '024720.KS',
    '한화테크윈':       '012450.KS',
    '케이티앤지':       '033780.KS',
    '아시아나항공':     '020560.KS',
    '아남전자':         '008360.KS',
    '진원생명과학':     '011000.KS',
    'LG이노텍':         '011070.KS',
    '후성':             '093370.KS',
    '기아차':           '000270.KS',
    '기아':             '000270.KS',
    '일진홀딩스':       '015860.KS',
    '명신산업':         '009900.KS',
    '신신제약':         '002800.KS',
    '상신전자':         '004920.KS',
    '이화공영':         '001840.KS',
    '한창':             '005650.KS',
    '한국콜마':         '161890.KS',
    '한국화장품':       '008060.KS',
    '한국화장품제조':   '003350.KS',
    # ── KOSDAQ ─────────────────────────────────────────
    '안랩':             '053800.KQ',
    '안철수연구소':     '053800.KQ',
    '카카오':           '035720.KQ',
    '다음':             '035720.KQ',
    '다음카카오':       '035720.KQ',
    '넷마블':           '251270.KQ',
    '넷마블게임즈':     '251270.KQ',
    '텔콘RF제약':       '200230.KQ',
    '텔콘알에프제약':   '200230.KQ',
    '두산로보틱스':     '454910.KQ',
    '제일약품':         '271980.KQ',
    '지누스':           '013890.KQ',
    '에이프로젠HG':     '239610.KQ',
    '에이프로젠H&G':    '239610.KQ',
    '에이프로젠H':      '239610.KQ',
    '펄어비스':         '263750.KQ',
    '이퓨쳐':           '058220.KQ',
    '라이프앤비':       '067160.KQ',
    '아가방컴퍼니':     '013990.KQ',
    '아가방앤컴퍼니':   '013990.KQ',
    '대현':             '016090.KQ',
    '동양철관':         '008970.KQ',
    '에스맥':           '097780.KQ',
    '보령메디앙스':     '014200.KQ',
    '바른손':           '018700.KQ',
    '바른손게임즈':     '018700.KQ',
    '시노펙스':         '025320.KQ',
    'iMBC':             '052260.KQ',
    '이노셀':           '954100.KQ',
    '지아이바이오':     '064990.KQ',
    '써니전자':         '004770.KQ',
    '우리들생명과학':   '011620.KQ',
    '우리들제약':       '011620.KQ',
    '미래산업':         '025560.KQ',
    '루미마이크로':     '032680.KQ',
    '디아이디':         '093340.KQ',
    '디아이':           '003160.KQ',
    '와이디온라인':     '052400.KQ',
    'EG':               '037370.KQ',
    '경동제약':         '011040.KQ',
    '다믈멀티미디어':   '093720.KQ',
    '모베이스':         '101330.KQ',
    '어보브반도체':     '102120.KQ',
    '누리텔레콤':       '040160.KQ',
    '모다정보통신':     '023160.KQ',
    '효성ITX':          '094280.KQ',
    '한국콜마':         '161890.KQ',
    '웹젠':             '069080.KQ',
    '코콤':             '015710.KQ',
    '컴투스':           '078340.KQ',
    'NHN엔터테인먼트':  '181710.KQ',
    '에스넷':           '038680.KQ',
    '에스넷시스템':     '038680.KQ',
    '리젠':             '038070.KQ',
    '휴메딕스':         '200670.KQ',
    '보타바이오':       '326030.KQ',
    '키이스트':         '054780.KQ',
    '웰크론':           '065950.KQ',
    '쌍방울':           '102280.KQ',
    '아이즈비전':       '031310.KQ',
    '제노포커스':       '187420.KQ',
    '내츄럴엔도텍':     '168330.KQ',
    '형지엘리트':       '093240.KQ',
    '셀루메드':         '049180.KQ',
    '크리스탈':         '083790.KQ',
    '초록뱀':           '047820.KQ',
    '버추얼텍':         '093690.KQ',
    '다날':             '064260.KQ',
    '손오공':           '066910.KQ',
    'JW생명과학':       '234080.KQ',
    '웹스':             '066820.KQ',
    '유니더스':         '011320.KQ',
    '에이프로젠':       '239610.KQ',
    '파인디앤씨':       '038950.KQ',
    '인프라웨어':       '041020.KQ',
    'DSR':              '155660.KQ',
    '자연과환경':       '043910.KQ',
    '일신바이오':       '052670.KQ',
    '제이스텍':         '067290.KQ',
    '코리아나':         '027050.KQ',
    '엠젠플러스':       '032790.KQ',
    '씨트리':           '051480.KQ',
    '신라젠':           '215600.KQ',
    '엔에스엔':         '080000.KQ',
    '엔씨소프트':       '036570.KQ',
    '파루':             '043200.KQ',
    '레이젠':           '040570.KQ',
    '썬텍':             '107610.KQ',
    '씨아이테크':       '040960.KQ',
    '셀트리온헬스케어': '091990.KQ',
    'JYP Ent.':         '035900.KQ',
    '제이와이피엔터테인먼트': '035900.KQ',
    '넵튠':             '217270.KQ',
    '유니슨':           '018000.KQ',
    '감마누':           '192410.KQ',
    '티슈진':           '214870.KQ',
    'CMG제약':          '058820.KQ',
    '에스티큐브':       '052020.KQ',
    '메지온':           '140410.KQ',
    '에이치엘비':       '028300.KQ',
    '에이치엘비생명과학': '067630.KQ',
    '에이치엘비제약':   '022350.KQ',
    '혜인':             '003010.KQ',
    '대유위니아':       '071840.KQ',
    '네이처셀':         '007390.KQ',
    '넥슨지티':         '041140.KQ',
    '현대바이오사이언스': '048410.KQ',
    '씨젠':             '096530.KQ',
    '셀트리온제약':     '068760.KQ',
    '에스씨디':         '042110.KQ',
    '퀀타매트릭스':     '317690.KQ',
    '디에스씨인베스트먼트': '430720.KQ',
    '엔젠바이오':       '354490.KQ',
    '포인트모바일':     '318020.KQ',
    '알체라':           '347570.KQ',
    '박셀바이오':       '323990.KQ',
    '포스코아이씨티':   '022100.KQ',
    '포스코DX':         '022100.KQ',
    '한네트':           '064340.KQ',
    '신성델타테크':     '065350.KQ',
    '넥슨게임즈':       '225570.KQ',
    '로보티즈':         '108490.KQ',
    '로보AI':           '338100.KQ',
    # ── ETF (KS) ───────────────────────────────────────
    '미래에셋TIGER인도니프티50': '448540.KS',
    '신한SOL조선TOP3플러스':     '465670.KS',
    # ── 미국 주식 (티커 그대로) ────────────────────────
    '메타플랫폼스':     'META',
    '메타 플랫폼스':    'META',
    '페이스북':         'META',
    '브로드컴':         'AVGO',
    '애플':             'AAPL',
    '테슬라':           'TSLA',
}

# KOSPI/KOSDAQ 판별: 간단히 코드 앞자리로 구분 (완전하진 않지만 실용적)
_KOSDAQ_PREFIXES = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')

def _code_to_yf(code6: str) -> list[str]:
    """6자리 종목코드 → yfinance 티커 후보 리스트 (KS 먼저, KQ 두 번째)."""
    return [f"{code6}.KS", f"{code6}.KQ"]


def _is_us_ticker(ticker: str) -> bool:
    """META, AAPL 같은 미국 티커 여부."""
    return bool(ticker) and '.' not in ticker and ticker.upper() == ticker


@st.cache_data(ttl=300, show_spinner=False)
def fetch_current_prices_yf(yf_tickers: tuple) -> dict:
    """
    yf_tickers: ('005930.KS', '263750.KQ', ...)
    반환: {'005930.KS': 71400, '263750.KQ': 18500, ...}
    5분 캐시.
    """
    import yfinance as yf
    if not yf_tickers:
        return {}
    try:
        data = yf.download(
            list(yf_tickers),
            period="5d",
            interval="1d",
            auto_adjust=True,
            progress=False,
        )
        prices = {}
        close = data.get('Close', data) if isinstance(data.columns, pd.MultiIndex) else data
        for ticker in yf_tickers:
            try:
                if isinstance(close.columns, pd.MultiIndex):
                    col = ('Close', ticker)
                    series = close[col].dropna() if col in close.columns else pd.Series(dtype=float)
                else:
                    series = close[ticker].dropna() if ticker in close.columns else pd.Series(dtype=float)
                if not series.empty:
                    prices[ticker] = int(series.iloc[-1])
            except Exception:
                continue
        return prices
    except Exception:
        return {}


def resolve_tickers_yf(holding_df: pd.DataFrame) -> dict:
    """
    종목키 → yfinance 티커 매핑.
    우선순위: 사용자 정의 매핑 → 종목코드6 → _YF_NAME_MAP → 못 찾으면 제외.
    """
    user_map = st.session_state.get('_user_ticker_map', {})
    candidates: dict[str, list[str]] = {}
    for _, row in holding_df.iterrows():
        key   = row['종목키']
        code6 = str(row.get('종목코드6', '') or '').strip()
        name  = row.get('종목명', key)
        norm  = normalize_stock_name(name)

        # 1순위: 사용자 정의 매핑
        if name in user_map:
            candidates[key] = [user_map[name]]
        elif norm in user_map:
            candidates[key] = [user_map[norm]]
        elif key in user_map:
            candidates[key] = [user_map[key]]
        # 2순위: 종목코드6
        elif len(code6) == 6 and code6.isdigit():
            candidates[key] = _code_to_yf(code6)
        # 3순위: 내장 매핑
        elif name in _YF_NAME_MAP:
            candidates[key] = [_YF_NAME_MAP[name]]
        elif norm in _YF_NAME_MAP:
            candidates[key] = [_YF_NAME_MAP[norm]]
        elif key in _YF_NAME_MAP:
            candidates[key] = [_YF_NAME_MAP[key]]

    # 2단계: 후보 티커 전체를 한 번에 yfinance 조회
    all_candidates = list({t for tl in candidates.values() for t in tl})
    prices = fetch_current_prices_yf(tuple(all_candidates))

    # 3단계: 유효한(가격이 있는) 티커 선택
    mapping: dict[str, str] = {}
    for key, ticker_list in candidates.items():
        for t in ticker_list:
            if t in prices:
                mapping[key] = t
                break
    return mapping


# ==========================================
# 종목명 정규화
# ==========================================
ALIAS_MAP = {
    '안철수연구소': '안랩',
    '다음': '카카오', '다음카카오': '카카오',
    '하이닉스': 'SK하이닉스', '에스케이하이닉스': 'SK하이닉스',
    '한국항공우주산업': '한국항공우주',
    '넷마블게임즈': '넷마블',
    '텔콘알에프제약': '텔콘RF제약',
    '삼성전자보통주': '삼성전자',
    'LG전자보통주': 'LG전자', 'SK보통주': 'SK',
    '대우건설보통주': '대우건설', '대웅제약보통주': '대웅제약',
    '두산로보틱스보통주': '두산로보틱스',
    '두산에너빌리티보통주': '두산에너빌리티',
    '제일약품보통주': '제일약품', '지누스보통주': '지누스',
    '카카오보통주': '카카오', '현대건설보통주': '현대건설',
    '후성보통주': '후성', '한국전력공사보통주': '한국전력공사',
    '현대자동차보통주': '현대자동차', '케이티앤지보통주': '케이티앤지',
    '진원생명과학보통주': '진원생명과학', '아시아나항공보통주': '아시아나항공',
    '에이프로젠HG': '에이프로젠HG', '에이프로젠H&G': '에이프로젠HG',
    '에이프로젠H': '에이프로젠HG',
    'LG이노텍보통주': 'LG이노텍',
    '한국항공우주산업보통주': '한국항공우주',
}

def normalize_stock_name(name):
    s = str(name).strip()
    s = s.replace('&G', 'G').replace('&', '')
    s = re.sub(r'\(reg\.?s?\)', '', s, flags=re.IGNORECASE)
    s = re.sub(r'[\[\]\(\)\-_,.]', '', s)
    s = re.sub(r'\s+', '', s)
    s = re.sub(r'(보통주|보통)$', '', s)
    return ALIAS_MAP.get(s, s)


def parse_exclude_symbol_keys(text):
    """사이드바 제외 목록(줄바꿈/쉼표) → 정규화 종목키 집합."""
    if not text or not str(text).strip():
        return set()
    parts = re.split(r'[\n,;]+', str(text))
    return {normalize_stock_name(p.strip()) for p in parts if p and str(p).strip()}


def apply_exclude_symbols(df, exclude_text):
    """펀드·상폐 등 엑셀만으로 맞추기 어려운 종목 제외. 수동 입력 거래도 동일 키면 함께 제외됨."""
    if df.empty:
        return df
    keys = parse_exclude_symbol_keys(exclude_text)
    if not keys:
        return df
    if '종목키' not in df.columns:
        return df
    return df[~df['종목키'].isin(keys)].copy()


def sort_trades_for_settlement_export(df, same_day_buy_first=True):
    """
    체결시각이 없는 일자별·입금일 기준 원장용 정렬.
    동일 거래일·동일 종목에서 표에 매도가 매수보다 위에 있으면 잔고가 깨지므로,
    (선택 시) 매수 행을 먼저 두고 시트상 상대 순서(_raw_order)는 유지한다.
    """
    if df.empty:
        return df
    df = df.copy()
    keys = ['거래일자', '종목키']
    if '_file_ord' in df.columns:
        keys.append('_file_ord')
    if same_day_buy_first and '매매유형' in df.columns:
        df['_side_sort'] = df['매매유형'].map({'BUY': 0, 'SELL': 1}).fillna(2)
        keys.append('_side_sort')
    if '_raw_order' in df.columns:
        keys.append('_raw_order')
    elif '_global_seq' in df.columns:
        keys.append('_global_seq')
    elif '_intra_file_seq' in df.columns:
        keys.append('_intra_file_seq')
    df = df.sort_values(keys, kind='mergesort').reset_index(drop=True)
    return df.drop(columns=['_side_sort'], errors='ignore')


# ==========================================
# session_state 초기화
# ==========================================
if 'manual_trades' not in st.session_state:
    st.session_state.manual_trades = []


def _load_default_exclude() -> str:
    """영구 기본 제외종목 로드: Secrets → exclude_default.txt → 빈 문자열 순서."""
    try:
        val = st.secrets.get("DEFAULT_EXCLUDE_SYMBOLS", "")
        if val and str(val).strip():
            return str(val).strip()
    except Exception:
        pass
    try:
        import os
        path = os.path.join(os.path.dirname(__file__), "exclude_default.txt")
        if os.path.exists(path):
            content = open(path, encoding="utf-8").read().strip()
            if content:
                return content
    except Exception:
        pass
    return ""


# 앱 최초 기동 시 기본값 주입 (_shadow 키 사용 — 위젯 key 직접 수정 금지)
if '_excl_shadow' not in st.session_state:
    st.session_state['_excl_shadow'] = _load_default_exclude()
if '_buy_first_shadow' not in st.session_state:
    st.session_state['_buy_first_shadow'] = True
if '_user_ticker_map' not in st.session_state:
    st.session_state['_user_ticker_map'] = {}   # {'종목명': 'XXXXXX.KQ'}


# ==========================================
# 1. 데이터 전처리
# ==========================================
def preprocess_data(file, file_name, file_order=0):
    try:
        if file_name.endswith('.csv'):
            try:
                df = pd.read_csv(file, encoding='utf-8')
            except Exception:
                df = pd.read_csv(file, encoding='cp949')
        else:
            df_raw = pd.read_excel(file, header=None)
            header_idx = 0
            for i, row in df_raw.iterrows():
                row_str = "".join(row.fillna('').astype(str))
                if '거래일자' in row_str and ('거래종류' in row_str or '종목명' in row_str):
                    header_idx = i
                    break
            df = pd.read_excel(file, header=header_idx)
        df.columns = [str(c).replace('\n', '').replace(' ', '').strip() for c in df.columns]
    except Exception as e:
        st.error(f"파일 로드 실패 ({file_name}): {e}")
        return pd.DataFrame()

    col_map = {
        '체결일자': '거래일자', '일자': '거래일자', '날짜': '거래일자',
        '거래종류': '매매구분', '구분': '매매구분', '거래구분': '매매구분',
        '종목': '종목명', '종목명(코드)': '종목명_코드',
        '단가': '거래단가', '수량': '거래수량',
    }
    df = df.rename(columns=col_map)

    if '거래일자' not in df.columns or '매매구분' not in df.columns:
        st.warning(f"'{file_name}' 필수 컬럼 누락. ({list(df.columns)})")
        return pd.DataFrame()

    for col in ['거래수량', '거래단가', '수수료', '제세금', '거래금액']:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(',', '').str.split('(').str[0].str.extract(r'([-\d.]+)')[0]
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if '종목명' in df.columns:
        df['종목명'] = df['종목명'].replace('', pd.NA).ffill()
        df['종목명'] = df['종목명'].astype(str).str.strip()
        df = df[df['종목명'].notna() & (df['종목명'] != '') & (df['종목명'] != 'nan')]
        if '거래수량' in df.columns:
            df = df[df['거래수량'] > 0]

    if df.empty:
        return pd.DataFrame()

    df['거래일자'] = pd.to_datetime(
        df['거래일자'].astype(str).str.replace('.', '-', regex=False), errors='coerce'
    )
    df = df.dropna(subset=['거래일자', '종목명']).reset_index(drop=True)

    if '거래단가' not in df.columns:
        df['거래단가'] = 0.0
    mask_no_price = (df['거래단가'] == 0) & (df['거래수량'] > 0) & (df['거래금액'] > 0)
    df.loc[mask_no_price, '거래단가'] = (df['거래금액'] / df['거래수량']).round().astype(int)

    # 집계 키는 반드시 '정규화 종목명'만 사용.
    # 일부 행만 종목코드가 있으면 6자리 vs 이름으로 종목키가 갈라져
    # 매수/매도가 다른 버킷에 쌓이고(누적매수==누적매도인데 잔고만 남는 현상) 잔고가 왜곡됨.
    df['종목키'] = df['종목명'].apply(normalize_stock_name)
    for c in ['종목코드', '단축코드', '종목번호', '종목명_코드']:
        if c in df.columns:
            code_6 = df[c].astype(str).str.extract(r'(\d{6})', expand=False)
            df['종목코드6'] = code_6
            break

    def classify_type(x):
        x = str(x)
        if any(w in x for w in ['출고', '매도', '판매', '환매']): return 'SELL'
        if any(w in x for w in ['입고', '매수', '구매', '재투자']): return 'BUY'
        return 'ETC'

    df['매매유형'] = df['매매구분'].apply(classify_type)
    df = df[df['매매유형'] != 'ETC'].reset_index(drop=True)

    # 시트 원본 행 순서(ETC 제거 후). 체결시각 없음 → 입금일/일자별 원장은 동일일 매도가 위에 올 수 있음.
    df['_raw_order'] = range(len(df))
    df['_file_ord'] = int(file_order)
    buy_first = st.session_state.get('opt_same_day_buy_first', True)
    df = sort_trades_for_settlement_export(df, same_day_buy_first=buy_first)
    df['_intra_file_seq'] = range(len(df))

    df['계좌'] = file_name.split('.')[0]
    df['수동입력'] = False
    return df


# ==========================================
# 2. 수동 거래 → DataFrame 변환
# ==========================================
def manual_trades_to_df(trades):
    if not trades:
        return pd.DataFrame()
    rows = []
    for i, t in enumerate(trades):
        qty   = float(t['수량'])
        price = float(t['단가'])
        ttype = 'BUY' if t['매매유형'] == '매수' else 'SELL'
        rows.append({
            '거래일자': pd.to_datetime(t['날짜']),
            '매매구분': t['매매유형'],
            '종목명':   t['종목명'],
            '거래수량': qty,
            '거래단가': price,
            '거래금액': qty * price,
            '수수료':   float(t.get('수수료', 0)),
            '계좌':     t.get('계좌', '수동입력'),
            '종목키':   normalize_stock_name(t['종목명']),
            '매매유형': ttype,
            '수동입력': True,
            '_file_ord': 10**6,
            '_intra_file_seq': i,
            '_raw_order': i,
            '_global_seq': 10**9 + i,
        })
    return pd.DataFrame(rows)


# ==========================================
# 3. 잔고 계산 (이동평균단가법)
# ==========================================
def _sort_trades_chronological(df):
    """거래일자가 같을 때 수량으로 정렬하면 체결 순서가 뒤집혀 잔고가 틀어짐. 원본 순서 유지."""
    df = df.copy()
    if '_global_seq' in df.columns and df['_global_seq'].notna().all():
        df = df.sort_values(['거래일자', '_global_seq'], kind='mergesort').reset_index(drop=True)
    elif '_file_ord' in df.columns and '_raw_order' in df.columns:
        df = df.sort_values(['거래일자', '_file_ord', '_raw_order'], kind='mergesort').reset_index(drop=True)
    elif '_file_ord' in df.columns and '_intra_file_seq' in df.columns:
        df = df.sort_values(['거래일자', '_file_ord', '_intra_file_seq'], kind='mergesort').reset_index(drop=True)
    elif '_raw_order' in df.columns:
        df = df.sort_values(['거래일자', '_raw_order'], kind='mergesort').reset_index(drop=True)
    else:
        df = df.sort_values(['거래일자'], kind='mergesort').reset_index(drop=True)
    buy_first = st.session_state.get('opt_same_day_buy_first', True)
    return sort_trades_for_settlement_export(df, same_day_buy_first=buy_first)


def calculate_positions(df):
    if df.empty:
        return pd.DataFrame()
    df = _sort_trades_chronological(df)
    positions = {}

    for _, row in df.iterrows():
        symbol       = row.get('종목키', row['종목명'])
        display_name = row['종목명']
        qty   = abs(float(row.get('거래수량', 0)))
        price = float(row.get('거래단가', 0))
        fee   = float(row.get('수수료', 0)) + float(row.get('제세금', 0))
        ttype = row.get('매매유형', 'ETC')

        if symbol not in positions:
            positions[symbol] = {
                'display_name': display_name,
                'qty': 0.0, 'avg_price': 0.0, 'holding_cost': 0.0,
                'total_buy_qty': 0.0, 'total_sell_qty': 0.0,
                'total_buy_amt': 0.0, 'total_sell_amt': 0.0,
                'realized_pnl': 0.0, 'total_fee': 0.0,
            }
        p = positions[symbol]

        if ttype == 'BUY' and price > 0 and qty > 0:
            buy_amt = qty * price
            new_qty = p['qty'] + qty
            p['avg_price']     = (p['holding_cost'] + buy_amt) / new_qty
            p['holding_cost']  += buy_amt
            p['qty']           += qty
            p['total_buy_qty'] += qty
            p['total_buy_amt'] += buy_amt
            p['total_fee']     += fee

        elif ttype == 'SELL' and qty > 0:
            aq = min(qty, p['qty'])
            if aq > 0:
                if price > 0:
                    p['realized_pnl']   += aq * (price - p['avg_price']) - fee
                    p['holding_cost']   -= aq * p['avg_price']
                    p['total_sell_amt'] += aq * price
                else:
                    # 단가 0 매도(출고 등): 실현손익·매도금액은 0이어도 보유수량·원가는 감소해야 함
                    p['holding_cost'] -= aq * p['avg_price']
                    p['realized_pnl']   -= fee
            p['qty'] -= qty
            if p['qty'] < 0:
                p['qty'] = 0.0; p['holding_cost'] = 0.0; p['avg_price'] = 0.0
            elif p['qty'] > 0 and p['holding_cost'] > 0:
                p['avg_price'] = p['holding_cost'] / p['qty']
            elif p['qty'] == 0:
                p['holding_cost'] = 0.0; p['avg_price'] = 0.0
            p['total_sell_qty'] += qty
            p['total_fee']      += fee

    rows = []
    for sym, p in positions.items():
        rows.append({
            '종목키': sym, '종목명': p['display_name'],
            '잔고수량':    round(p['qty']),
            '평균단가':    round(p['avg_price']),
            '보유원가':    round(p['holding_cost']),
            '누적매수수량': round(p['total_buy_qty']),
            '누적매도수량': round(p['total_sell_qty']),
            '누적매수금액': round(p['total_buy_amt']),
            '누적매도금액': round(p['total_sell_amt']),
            '실현손익':    round(p['realized_pnl']),
            '총수수료':    round(p['total_fee']),
        })
    return pd.DataFrame(rows)


# ==========================================
# 4. 종목별 거래 상세 (행별 잔고/손익)
# ==========================================
def calculate_trade_detail(df, symbol_key):
    s_df = df[df['종목키'] == symbol_key].copy()
    s_df = _sort_trades_chronological(s_df)
    qty = 0.0; avg_price = 0.0; holding_cost = 0.0
    rows = []
    for _, row in s_df.iterrows():
        trade_qty = abs(float(row.get('거래수량', 0)))
        price     = float(row.get('거래단가', 0))
        fee       = float(row.get('수수료', 0)) + float(row.get('제세금', 0))
        ttype     = row.get('매매유형', 'ETC')
        realized  = 0.0

        if ttype == 'BUY' and price > 0 and trade_qty > 0:
            buy_amt = trade_qty * price
            new_qty = qty + trade_qty
            avg_price    = (holding_cost + buy_amt) / new_qty
            holding_cost += buy_amt
            qty = new_qty
        elif ttype == 'SELL' and trade_qty > 0:
            aq = min(trade_qty, qty)
            if aq > 0:
                if price > 0:
                    realized     = aq * (price - avg_price) - fee
                    holding_cost -= aq * avg_price
                else:
                    holding_cost -= aq * avg_price
                    realized     = -fee
            qty -= trade_qty
            if qty < 0:
                qty = 0.0; holding_cost = 0.0; avg_price = 0.0
            elif qty > 0 and holding_cost > 0:
                avg_price = holding_cost / qty
            elif qty == 0:
                holding_cost = 0.0; avg_price = 0.0

        r = row.to_dict()
        r['잔고수량'] = round(qty)
        r['평균단가'] = round(avg_price)
        r['실현손익'] = round(realized)
        rows.append(r)
    return pd.DataFrame(rows)


def render_lot_tabs(combined_df, symbol_key, live_price, pos_row):
    """
    계좌별 탭 + '전체' 탭으로 FIFO 로트 손익 테이블 렌더링.
    live_price: int|None, pos_row: DataFrame row
    """
    _qty_held = float(pos_row.iloc[0]['잔고수량']) if not pos_row.empty else 0
    if not live_price:
        st.info("💡 Tab1의 '📡 현재가 조회' 버튼을 누르면 매수 로트별 손익이 표시됩니다.")
        return
    if _qty_held <= 0:
        st.info("보유 수량이 없습니다 (전량 매도 완료).")
        return

    all_lots = calculate_fifo_lots(combined_df, symbol_key)
    if all_lots.empty:
        st.info("로트 데이터가 없습니다.")
        return

    # 계좌 목록
    accounts = sorted(all_lots['계좌'].dropna().unique().tolist())
    tab_labels = ['전체'] + accounts
    tabs = st.tabs(tab_labels)

    def _render_lots_table(lots_df):
        if lots_df.empty:
            st.info("해당 계좌의 보유 로트가 없습니다.")
            return
        lots_df = lots_df.copy()
        lots_df['현재가']     = live_price
        lots_df['단가손익']   = lots_df['매수단가'].apply(lambda p: int(live_price - p))
        lots_df['수익률(%)']  = lots_df['매수단가'].apply(
            lambda p: round((live_price - p) / p * 100, 2) if p > 0 else 0)
        lots_df['미실현손익'] = (lots_df['단가손익'] * lots_df['잔여수량']).astype(int)
        lots_df['매수금액']   = (lots_df['매수단가'] * lots_df['잔여수량']).astype(int)
        lots_df['평가금액']   = (live_price * lots_df['잔여수량']).astype(int)
        lots_df['매수일']     = pd.to_datetime(lots_df['매수일']).dt.strftime('%Y-%m-%d')

        tunr  = lots_df['미실현손익'].sum()
        tcost = lots_df['매수금액'].sum()
        tret  = tunr / tcost * 100 if tcost else 0

        lc1, lc2, lc3, lc4 = st.columns(4)
        lc1.metric("잔여 로트 수",       f"{len(lots_df)}건")
        lc2.metric("미실현손익 합",       f"₩{tunr:,.0f}",
                   delta=f"{tret:+.2f}%",
                   delta_color="normal" if tunr >= 0 else "inverse")
        lc3.metric("총 매수금액(잔여)",  f"₩{tcost:,.0f}")
        lc4.metric("총 평가금액",         f"₩{lots_df['평가금액'].sum():,.0f}")

        disp = lots_df[['매수일','매수단가','매수수량','잔여수량',
                         '현재가','단가손익','수익률(%)','미실현손익',
                         '매수금액','평가금액','계좌']]

        def _hl(row):
            try: bg = '#1a3a1a' if float(row.get('미실현손익', 0)) >= 0 else '#3a1a1a'
            except: bg = ''
            return [f'background-color:{bg};color:white'] * len(row)

        def _cl(val):
            try:
                v = float(val)
                if v > 0: return 'color:#4ecca3;font-weight:bold'
                if v < 0: return 'color:#ff6b6b;font-weight:bold'
            except: pass
            return ''

        st.dataframe(
            disp.style.apply(_hl, axis=1)
            .map(_cl, subset=['단가손익','수익률(%)','미실현손익'])
            .format({'매수단가':'{:,.0f}','매수수량':'{:,.0f}','잔여수량':'{:,.0f}',
                     '현재가':'{:,.0f}','단가손익':'{:+,.0f}','수익률(%)':'{:+.2f}',
                     '미실현손익':'{:+,.0f}','매수금액':'{:,.0f}','평가금액':'{:,.0f}'}),
            use_container_width=True,
            height=min(80 + len(lots_df) * 38, 500),
        )
        st.caption("🟢 초록 = 수익 로트 · 🔴 빨강 = 손실 로트  |  매도 완료 로트는 표시 안 함")

    with tabs[0]:  # 전체
        _render_lots_table(all_lots)

    for i, acc in enumerate(accounts):
        with tabs[i + 1]:
            _render_lots_table(all_lots[all_lots['계좌'] == acc])
    """
    FIFO 방식으로 매수 로트별 잔여수량을 계산.
    반환 컬럼: 거래일자, 매수단가, 매수수량, 잔여수량, 계좌
    잔여수량 > 0 인 로트만 반환 (매도로 완전히 소진된 로트 제외).
    """
    s_df = df[df['종목키'] == symbol_key].copy()
    s_df = _sort_trades_chronological(s_df)

    # 매수 로트 큐: [매수일, 매수단가, 원래수량, 잔여수량, 계좌, lot_idx]
    lots = []
    lot_idx = 0

    for _, row in s_df.iterrows():
        trade_qty = abs(float(row.get('거래수량', 0)))
        price     = float(row.get('거래단가', 0))
        ttype     = row.get('매매유형', 'ETC')

        if ttype == 'BUY' and price > 0 and trade_qty > 0:
            lots.append({
                'lot_idx':   lot_idx,
                '매수일':    row['거래일자'],
                '매수단가':  price,
                '매수수량':  trade_qty,
                '잔여수량':  trade_qty,
                '계좌':      row.get('계좌', ''),
            })
            lot_idx += 1

        elif ttype == 'SELL' and trade_qty > 0:
            # FIFO: 앞에서부터 차감
            remain = trade_qty
            for lot in lots:
                if remain <= 0:
                    break
                take = min(lot['잔여수량'], remain)
                lot['잔여수량'] -= take
                remain -= take

    # 잔여수량 > 0 인 로트만
    result = [l for l in lots if l['잔여수량'] > 0]
    return pd.DataFrame(result) if result else pd.DataFrame(
        columns=['lot_idx','매수일','매수단가','매수수량','잔여수량','계좌']
    )


# ==========================================
# 5. 백업 / 복원 헬퍼
# ==========================================
def build_backup_json():
    data = {
        'manual_trades': st.session_state.get('manual_trades', []),
        'exclude_symbols_text': st.session_state.get('exclude_symbols_text', '') or '',
        'opt_same_day_buy_first': bool(st.session_state.get('opt_same_day_buy_first', True)),
        'user_ticker_map': st.session_state.get('_user_ticker_map', {}),
    }
    if 'master_df' in st.session_state:
        df = st.session_state.master_df.copy()
        df['거래일자'] = df['거래일자'].astype(str)
        data['master_df'] = df.to_dict(orient='records')
    return json.dumps(data, ensure_ascii=False, indent=2)

def build_backup_excel():
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        if 'master_df' in st.session_state:
            df = st.session_state.master_df.copy()
            df['거래일자'] = df['거래일자'].astype(str)
            df.to_excel(writer, sheet_name='거래내역', index=False)
        if st.session_state.get('manual_trades'):
            pd.DataFrame(st.session_state.manual_trades).to_excel(
                writer, sheet_name='수동입력', index=False)
        if 'positions_df' in st.session_state:
            st.session_state.positions_df.to_excel(
                writer, sheet_name='잔고현황', index=False)
        settings_df = pd.DataFrame({
            '항목': ['exclude_symbols_text', 'opt_same_day_buy_first'],
            '값': [
                st.session_state.get('exclude_symbols_text', '') or '',
                'TRUE' if st.session_state.get('opt_same_day_buy_first', True) else 'FALSE',
            ],
        })
        settings_df.to_excel(writer, sheet_name='설정', index=False)
        # 사용자 티커 매핑
        utm = st.session_state.get('_user_ticker_map', {})
        pd.DataFrame({'종목명': list(utm.keys()), '티커': list(utm.values())}).to_excel(
            writer, sheet_name='티커매핑', index=False)
    buf.seek(0)
    return buf.read()

def _apply_settings_to_shadow(exclude_val: str | None, buy_first_val: bool | None):
    """
    복원 시 위젯 key를 직접 건드리지 않고 shadow 키에 쓴 뒤
    위젯 key 자체를 session_state에서 제거 → 다음 rerun에서 value= 로 재초기화.
    """
    if exclude_val is not None:
        st.session_state['_excl_shadow'] = exclude_val
        st.session_state.pop('exclude_symbols_text', None)   # 위젯 강제 재초기화
    if buy_first_val is not None:
        st.session_state['_buy_first_shadow'] = buy_first_val
        st.session_state.pop('opt_same_day_buy_first', None) # 위젯 강제 재초기화


def restore_from_json(uploaded):
    try:
        data = json.load(uploaded)
        if 'master_df' in data and data['master_df']:
            df = pd.DataFrame(data['master_df'])
            df['거래일자'] = pd.to_datetime(df['거래일자'], errors='coerce')
            st.session_state.master_df = df
        if 'manual_trades' in data:
            st.session_state.manual_trades = data['manual_trades']
        excl = str(data['exclude_symbols_text'] or '') if 'exclude_symbols_text' in data else None
        buy  = bool(data['opt_same_day_buy_first']) if 'opt_same_day_buy_first' in data else None
        _apply_settings_to_shadow(excl, buy)
        if 'user_ticker_map' in data and isinstance(data['user_ticker_map'], dict):
            st.session_state['_user_ticker_map'] = data['user_ticker_map']
        return True, "JSON 복원 완료 (설정·제외목록·티커매핑 포함)"
    except Exception as e:
        return False, str(e)


def restore_from_excel(uploaded):
    try:
        xls = pd.ExcelFile(uploaded)
        if '거래내역' in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name='거래내역')
            df['거래일자'] = pd.to_datetime(df['거래일자'], errors='coerce')
            st.session_state.master_df = df
        if '수동입력' in xls.sheet_names:
            manual_df = pd.read_excel(xls, sheet_name='수동입력')
            st.session_state.manual_trades = manual_df.to_dict(orient='records')
        excl, buy = None, None
        if '설정' in xls.sheet_names:
            sdf = pd.read_excel(xls, sheet_name='설정')
            if len(sdf.columns) >= 2:
                col0, col1 = sdf.columns[0], sdf.columns[1]
                for _, row in sdf.iterrows():
                    k = str(row.get(col0, '')).strip()
                    v = row.get(col1, '')
                    if k == 'exclude_symbols_text':
                        excl = '' if pd.isna(v) else str(v)
                    elif k == 'opt_same_day_buy_first':
                        buy = str(v).strip().upper() in ('TRUE', '1', 'YES', 'Y')
        _apply_settings_to_shadow(excl, buy)
        if '티커매핑' in xls.sheet_names:
            tdf = pd.read_excel(xls, sheet_name='티커매핑')
            if '종목명' in tdf.columns and '티커' in tdf.columns:
                st.session_state['_user_ticker_map'] = dict(
                    zip(tdf['종목명'].astype(str), tdf['티커'].astype(str))
                )
        return True, "Excel 복원 완료 (설정·제외목록·티커매핑 포함)"
    except Exception as e:
        return False, str(e)

def _build_combined():
    base = st.session_state.get('master_df', pd.DataFrame())
    manual_df = manual_trades_to_df(st.session_state.get('manual_trades', []))

    if not base.empty:
        base = base.copy()
        if '_global_seq' not in base.columns:
            if '_file_ord' in base.columns and '_raw_order' in base.columns:
                base = base.sort_values(
                    ['거래일자', '_file_ord', '_raw_order'], kind='mergesort'
                ).reset_index(drop=True)
            elif '_file_ord' in base.columns and '_intra_file_seq' in base.columns:
                base = base.sort_values(
                    ['거래일자', '_file_ord', '_intra_file_seq'], kind='mergesort'
                ).reset_index(drop=True)
            elif '_raw_order' in base.columns:
                base = base.sort_values(['거래일자', '_raw_order'], kind='mergesort').reset_index(drop=True)
            else:
                base = base.sort_values('거래일자', kind='mergesort').reset_index(drop=True)
            base['_global_seq'] = range(len(base))

    frames = [f for f in [base, manual_df] if not f.empty]
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = _sort_trades_chronological(out)
    if '_global_seq' in out.columns:
        out = out.reset_index(drop=True)
        out['_global_seq'] = range(len(out))
    return out


# ==========================================
# 6. 메인 UI
# ==========================================
st.title("📊 주식 통합 거래 분석 시스템")

# ── 사이드바 ──
with st.sidebar:
    st.header("📂 거래내역 파일 업로드")
    st.checkbox(
        "체결시각 없음: 동일 거래일·동일 종목은 매수를 먼저 처리 (입금일/일자별 원장용)",
        value=st.session_state.get('_buy_first_shadow', True),
        key="opt_same_day_buy_first",
        help="증권사 엑셀에 체결시각이 없을 때, 당일 표에서 매도가 매수보다 위에 있어도 잔고 계산상으로는 매수를 먼저 반영합니다.",
    )
    st.text_area(
        "잔고 계산에서 제외할 종목 (한 줄에 하나)",
        value=st.session_state.get('_excl_shadow', ''),
        height=88,
        key="exclude_symbols_text",
        placeholder="미래드림타겟주식A\n한진해운\n로코조이",
        help="펀드·상폐 등 원장과 맞출 수 없는 종목명을 적으면 해당 종목 거래는 집계에서 빠집니다. (수동 입력도 같은 이름이면 함께 제외)",
    )

    # ── 사용자 정의 티커 매핑 ──────────────────────────
    st.divider()
    st.subheader("🔗 현재가 조회 티커 직접 입력")
    st.caption("내장 코드에 없는 종목은 여기에 추가하세요. 종목명은 거래내역과 동일하게 입력.")

    utm = st.session_state.get('_user_ticker_map', {})

    # 기존 매핑 표시 + 삭제
    if utm:
        to_delete = []
        for name, ticker in list(utm.items()):
            c1, c2, c3 = st.columns([4, 3, 1])
            c1.text(name)
            c2.text(ticker)
            if c3.button("✕", key=f"del_ticker_{name}", help="삭제"):
                to_delete.append(name)
        for name in to_delete:
            del st.session_state['_user_ticker_map'][name]
        if to_delete:
            st.rerun()

    # 신규 추가
    with st.form("add_ticker_form", clear_on_submit=True):
        fc1, fc2 = st.columns(2)
        new_name   = fc1.text_input("종목명", placeholder="한국항공우주산업보통")
        new_ticker = fc2.text_input("티커", placeholder="047810.KS")
        if st.form_submit_button("추가", use_container_width=True):
            n = new_name.strip()
            t = new_ticker.strip()
            if n and t:
                st.session_state['_user_ticker_map'][n] = t
                st.rerun()
            else:
                st.warning("종목명과 티커를 모두 입력하세요.")
    st.caption(
        "체결일 ≠ 입금일: 엑셀에 없는 매도(예 삼성전자 4·16 매도입금만 반영)는 "
        "「수동 거래 입력」에서 체결일 기준으로 매도를 추가하세요."
    )
    files = st.file_uploader(
        "미래에셋/삼성/토스 파일", accept_multiple_files=True, type=['xlsx', 'xls', 'csv']
    )
    if st.button("데이터 분석 실행", type="primary"):
        if files:
            all_dfs = [preprocess_data(f, f.name, file_order=i) for i, f in enumerate(files)]
            base = pd.concat([d for d in all_dfs if not d.empty], ignore_index=True)
            if not base.empty:
                base = sort_trades_for_settlement_export(
                    base, st.session_state.get('opt_same_day_buy_first', True)
                )
                base['_global_seq'] = range(len(base))
                st.session_state.master_df = base
                st.success(f"분석 완료! ({len(base):,}건)")
            else:
                st.error("유효한 데이터가 없습니다.")

    st.divider()
    st.header("💾 백업 / 복원")

    if 'master_df' in st.session_state:
        col_j, col_e = st.columns(2)
        col_j.download_button(
            "JSON 백업", build_backup_json().encode('utf-8'),
            file_name="trades_backup.json", mime="application/json",
            use_container_width=True
        )
        col_e.download_button(
            "Excel 백업", build_backup_excel(),
            file_name="trades_backup.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(
            "백업 포함: JSON은 거래·수동·제외목록·당일매수우선. "
            "Excel은 위 + 잔고현황 + 시트「설정」(제외·옵션)."
        )

    st.caption("복원할 파일 선택 (JSON 또는 Excel)")
    restore_file = st.file_uploader("복원 파일", type=['json', 'xlsx'],
                                     key="restore_uploader", label_visibility="collapsed")
    if restore_file and st.button("복원 실행"):
        if restore_file.name.endswith('.json'):
            ok, msg = restore_from_json(restore_file)
        else:
            ok, msg = restore_from_excel(restore_file)
        (st.success if ok else st.error)(msg)
        if ok:
            st.rerun()


# ── 데이터 없으면 안내 ──
if 'master_df' not in st.session_state:
    st.info("사이드바에서 파일을 업로드한 후 분석 실행 버튼을 눌러주세요.")
    st.stop()

combined_raw = _build_combined()
exclude_txt = st.session_state.get('exclude_symbols_text', '') or ''
combined_df = apply_exclude_symbols(combined_raw, exclude_txt)
if exclude_txt.strip() and len(combined_df) < len(combined_raw):
    st.sidebar.caption(f"제외 적용: {len(combined_raw) - len(combined_df):,}건 제거 → {len(combined_df):,}건")
if combined_df.empty and not combined_raw.empty:
    st.warning("제외 목록 때문에 남은 거래가 없습니다. 제외 종목을 비우거나 줄여 주세요.")
    combined_df = combined_raw.copy()
positions_df = calculate_positions(combined_df)
st.session_state.positions_df = positions_df

# ── 상단 요약 ──
total_realized  = positions_df['실현손익'].sum()
holding_stocks  = positions_df[positions_df['잔고수량'] > 0]
total_hold_cost = holding_stocks['보유원가'].sum()
manual_cnt      = len(st.session_state.get('manual_trades', []))

c1, c2, c3, c4 = st.columns(4)
c1.metric("누적 실현손익",  f"₩{total_realized:,.0f}")
c2.metric("현재 보유 종목", f"{len(holding_stocks)}개")
c3.metric("보유 평가원가",  f"₩{total_hold_cost:,.0f}")
c4.metric("수동 입력 건수", f"{manual_cnt}건", delta="T+2 미반영 보정용")
st.divider()


# ════════════════════════════════════════
# 종목 상세 팝업 (공통)
# ════════════════════════════════════════
@st.dialog("종목 상세", width="large")
def show_stock_dialog(stock_name: str):
    sym_key = combined_df[combined_df["종목명"] == stock_name]["종목키"].iloc[0]
    detail_df = calculate_trade_detail(combined_df, sym_key)
    pos_row = positions_df[positions_df["종목키"] == sym_key]

    # ── 상단 요약 지표
    if not pos_row.empty:
        p = pos_row.iloc[0]
        pnl_rate = (p["실현손익"] / p["누적매수금액"] * 100) if p["누적매수금액"] else 0
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("실현손익", f"₩{p['실현손익']:,.0f}")
        c2.metric("손익률", f"{pnl_rate:+.2f}%")
        c3.metric("잔고수량", f"{p['잔고수량']:,}주")
        c4.metric("평균단가", f"₩{p['평균단가']:,.0f}")
        c5.metric("보유원가", f"₩{p['보유원가']:,.0f}")

    # ── 보유기간 / 계좌별 비중
    buy_sells = detail_df[detail_df["매매유형"].isin(["BUY","SELL"])]
    if not buy_sells.empty:
        first_buy = detail_df[detail_df["매매유형"] == "BUY"]["거래일자"].min()
        last_trade = detail_df["거래일자"].max()
        hold_days = (last_trade - first_buy).days if pd.notna(first_buy) else 0

        acc_dist = combined_df[combined_df["종목키"] == sym_key].groupby("계좌")["거래금액"].sum()
        acc_str = "  /  ".join([f"{a}: ₩{v:,.0f}" for a, v in acc_dist.items()])

        i1, i2 = st.columns(2)
        i1.info(f"📅 첫 매수: {str(first_buy)[:10]}  |  활동 기간: {hold_days}일")
        i2.info(f"🏦 계좌별 거래금액:  {acc_str}")

    st.divider()

    # ── 탭 구성
    dtab1, dtab2, dtab3 = st.tabs(["📈 매매 타임라인", "📋 거래 내역", "💰 실현손익 추이"])

    with dtab1:
        buy_df  = detail_df[detail_df["매매유형"] == "BUY"]
        sell_df = detail_df[detail_df["매매유형"] == "SELL"]
        fig = go.Figure()
        if not buy_df.empty:
            fig.add_trace(go.Scatter(
                x=buy_df["거래일자"], y=buy_df["거래단가"], mode="markers", name="매수",
                marker=dict(color="#e74c3c", symbol="triangle-up",
                            size=buy_df["거래수량"].apply(lambda x: max(8, min(28, x/10)))),
                hovertemplate="%{x}<br>매수 %{y:,.0f}원<extra></extra>"
            ))
        if not sell_df.empty:
            fig.add_trace(go.Scatter(
                x=sell_df["거래일자"], y=sell_df["거래단가"], mode="markers", name="매도",
                marker=dict(color="#2980b9", symbol="triangle-down",
                            size=sell_df["거래수량"].apply(lambda x: max(8, min(28, x/10)))),
                hovertemplate="%{x}<br>매도 %{y:,.0f}원<extra></extra>"
            ))
        if not detail_df.empty:
            fig.add_trace(go.Scatter(
                x=detail_df["거래일자"], y=detail_df["평균단가"],
                mode="lines", name="평균단가",
                line=dict(color="#f39c12", width=1.5, dash="dot"),
            ))
        fig.update_layout(template="plotly_dark", height=360,
                          xaxis_title="날짜", yaxis_title="단가 (원)",
                          legend=dict(orientation="h", y=1.08))
        st.plotly_chart(fig, use_container_width=True)

        # 계좌별 비중 파이
        acc_buy = combined_df[(combined_df["종목키"] == sym_key) & (combined_df["매매유형"] == "BUY")]
        if not acc_buy.empty:
            acc_amt = acc_buy.groupby("계좌")["거래금액"].sum().reset_index()
            fig_pie = go.Figure(go.Pie(
                labels=acc_amt["계좌"], values=acc_amt["거래금액"],
                hole=0.4, textinfo="label+percent",
            ))
            fig_pie.update_layout(template="plotly_dark", height=260,
                                  title="계좌별 매수 비중", showlegend=False)
            st.plotly_chart(fig_pie, use_container_width=True)

    with dtab2:
        show_cols = ["거래일자", "매매구분", "거래수량", "거래단가", "거래금액",
                     "평균단가", "잔고수량", "실현손익", "계좌"]
        show_cols = [c for c in show_cols if c in detail_df.columns]

        def _cpnl(v):
            try:
                f = float(v)
                if f > 0: return "color:#e74c3c;font-weight:bold"
                if f < 0: return "color:#2980b9;font-weight:bold"
            except: pass
            return ""
        def _ctype(v):
            s = str(v)
            if "매수" in s: return "color:#e74c3c"
            if "매도" in s: return "color:#2980b9"
            return ""

        st.dataframe(
            detail_df[show_cols].sort_values("거래일자", ascending=False)
            .style
            .map(_cpnl, subset=["실현손익"])
            .map(_ctype, subset=["매매구분"])
            .format({"거래수량": "{:,.0f}", "거래단가": "{:,.0f}", "거래금액": "{:,.0f}",
                     "평균단가": "{:,.0f}", "잔고수량": "{:,.0f}", "실현손익": "{:,.0f}"}),
            use_container_width=True, height=380
        )

    with dtab3:
        pnl_df = detail_df[detail_df["실현손익"] != 0].copy()
        if pnl_df.empty:
            st.info("실현손익 데이터가 없습니다.")
        else:
            pnl_df["누적손익"] = pnl_df["실현손익"].cumsum()
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=pnl_df["거래일자"], y=pnl_df["실현손익"], name="매도별 실현손익",
                marker_color=pnl_df["실현손익"].apply(lambda v: "#e74c3c" if v > 0 else "#2980b9")
            ))
            fig2.add_trace(go.Scatter(
                x=pnl_df["거래일자"], y=pnl_df["누적손익"],
                mode="lines+markers", name="누적손익",
                line=dict(color="#f1c40f", width=2), yaxis="y2"
            ))
            fig2.update_layout(
                yaxis=dict(title="매도별 손익"),
                yaxis2=dict(title="누적손익", overlaying="y", side="right"),
                template="plotly_dark", height=360,
                legend=dict(orientation="h", y=1.08),
            )
            st.plotly_chart(fig2, use_container_width=True)


def stock_selector_popup(label: str, stocks: list, key: str):
    """종목 선택 → 팝업 트리거 공통 위젯."""
    if not stocks:
        return
    cols = st.columns([3, 1])
    sel = cols[0].selectbox(label, ["-- 종목 선택 --"] + stocks, key=key)
    if cols[1].button("🔍 상세보기", key=key + "_btn", use_container_width=True):
        if sel != "-- 종목 선택 --":
            show_stock_dialog(sel)
        else:
            st.toast("종목을 먼저 선택해주세요.")

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13 = st.tabs([
    "🗂 현재 잔고",
    "✏️ 수동 거래 입력",
    "🎯 종목별 상세",
    "📋 전체 거래내역",
    "📅 기간별 손익",
    "🏆 손익률 랭킹",
    "🏦 계좌별 손익",
    "📈 수익곡선",
    "🔄 매매 패턴",
    "⚠️ 리스크",
    "🎯 목표 시뮬레이터",
    "📊 벤치마크",
    "🤖 매매 시나리오",
])


# ════════════════════════════════════════
# Tab 1: 현재 잔고
# ════════════════════════════════════════
with tab1:
    st.subheader("현재 보유 잔고 (이동평균단가법)")
    st.caption("✅ 증권사 표준 방식 · 수동 입력 거래 포함")

    all_accounts = ['전체'] + sorted(combined_df['계좌'].dropna().astype(str).unique())
    sel_account  = st.selectbox("계좌 필터", all_accounts, key="tab1_account")

    if sel_account != '전체':
        disp_pos = calculate_positions(combined_df[combined_df['계좌'] == sel_account])
    else:
        disp_pos = positions_df.copy()

    show_all = st.checkbox("매도 완료 종목도 표시", value=False)
    if not show_all:
        disp_pos = disp_pos[disp_pos['잔고수량'] > 0].copy()

    # ── 현재가 조회 버튼 ──
    _col_btn, _col_msg = st.columns([2, 8])
    if _col_btn.button("📡 현재가 조회 (실시간)", type="primary", use_container_width=True):
        with st.spinner("현재가를 가져오는 중..."):
            try:
                import yfinance as _yf_chk  # noqa
                _holding = disp_pos[disp_pos['잔고수량'] > 0].copy()
                if '종목코드6' in combined_df.columns:
                    _code_src = combined_df.drop_duplicates('종목키')[['종목키', '종목코드6']]
                    _holding  = _holding.merge(_code_src, on='종목키', how='left')
                ticker_map = resolve_tickers_yf(_holding)
                if not ticker_map:
                    _col_msg.warning("⚠️ 조회 가능한 종목이 없습니다. 거래내역에 종목코드6 컬럼이 있는지 확인하세요.")
                else:
                    fetch_current_prices_yf.clear()
                    prices     = fetch_current_prices_yf(tuple(set(ticker_map.values())))
                    key_to_price = {k: prices[v] for k, v in ticker_map.items() if v in prices}
                    st.session_state['live_prices']      = key_to_price
                    st.session_state['live_prices_time'] = datetime.now().strftime("%H:%M:%S")
                    hit  = len(key_to_price)
                    miss = len(_holding) - hit
                    msg  = f"✅ {hit}/{len(_holding)}개 종목 조회 완료 — {st.session_state['live_prices_time']} 기준"
                    if miss:
                        msg += f"  (미조회 {miss}개: 종목코드6 없거나 상폐 등)"
                    _col_msg.success(msg)
            except ImportError as e:
                _col_msg.error(f"❌ yfinance import 실패: {e}")
            except Exception as e:
                _col_msg.error(f"❌ 조회 실패: {e}")
                st.exception(e)
    elif 'live_prices_time' in st.session_state:
        _col_msg.caption(f"마지막 조회: {st.session_state['live_prices_time']} (재조회하려면 버튼을 누르세요)")

    # ── 테이블 구성 ──
    _live = st.session_state.get('live_prices', {})
    disp_pos = disp_pos.copy()

    if _live:
        disp_pos['현재가']     = disp_pos['종목키'].map(_live)
        disp_pos['미실현손익'] = disp_pos.apply(
            lambda r: int((r['현재가'] - r['평균단가']) * r['잔고수량'])
                      if pd.notna(r.get('현재가')) and r['평균단가'] > 0 and r['잔고수량'] > 0 else None, axis=1)
        disp_pos['수익률(%)']  = disp_pos.apply(
            lambda r: round((r['현재가'] - r['평균단가']) / r['평균단가'] * 100, 2)
                      if pd.notna(r.get('현재가')) and r['평균단가'] > 0 and r['잔고수량'] > 0 else None, axis=1)
        disp_pos['평가금액']   = disp_pos.apply(
            lambda r: int(r['현재가'] * r['잔고수량'])
                      if pd.notna(r.get('현재가')) and r['잔고수량'] > 0 else None, axis=1)

        # 최초매수일 / 보유일수 / 매수횟수 계산
        _buy_df = combined_df[combined_df['매매유형'] == 'BUY'].copy()
        _buy_stats = _buy_df.groupby('종목키').agg(
            최초매수일=('거래일자', 'min'),
            매수횟수=('거래일자', 'count'),
        ).reset_index()
        _today = pd.Timestamp(datetime.today().date())
        _buy_stats['보유일수'] = (_today - _buy_stats['최초매수일']).dt.days
        _buy_stats['최초매수일'] = _buy_stats['최초매수일'].dt.strftime('%Y-%m-%d')
        disp_pos = disp_pos.merge(_buy_stats[['종목키','최초매수일','보유일수','매수횟수']], on='종목키', how='left')

        # 요약 메트릭
        _hp = disp_pos[(disp_pos['잔고수량'] > 0) & disp_pos['미실현손익'].notna()]
        if not _hp.empty:
            _unr  = _hp['미실현손익'].sum()
            _cost = _hp['보유원가'].sum()
            _m1, _m2, _m3 = st.columns(3)
            _m1.metric("미실현 평가손익", f"₩{_unr:,.0f}",
                       delta=f"{_unr/_cost*100:+.2f}%" if _cost else None,
                       delta_color="normal" if _unr >= 0 else "inverse")
            _m2.metric("총 평가금액",  f"₩{_hp['평가금액'].sum():,.0f}")
            _m3.metric("총 보유원가",  f"₩{_cost:,.0f}")

        base_cols = ['종목명', '잔고수량', '평균단가', '현재가', '수익률(%)',
                     '미실현손익', '평가금액', '보유원가', '실현손익',
                     '최초매수일', '보유일수', '매수횟수']
        base_cols = [c for c in base_cols if c in disp_pos.columns]
        fmt = {
            '잔고수량': '{:,.0f}', '평균단가': '{:,.0f}', '현재가': '{:,.0f}',
            '보유원가': '{:,.0f}', '실현손익': '{:,.0f}',
            '미실현손익': '{:,.0f}', '평가금액': '{:,.0f}', '수익률(%)': '{:+.2f}',
            '보유일수': '{:,.0f}', '매수횟수': '{:,.0f}',
        }

        def _color_pnl_cell(val):
            try:
                v = float(val)
                if v > 0: return 'color:#ff6b6b;font-weight:bold'
                if v < 0: return 'color:#74b9ff;font-weight:bold'
            except Exception:
                pass
            return ''

        def highlight_holding(row):
            if row.get('잔고수량', 0) <= 0:
                return [''] * len(row)
            unr = row.get('미실현손익', None)
            bg  = '#1a3a5c' if (unr is None or pd.isna(unr) or float(unr) >= 0) else '#3a1a1a'
            return [f'background-color:{bg};color:white'] * len(row)

        st.dataframe(
            disp_pos[base_cols].sort_values('잔고수량', ascending=False)
            .style.apply(highlight_holding, axis=1)
            .map(_color_pnl_cell, subset=['미실현손익', '수익률(%)'])
            .format(fmt, na_rep='-'),
            use_container_width=True, height=450
        )

    else:
        base_cols = ['종목명', '잔고수량', '평균단가', '보유원가', '실현손익', '누적매수수량', '누적매도수량']
        fmt = {
            '잔고수량': '{:,.0f}', '평균단가': '{:,.0f}', '보유원가': '{:,.0f}',
            '실현손익': '{:,.0f}', '누적매수수량': '{:,.0f}', '누적매도수량': '{:,.0f}',
        }

        def highlight_holding(row):
            return ['background-color:#1a3a5c; color:white'] * len(row) \
                if row.get('잔고수량', 0) > 0 else [''] * len(row)

        st.dataframe(
            disp_pos[base_cols].sort_values('실현손익', ascending=False)
            .style.apply(highlight_holding, axis=1).format(fmt),
            use_container_width=True, height=450
        )
    csv_bytes = disp_pos[base_cols].to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button("📥 잔고 CSV 다운로드", csv_bytes, "잔고현황.csv", "text/csv")
    st.divider()

    # ── 종목 클릭 → 인라인 상세 ──────────────────────────
    _stock_names = sorted(disp_pos['종목명'].dropna().unique().tolist())
    _sel_inline  = st.selectbox("🔍 종목 선택 → 전체 거래내역 + 로트별 손익",
                                ["-- 종목 선택 --"] + _stock_names,
                                key="tab1_inline_stock")

    if _sel_inline and _sel_inline != "-- 종목 선택 --":
        _sym_key   = combined_df[combined_df['종목명'] == _sel_inline]['종목키'].iloc[0]
        _det_df    = calculate_trade_detail(combined_df, _sym_key)
        _pos_inline = positions_df[positions_df['종목키'] == _sym_key]
        _lp_inline  = st.session_state.get('live_prices', {}).get(_sym_key)
        _ltime_inline = st.session_state.get('live_prices_time', '')

        with st.container(border=True):
            # ── 상단 요약 메트릭 ──
            if not _pos_inline.empty:
                _pi   = _pos_inline.iloc[0]
                _avg_i = float(_pi['평균단가'])
                _qty_i = float(_pi['잔고수량'])

                if _lp_inline and _qty_i > 0:
                    _unr_i   = int((_lp_inline - _avg_i) * _qty_i)
                    _ret_i   = (_lp_inline - _avg_i) / _avg_i * 100 if _avg_i > 0 else 0
                    _fb_i    = _det_df[_det_df['매매유형'] == 'BUY']['거래일자'].min()
                    _hdays_i = (pd.Timestamp(datetime.today().date()) - _fb_i).days if pd.notna(_fb_i) else 0
                    st.caption(f"**{_sel_inline}** · 현재가 기준 {_ltime_inline}")
                    _mc1, _mc2, _mc3, _mc4, _mc5, _mc6, _mc7 = st.columns(7)
                    _mc1.metric("실현손익",   f"₩{_pi['실현손익']:,.0f}")
                    _mc2.metric("잔고수량",   f"{_qty_i:,.0f}주")
                    _mc3.metric("평균단가",   f"₩{_avg_i:,.0f}")
                    _mc4.metric("현재가",     f"₩{_lp_inline:,.0f}",
                                delta=f"{_ret_i:+.2f}%",
                                delta_color="normal" if _unr_i >= 0 else "inverse")
                    _mc5.metric("미실현손익", f"₩{_unr_i:,.0f}",
                                delta=f"{_ret_i:+.2f}%",
                                delta_color="normal" if _unr_i >= 0 else "inverse")
                    _mc6.metric("평가금액",   f"₩{int(_lp_inline * _qty_i):,.0f}")
                    _mc7.metric("보유일수",   f"{_hdays_i:,}일",
                                delta=f"최초 {str(_fb_i)[:10]}", delta_color="off")
                else:
                    if _qty_i > 0:
                        st.caption(f"**{_sel_inline}** · 현재가 조회 전 (위 '📡 현재가 조회' 버튼 누르면 손익 표시)")
                    _mc1, _mc2, _mc3, _mc4, _mc5 = st.columns(5)
                    _mc1.metric("실현손익", f"₩{_pi['실현손익']:,.0f}")
                    _mc2.metric("잔고수량", f"{_qty_i:,.0f}주")
                    _mc3.metric("평균단가", f"₩{_avg_i:,.0f}")
                    _mc4.metric("누적매수", f"₩{_pi['누적매수금액']:,.0f}")
                    _mc5.metric("누적매도", f"₩{_pi['누적매도금액']:,.0f}")

            st.divider()

            # ── 전체 거래내역 테이블 ──
            _itab1, _itab2 = st.tabs(["📋 전체 거래내역", "📦 매수 로트별 손익"])

            with _itab1:
                _sc = ['거래일자','매매구분','거래수량','거래단가','거래금액',
                       '평균단가','잔고수량','실현손익','계좌']
                _sc = [c for c in _sc if c in _det_df.columns]

                def _di_cpnl(v):
                    try:
                        f = float(v)
                        if f > 0: return 'color:#e74c3c;font-weight:bold'
                        if f < 0: return 'color:#2980b9;font-weight:bold'
                    except: pass
                    return ''
                def _di_ctype(v):
                    s = str(v)
                    if '매수' in s: return 'color:#e74c3c'
                    if '매도' in s: return 'color:#2980b9'
                    return ''

                st.dataframe(
                    _det_df[_sc].sort_values('거래일자', ascending=False)
                    .style
                    .map(_di_cpnl,  subset=['실현손익'])
                    .map(_di_ctype, subset=['매매구분'])
                    .format({'거래수량':'{:,.0f}','거래단가':'{:,.0f}','거래금액':'{:,.0f}',
                             '평균단가':'{:,.0f}','잔고수량':'{:,.0f}','실현손익':'{:,.0f}'}),
                    use_container_width=True, height=380
                )

            with _itab2:
                render_lot_tabs(combined_df, _sym_key, _lp_inline, _pos_inline)


# ════════════════════════════════════════
# Tab 2: 수동 거래 입력
# ════════════════════════════════════════
with tab2:
    st.subheader("✏️ 수동 거래 입력")
    st.caption("💡 T+2 결제 미반영, 당일 체결 등 파일에 아직 없는 거래를 직접 추가합니다.")

    with st.form("manual_trade_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        col4, col5, col6 = st.columns(3)

        input_date  = col1.date_input("거래일자", value=date.today())
        input_type  = col2.selectbox("매매구분", ["매수", "매도"])
        input_name  = col3.text_input("종목명", placeholder="예) 펄어비스")
        input_qty   = col4.number_input("수량 (주)", min_value=1, value=1, step=1)
        input_price = col5.number_input("단가 (원)", min_value=1, value=10000, step=100)
        input_fee   = col6.number_input("수수료 (원)", min_value=0, value=0, step=10)

        existing_accounts = sorted(combined_df['계좌'].dropna().astype(str).unique().tolist())
        account_options   = existing_accounts + ['직접입력']
        sel_acc_opt = st.selectbox("계좌", account_options, key="manual_acc_select")
        if sel_acc_opt == '직접입력':
            input_account = st.text_input("계좌명 직접 입력", value="수동입력")
        else:
            input_account = sel_acc_opt

        submitted = st.form_submit_button("➕ 거래 추가", type="primary", use_container_width=True)

    if submitted:
        if not input_name.strip():
            st.error("종목명을 입력해주세요.")
        else:
            st.session_state.manual_trades.append({
                '날짜':    str(input_date),
                '매매유형': input_type,
                '종목명':  input_name.strip(),
                '수량':    int(input_qty),
                '단가':    int(input_price),
                '수수료':  int(input_fee),
                '계좌':    input_account,
            })
            st.success(f"✅ {input_date} {input_type} {input_name.strip()} "
                       f"{int(input_qty):,}주 @{int(input_price):,}원 추가됨")
            st.rerun()

    st.divider()
    trades = st.session_state.manual_trades
    if trades:
        st.write(f"**수동 입력 거래 목록** ({len(trades)}건)")
        for idx, t in enumerate(trades):
            col_info, col_del = st.columns([11, 1])
            icon = "🔴" if t['매매유형'] == '매수' else "🔵"
            col_info.markdown(
                f"`{t['날짜']}` &nbsp; {icon} **{t['매매유형']}** &nbsp; "
                f"**{t['종목명']}** &nbsp; {int(t['수량']):,}주 @{int(t['단가']):,}원 &nbsp; "
                f"수수료 {int(t.get('수수료', 0)):,}원 &nbsp; _(계좌: {t['계좌']})_"
            )
            if col_del.button("🗑", key=f"del_{idx}", help="삭제"):
                st.session_state.manual_trades.pop(idx)
                st.rerun()

        st.divider()
        if st.button("🗑️ 전체 수동 거래 삭제", type="secondary"):
            st.session_state.manual_trades = []
            st.rerun()
    else:
        st.info("아직 수동 입력된 거래가 없습니다.")


# ════════════════════════════════════════
# Tab 3: 종목별 상세
# ════════════════════════════════════════
with tab3:
    st.subheader("종목별 상세 매매 현황")

    all_stocks     = sorted(combined_df['종목명'].dropna().unique())
    selected_stock = st.selectbox("종목 선택", all_stocks, key="detail_stock")

    if selected_stock:
        symbol_key = combined_df[combined_df['종목명'] == selected_stock]['종목키'].iloc[0]
        detail_df  = calculate_trade_detail(combined_df, symbol_key)

        pos_row = positions_df[positions_df['종목키'] == symbol_key]
        if not pos_row.empty:
            p = pos_row.iloc[0]
            _lp   = st.session_state.get('live_prices', {}).get(symbol_key)
            _ltime = st.session_state.get('live_prices_time', '')
            _avg  = float(p['평균단가'])
            _qty  = float(p['잔고수량'])

            if _lp and _qty > 0:
                _unr     = int((_lp - _avg) * _qty)
                _ret_pct = (_lp - _avg) / _avg * 100 if _avg > 0 else 0
                # 최초매수일 계산
                _fb = detail_df[detail_df['매매유형']=='BUY']['거래일자'].min()
                _hold_days = (pd.Timestamp(datetime.today().date()) - _fb).days if pd.notna(_fb) else 0

                st.caption(f"현재가 기준 — {_ltime}  (tab1 '📡 현재가 조회' 버튼으로 갱신)")
                c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
                c1.metric("실현손익",   f"₩{p['실현손익']:,.0f}")
                c2.metric("잔고수량",   f"{_qty:,.0f}주")
                c3.metric("평균단가",   f"₩{_avg:,.0f}")
                c4.metric("현재가",     f"₩{_lp:,.0f}",
                          delta=f"{_ret_pct:+.2f}%",
                          delta_color="normal" if _unr >= 0 else "inverse")
                c5.metric("미실현손익", f"₩{_unr:,.0f}",
                          delta=f"{_ret_pct:+.2f}%",
                          delta_color="normal" if _unr >= 0 else "inverse")
                c6.metric("평가금액",   f"₩{int(_lp*_qty):,.0f}")
                c7.metric("보유일수",   f"{_hold_days:,}일",
                          delta=f"최초 {str(_fb)[:10]}", delta_color="off")
            else:
                if _qty > 0 and not _lp:
                    st.caption("💡 tab1의 '📡 현재가 조회' 버튼을 누르면 현재가 기준 손익이 표시됩니다.")
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("실현손익",   f"₩{p['실현손익']:,.0f}")
                c2.metric("잔고수량",   f"{_qty:,.0f}주")
                c3.metric("평균단가",   f"₩{_avg:,.0f}")
                c4.metric("누적매수",   f"₩{p['누적매수금액']:,.0f}")
                c5.metric("누적매도",   f"₩{p['누적매도금액']:,.0f}")

        # ── 전체 거래내역 테이블 ──────────────────────────────────
        show_cols = ['거래일자', '매매구분', '거래수량', '거래단가', '거래금액',
                     '평균단가', '잔고수량', '실현손익', '계좌', '수동입력']
        show_cols = [c for c in show_cols if c in detail_df.columns]

        def color_pnl(val):
            try:
                v = float(val)
                if v > 0: return 'color:#e74c3c;font-weight:bold'
                if v < 0: return 'color:#2980b9;font-weight:bold'
            except Exception:
                pass
            return ''

        def color_type(val):
            s = str(val)
            if '매수' in s: return 'color:#e74c3c'
            if '매도' in s: return 'color:#2980b9'
            return ''

        def highlight_manual(row):
            if row.get('수동입력', False):
                return ['background-color:#2d2d00'] * len(row)
            return [''] * len(row)

        st.dataframe(
            detail_df[show_cols].sort_values('거래일자', ascending=False)
            .style
            .apply(highlight_manual, axis=1)
            .map(color_pnl,  subset=['실현손익'])
            .map(color_type, subset=['매매구분'])
            .format({'거래수량': '{:,.0f}', '거래단가': '{:,.0f}', '거래금액': '{:,.0f}',
                     '평균단가': '{:,.0f}', '잔고수량': '{:,.0f}', '실현손익': '{:,.0f}'}),
            use_container_width=True, height=400
        )
        st.caption("🟡 어두운 노란 배경 = 수동 입력 거래")

        # ── 매수 로트별 현재 손익 (FIFO 잔여수량 기준) ──────────────
        _lp_for_table = st.session_state.get('live_prices', {}).get(symbol_key)
        _qty_held = float(pos_row.iloc[0]['잔고수량']) if not pos_row.empty else 0

        if _qty_held > 0:
            st.divider()
            st.markdown(f"**📦 매수 로트별 현재 손익** — FIFO 잔여수량 기준"
                        + (f" · 현재가 {_lp_for_table:,}원" if _lp_for_table else ""))
            render_lot_tabs(combined_df, symbol_key, _lp_for_table, pos_row)

        buy_df  = detail_df[detail_df['매매유형'] == 'BUY']
        sell_df = detail_df[detail_df['매매유형'] == 'SELL']

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=buy_df['거래일자'], y=buy_df['거래단가'], mode='markers', name='매수',
            marker=dict(color='#e74c3c', symbol='triangle-up',
                        size=buy_df['거래수량'].apply(lambda x: max(6, min(30, x / 10)))),
            hovertemplate='%{x}<br>매수 %{y:,.0f}원<extra></extra>'
        ))
        fig.add_trace(go.Scatter(
            x=sell_df['거래일자'], y=sell_df['거래단가'], mode='markers', name='매도',
            marker=dict(color='#2980b9', symbol='triangle-down',
                        size=sell_df['거래수량'].apply(lambda x: max(6, min(30, x / 10)))),
            hovertemplate='%{x}<br>매도 %{y:,.0f}원<extra></extra>'
        ))
        if not detail_df.empty:
            fig.add_trace(go.Scatter(
                x=detail_df['거래일자'], y=detail_df['평균단가'],
                mode='lines', name='평균단가(이동평균)',
                line=dict(color='#f39c12', width=1.5, dash='dot'),
                hovertemplate='%{x}<br>평균단가 %{y:,.0f}원<extra></extra>'
            ))

        # 현재가 수평선 추가
        _lp_chart = st.session_state.get('live_prices', {}).get(symbol_key)
        _avg_now  = float(pos_row.iloc[0]['평균단가']) if not pos_row.empty else 0
        if _lp_chart and not detail_df.empty:
            fig.add_hline(
                y=_lp_chart,
                line=dict(color='#2ecc71', width=2, dash='dash'),
                annotation_text=f"현재가 {_lp_chart:,}원",
                annotation_position="top right",
                annotation_font=dict(color='#2ecc71', size=12),
            )
            if _avg_now > 0:
                _unr_pct = (_lp_chart - _avg_now) / _avg_now * 100
                fig.add_hline(
                    y=_avg_now,
                    line=dict(color='#f39c12', width=1.5, dash='dashdot'),
                    annotation_text=f"현재 평균단가 {_avg_now:,.0f}원 ({_unr_pct:+.1f}%)",
                    annotation_position="bottom right",
                    annotation_font=dict(color='#f39c12', size=11),
                )
        fig.update_layout(title=f"{selected_stock} 매매 타임라인",
                          xaxis_title='날짜', yaxis_title='단가 (원)',
                          template='plotly_dark', height=400)
        st.plotly_chart(fig, use_container_width=True)

        pnl_df = detail_df[detail_df['실현손익'] != 0].copy()
        if not pnl_df.empty:
            pnl_df['누적손익'] = pnl_df['실현손익'].cumsum()
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=pnl_df['거래일자'], y=pnl_df['실현손익'], name='매도별 실현손익',
                marker_color=pnl_df['실현손익'].apply(lambda v: '#e74c3c' if v > 0 else '#2980b9')
            ))
            fig2.add_trace(go.Scatter(
                x=pnl_df['거래일자'], y=pnl_df['누적손익'],
                mode='lines+markers', name='누적손익',
                line=dict(color='#f1c40f', width=2), yaxis='y2'
            ))
            fig2.update_layout(
                title=f"{selected_stock} 실현손익 추이",
                yaxis=dict(title='매도별 손익'),
                yaxis2=dict(title='누적손익', overlaying='y', side='right'),
                template='plotly_dark', height=350
            )
            st.plotly_chart(fig2, use_container_width=True)


# ════════════════════════════════════════
# Tab 4: 전체 거래내역
# ════════════════════════════════════════
with tab4:
    st.subheader("전체 거래내역")

    col_a, col_b, col_c, col_d = st.columns(4)
    accounts = ['전체'] + sorted(combined_df['계좌'].dropna().unique())
    stocks   = ['전체'] + sorted(combined_df['종목명'].dropna().unique())
    types    = ['전체', '매수', '매도']
    sources  = ['전체', '파일', '수동입력']

    sel_acc = col_a.selectbox("계좌",     accounts, key="raw_acc")
    sel_stk = col_b.selectbox("종목",     stocks,   key="raw_stk")
    sel_typ = col_c.selectbox("매매유형", types,    key="raw_typ")
    sel_src = col_d.selectbox("구분",     sources,  key="raw_src")

    view = combined_df.copy()
    if sel_acc != '전체': view = view[view['계좌'] == sel_acc]
    if sel_stk != '전체': view = view[view['종목명'] == sel_stk]
    if sel_typ != '전체':
        view = view[view['매매유형'] == {'매수': 'BUY', '매도': 'SELL'}[sel_typ]]
    if sel_src == '파일':
        view = view[view['수동입력'] == False]
    elif sel_src == '수동입력':
        view = view[view['수동입력'] == True]

    show_raw = ['거래일자', '매매구분', '종목명', '거래수량', '거래단가', '거래금액', '수수료', '계좌', '수동입력']
    show_raw = [c for c in show_raw if c in view.columns]

    st.dataframe(
        view[show_raw].sort_values('거래일자', ascending=False)
        .style.format({'거래수량': '{:,.0f}', '거래단가': '{:,.0f}',
                       '거래금액': '{:,.0f}', '수수료': '{:,.0f}'}),
        use_container_width=True, height=500
    )
    csv2 = view[show_raw].to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    st.download_button("📥 거래내역 CSV", csv2, "거래내역.csv", "text/csv")


# ════════════════════════════════════════
# Tab 5: 기간별 손익표
# ════════════════════════════════════════
with tab5:
    st.subheader("📅 기간별 실현손익")
    st.caption("매도 체결일 기준으로 실현된 손익을 일/주/월/연 단위로 집계합니다.")

    # 매도 거래 + 실현손익 계산
    @st.cache_data
    def build_pnl_timeseries(_df):
        """전체 거래에서 종목별로 매도 실현손익을 날짜 단위로 집계."""
        if _df.empty:
            return pd.DataFrame()
        rows = []
        for sym_key in _df['종목키'].unique():
            detail = calculate_trade_detail(_df, sym_key)
            sells = detail[detail['매매유형'] == 'SELL'][['거래일자', '실현손익', '계좌', '종목키', '종목명']].copy()
            rows.append(sells)
        if not rows:
            return pd.DataFrame()
        return pd.concat(rows, ignore_index=True)

    pnl_ts = build_pnl_timeseries(combined_df)

    period_opt = st.radio("집계 단위", ["일별", "주별", "월별", "연별"], horizontal=True, key="period_opt")
    period_map = {"일별": "D", "주별": "W", "월별": "ME", "연별": "YE"}
    freq = period_map[period_opt]
    freq_label_map = {"일별": "%Y-%m-%d", "주별": "%Y-W%W", "월별": "%Y-%m", "연별": "%Y"}

    if pnl_ts.empty:
        st.info("매도 거래 데이터가 없습니다.")
    else:
        pnl_ts['거래일자'] = pd.to_datetime(pnl_ts['거래일자'])
        grouped = pnl_ts.groupby(pd.Grouper(key='거래일자', freq=freq))['실현손익'].sum().reset_index()
        grouped = grouped[grouped['실현손익'] != 0].copy()
        grouped['기간'] = grouped['거래일자'].dt.strftime(freq_label_map[period_opt])
        grouped['누적손익'] = grouped['실현손익'].cumsum()
        grouped['수익'] = grouped['실현손익'].apply(lambda x: x if x > 0 else 0)
        grouped['손실'] = grouped['실현손익'].apply(lambda x: x if x < 0 else 0)

        # 차트
        fig_p = go.Figure()
        fig_p.add_trace(go.Bar(
            x=grouped['기간'], y=grouped['수익'], name='수익',
            marker_color='#e74c3c', opacity=0.85
        ))
        fig_p.add_trace(go.Bar(
            x=grouped['기간'], y=grouped['손실'], name='손실',
            marker_color='#2980b9', opacity=0.85
        ))
        fig_p.add_trace(go.Scatter(
            x=grouped['기간'], y=grouped['누적손익'],
            mode='lines+markers', name='누적손익',
            line=dict(color='#f1c40f', width=2), yaxis='y2'
        ))
        fig_p.update_layout(
            barmode='relative',
            yaxis=dict(title='실현손익 (원)'),
            yaxis2=dict(title='누적손익', overlaying='y', side='right', showgrid=False),
            template='plotly_dark', height=400,
            legend=dict(orientation='h', y=1.08),
        )
        st.plotly_chart(fig_p, use_container_width=True)

        # 요약 지표
        mc1, mc2, mc3, mc4 = st.columns(4)
        win_periods = (grouped['실현손익'] > 0).sum()
        lose_periods = (grouped['실현손익'] < 0).sum()
        mc1.metric("기간 합계", f"₩{grouped['실현손익'].sum():,.0f}")
        mc2.metric("수익 기간", f"{win_periods}회")
        mc3.metric("손실 기간", f"{lose_periods}회")
        mc4.metric("최대 수익 기간", f"₩{grouped['실현손익'].max():,.0f}")

        # 표
        display_table = grouped[['기간', '실현손익', '누적손익']].sort_values('기간', ascending=False).copy()
        def color_pnl_period(val):
            try:
                v = float(val)
                if v > 0: return 'color:#e74c3c;font-weight:bold'
                if v < 0: return 'color:#2980b9;font-weight:bold'
            except Exception:
                pass
            return ''

        st.dataframe(
            display_table.style
            .map(color_pnl_period, subset=['실현손익', '누적손익'])
            .format({'실현손익': '{:,.0f}', '누적손익': '{:,.0f}'}),
            use_container_width=True, height=350
        )
        csv_p = display_table.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button("📥 기간별 손익 CSV", csv_p, f"기간별손익_{period_opt}.csv", "text/csv")
        st.divider()
        stock_selector_popup("종목 상세 팝업", sorted(combined_df['종목명'].dropna().unique().tolist()), key="popup_tab5")


# ════════════════════════════════════════
# Tab 6: 손익률 종목 랭킹
# ════════════════════════════════════════
with tab6:
    st.subheader("🏆 종목별 손익률 랭킹")
    st.caption("실현손익 ÷ 누적매수금액으로 손익률을 계산합니다. 매도 이력이 없는 종목은 제외.")

    if positions_df.empty:
        st.info("데이터가 없습니다.")
    else:
        rank_df = positions_df.copy()
        # 손익률 = 실현손익 / 누적매수금액
        rank_df = rank_df[rank_df['누적매도금액'] > 0].copy()
        rank_df['손익률(%)'] = (rank_df['실현손익'] / rank_df['누적매수금액'] * 100).round(2)
        rank_df = rank_df.sort_values('손익률(%)', ascending=False).reset_index(drop=True)
        rank_df.index += 1  # 1위부터 표시

        col_r1, col_r2 = st.columns([1, 2])
        with col_r1:
            show_mode = st.radio("표시 기준", ["손익률 순", "실현손익 순", "수익 종목만", "손실 종목만"], key="rank_mode")

        rank_view = rank_df.copy()
        if show_mode == "실현손익 순":
            rank_view = rank_view.sort_values('실현손익', ascending=False).reset_index(drop=True)
            rank_view.index += 1
        elif show_mode == "수익 종목만":
            rank_view = rank_view[rank_view['실현손익'] > 0]
        elif show_mode == "손실 종목만":
            rank_view = rank_view[rank_view['실현손익'] < 0].sort_values('실현손익')
            rank_view.index = range(1, len(rank_view) + 1)

        # 지표
        win_stocks  = (rank_df['실현손익'] > 0).sum()
        lose_stocks = (rank_df['실현손익'] < 0).sum()
        total_stocks = len(rank_df)
        rc1, rc2, rc3, rc4 = st.columns(4)
        rc1.metric("분석 종목 수", f"{total_stocks}개")
        rc2.metric("수익 종목", f"{win_stocks}개", delta=f"승률 {win_stocks/total_stocks*100:.0f}%" if total_stocks else "")
        rc3.metric("손실 종목", f"{lose_stocks}개")
        rc4.metric("최고 손익률", f"{rank_df['손익률(%)'].max():.1f}%" if not rank_df.empty else "-")

        # 수평 바 차트
        top_n = min(30, len(rank_view))
        chart_df = rank_view.head(top_n) if show_mode != "손실 종목만" else rank_view.tail(top_n)

        fig_r = go.Figure()
        colors = ['#e74c3c' if v >= 0 else '#2980b9' for v in chart_df['손익률(%)']]
        fig_r.add_trace(go.Bar(
            y=chart_df['종목명'],
            x=chart_df['손익률(%)'],
            orientation='h',
            marker_color=colors,
            text=chart_df['손익률(%)'].apply(lambda x: f"{x:+.1f}%"),
            textposition='outside',
        ))
        fig_r.update_layout(
            xaxis_title='손익률 (%)',
            template='plotly_dark',
            height=max(350, len(chart_df) * 28),
            margin=dict(l=120, r=60),
        )
        st.plotly_chart(fig_r, use_container_width=True)

        # 표
        rank_cols = ['종목명', '실현손익', '손익률(%)', '누적매수금액', '누적매도금액', '잔고수량', '평균단가']
        rank_cols = [c for c in rank_cols if c in rank_view.columns]

        def color_rank_pnl(val):
            try:
                v = float(val)
                if v > 0: return 'color:#e74c3c;font-weight:bold'
                if v < 0: return 'color:#2980b9;font-weight:bold'
            except Exception:
                pass
            return ''

        st.dataframe(
            rank_view[rank_cols]
            .style
            .map(color_rank_pnl, subset=['실현손익', '손익률(%)'])
            .format({
                '실현손익': '{:,.0f}', '손익률(%)': '{:+.2f}',
                '누적매수금액': '{:,.0f}', '누적매도금액': '{:,.0f}',
                '잔고수량': '{:,.0f}', '평균단가': '{:,.0f}',
            }),
            use_container_width=True, height=400
        )
        csv_r = rank_view[rank_cols].to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button("📥 손익률 랭킹 CSV", csv_r, "손익률랭킹.csv", "text/csv")
        st.divider()
        stock_selector_popup("종목 상세 팝업", sorted(rank_view['종목명'].dropna().unique().tolist()), key="popup_tab6")


# ════════════════════════════════════════
# Tab 7: 계좌별 손익표
# ════════════════════════════════════════
with tab7:
    st.subheader("🏦 계좌별 손익 현황")
    st.caption("계좌별로 실현손익, 잔고, 매수/매도 총액을 집계합니다.")

    accounts_list = combined_df['계좌'].dropna().astype(str).unique().tolist()

    if not accounts_list:
        st.info("데이터가 없습니다.")
    else:
        # 계좌별 positions 계산
        acc_rows = []
        for acc in sorted(accounts_list):
            acc_df = combined_df[combined_df['계좌'] == acc]
            pos = calculate_positions(acc_df)
            if pos.empty:
                continue
            acc_rows.append({
                '계좌': acc,
                '종목수': len(pos),
                '보유종목수': (pos['잔고수량'] > 0).sum(),
                '실현손익': pos['실현손익'].sum(),
                '누적매수금액': pos['누적매수금액'].sum(),
                '누적매도금액': pos['누적매도금액'].sum(),
                '보유원가': pos['보유원가'].sum(),
                '총수수료': pos['총수수료'].sum(),
            })

        acc_summary = pd.DataFrame(acc_rows)
        if not acc_summary.empty:
            acc_summary['손익률(%)'] = (
                acc_summary['실현손익'] / acc_summary['누적매수금액'].replace(0, float('nan')) * 100
            ).round(2)

            # 상단 지표
            total_acc = len(acc_summary)
            best_acc = acc_summary.loc[acc_summary['실현손익'].idxmax()]
            ga1, ga2, ga3, ga4 = st.columns(4)
            ga1.metric("전체 계좌 수", f"{total_acc}개")
            ga2.metric("전체 실현손익", f"₩{acc_summary['실현손익'].sum():,.0f}")
            ga3.metric("최고 실현손익 계좌", best_acc['계좌'])
            ga4.metric("　", f"₩{best_acc['실현손익']:,.0f}")

            # 계좌별 손익 바 차트
            fig_a = go.Figure()
            colors_a = ['#e74c3c' if v >= 0 else '#2980b9' for v in acc_summary['실현손익']]
            fig_a.add_trace(go.Bar(
                x=acc_summary['계좌'],
                y=acc_summary['실현손익'],
                marker_color=colors_a,
                text=acc_summary['실현손익'].apply(lambda x: f"₩{x:,.0f}"),
                textposition='outside',
                name='실현손익',
            ))
            fig_a.update_layout(
                yaxis_title='실현손익 (원)',
                template='plotly_dark', height=350,
            )
            st.plotly_chart(fig_a, use_container_width=True)

            # 계좌별 종목 상세 확장
            st.divider()
            sel_acc_detail = st.selectbox("계좌 상세 보기", ['전체'] + sorted(accounts_list), key="tab7_acc")

            if sel_acc_detail == '전체':
                detail_pos = positions_df.copy()
                detail_pos['손익률(%)'] = (
                    detail_pos['실현손익'] / detail_pos['누적매수금액'].replace(0, float('nan')) * 100
                ).round(2)
                # 계좌 정보 붙이기
                acc_stock = combined_df.groupby(['종목키', '계좌']).size().reset_index()[['종목키', '계좌']]
                acc_stock = acc_stock.groupby('종목키')['계좌'].apply(lambda x: '/'.join(sorted(set(x)))).reset_index()
                detail_pos = detail_pos.merge(acc_stock, on='종목키', how='left')
            else:
                detail_pos = calculate_positions(combined_df[combined_df['계좌'] == sel_acc_detail])
                detail_pos['손익률(%)'] = (
                    detail_pos['실현손익'] / detail_pos['누적매수금액'].replace(0, float('nan')) * 100
                ).round(2)
                detail_pos['계좌'] = sel_acc_detail

            # 계좌별 요약 표
            st.write("**계좌별 요약**")
            acc_disp_cols = ['계좌', '종목수', '보유종목수', '실현손익', '손익률(%)', '누적매수금액', '누적매도금액', '보유원가', '총수수료']
            acc_disp_cols = [c for c in acc_disp_cols if c in acc_summary.columns]

            def color_acc_pnl(val):
                try:
                    v = float(val)
                    if v > 0: return 'color:#e74c3c;font-weight:bold'
                    if v < 0: return 'color:#2980b9;font-weight:bold'
                except Exception:
                    pass
                return ''

            st.dataframe(
                acc_summary[acc_disp_cols].sort_values('실현손익', ascending=False)
                .style
                .map(color_acc_pnl, subset=['실현손익', '손익률(%)'])
                .format({
                    '실현손익': '{:,.0f}', '손익률(%)': '{:+.2f}',
                    '누적매수금액': '{:,.0f}', '누적매도금액': '{:,.0f}',
                    '보유원가': '{:,.0f}', '총수수료': '{:,.0f}',
                }),
                use_container_width=True
            )

            # 선택 계좌 종목별 상세
            st.write(f"**{sel_acc_detail} 종목별 상세**")
            detail_cols = ['종목명', '잔고수량', '평균단가', '보유원가', '실현손익', '손익률(%)',
                           '누적매수금액', '누적매도금액']
            if sel_acc_detail == '전체' and '계좌' in detail_pos.columns:
                detail_cols = ['계좌'] + detail_cols
            detail_cols = [c for c in detail_cols if c in detail_pos.columns]

            st.dataframe(
                detail_pos[detail_cols].sort_values('실현손익', ascending=False)
                .style
                .map(color_acc_pnl, subset=['실현손익', '손익률(%)'])
                .format({
                    '잔고수량': '{:,.0f}', '평균단가': '{:,.0f}', '보유원가': '{:,.0f}',
                    '실현손익': '{:,.0f}', '손익률(%)': '{:+.2f}',
                    '누적매수금액': '{:,.0f}', '누적매도금액': '{:,.0f}',
                }),
                use_container_width=True, height=400
            )

            csv_a = acc_summary[acc_disp_cols].to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button("📥 계좌별 손익 CSV", csv_a, "계좌별손익.csv", "text/csv")
            st.divider()
            stock_selector_popup("종목 상세 팝업", sorted(detail_pos['종목명'].dropna().unique().tolist()), key="popup_tab7")


# ════════════════════════════════════════
# Tab 8: 수익곡선 / 자산 추이
# ════════════════════════════════════════
with tab8:
    st.subheader("📈 수익곡선 / 자산 추이")
    st.caption("매도 체결일 기준 누적 실현손익과 누적 투자원금(매수금액) 추이를 시계열로 표시합니다.")

    @st.cache_data
    def build_equity_curve(_df):
        if _df.empty:
            return pd.DataFrame()
        rows = []
        for sym_key in _df['종목키'].unique():
            detail = calculate_trade_detail(_df, sym_key)
            for _, r in detail.iterrows():
                rows.append({
                    '거래일자': r['거래일자'],
                    '매매유형': r.get('매매유형', ''),
                    '거래금액': float(r.get('거래금액', 0)),
                    '실현손익': float(r.get('실현손익', 0)),
                })
        if not rows:
            return pd.DataFrame()
        eq = pd.DataFrame(rows)
        eq['거래일자'] = pd.to_datetime(eq['거래일자'])
        # 날짜별 집계
        daily = eq.groupby('거래일자').agg(
            매수금액=('거래금액', lambda s: s[eq.loc[s.index, '매매유형'] == 'BUY'].sum()),
            실현손익=('실현손익', 'sum'),
        ).reset_index()
        daily = daily.sort_values('거래일자')
        daily['누적투자원금'] = daily['매수금액'].cumsum()
        daily['누적실현손익'] = daily['실현손익'].cumsum()
        daily['수익률(%)'] = (daily['누적실현손익'] / daily['누적투자원금'].replace(0, float('nan')) * 100).round(2)
        return daily

    eq_df = build_equity_curve(combined_df)

    if eq_df.empty:
        st.info("거래 데이터가 없습니다.")
    else:
        # 요약 지표
        last = eq_df.iloc[-1]
        e1, e2, e3, e4 = st.columns(4)
        e1.metric("누적 투자원금", f"₩{last['누적투자원금']:,.0f}")
        e2.metric("누적 실현손익", f"₩{last['누적실현손익']:,.0f}")
        e3.metric("전체 수익률", f"{last['수익률(%)']:+.2f}%")
        days_active = (eq_df['거래일자'].max() - eq_df['거래일자'].min()).days
        e4.metric("투자 활동 기간", f"{days_active}일")

        fig_eq = go.Figure()
        fig_eq.add_trace(go.Scatter(
            x=eq_df['거래일자'], y=eq_df['누적투자원금'],
            mode='lines', name='누적 투자원금',
            line=dict(color='#95a5a6', width=1.5, dash='dot'),
            fill='tozeroy', fillcolor='rgba(149,165,166,0.08)',
        ))
        fig_eq.add_trace(go.Scatter(
            x=eq_df['거래일자'], y=eq_df['누적실현손익'],
            mode='lines', name='누적 실현손익',
            line=dict(color='#f1c40f', width=2.5),
        ))
        fig_eq.add_hline(y=0, line_dash='dash', line_color='rgba(255,255,255,0.3)')
        fig_eq.update_layout(
            xaxis_title='날짜', yaxis_title='금액 (원)',
            template='plotly_dark', height=400,
            legend=dict(orientation='h', y=1.08),
        )
        st.plotly_chart(fig_eq, use_container_width=True)

        # 수익률 곡선
        fig_ret = go.Figure()
        fig_ret.add_trace(go.Scatter(
            x=eq_df['거래일자'], y=eq_df['수익률(%)'],
            mode='lines', name='누적 수익률(%)',
            line=dict(color='#2ecc71', width=2),
            fill='tozeroy',
            fillcolor=eq_df['수익률(%)'].apply(
                lambda v: 'rgba(46,204,113,0.15)' if v >= 0 else 'rgba(41,128,185,0.15)'
            ).iloc[-1],
        ))
        fig_ret.add_hline(y=0, line_dash='dash', line_color='rgba(255,255,255,0.3)')
        fig_ret.update_layout(
            xaxis_title='날짜', yaxis_title='수익률 (%)',
            template='plotly_dark', height=280,
        )
        st.plotly_chart(fig_ret, use_container_width=True)

        csv_eq = eq_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button("📥 수익곡선 CSV", csv_eq, "수익곡선.csv", "text/csv")
        st.divider()
        stock_selector_popup("종목 상세 팝업", sorted(combined_df['종목명'].dropna().unique().tolist()), key="popup_tab8")


# ════════════════════════════════════════
# Tab 9: 매매 패턴 분석
# ════════════════════════════════════════
with tab9:
    st.subheader("🔄 매매 패턴 분석")
    st.caption("요일별 매매 승률, 종목별 평균 보유기간, 매도 타이밍 분포를 분석합니다.")

    @st.cache_data
    def build_pattern_data(_df):
        if _df.empty:
            return pd.DataFrame(), pd.DataFrame()
        # 종목별 매수→매도 쌍으로 보유기간 계산
        hold_rows = []
        pnl_rows = []
        for sym_key in _df['종목키'].unique():
            detail = calculate_trade_detail(_df, sym_key)
            detail = detail.sort_values('거래일자')
            last_buy_date = None
            for _, r in detail.iterrows():
                if r.get('매매유형') == 'BUY':
                    last_buy_date = r['거래일자']
                elif r.get('매매유형') == 'SELL' and last_buy_date is not None:
                    hold_days = (r['거래일자'] - last_buy_date).days
                    pnl = float(r.get('실현손익', 0))
                    hold_rows.append({
                        '종목명': r['종목명'],
                        '매수일': last_buy_date,
                        '매도일': r['거래일자'],
                        '보유기간(일)': hold_days,
                        '실현손익': pnl,
                        '매도요일': r['거래일자'].strftime('%a'),
                        '매도요일번호': r['거래일자'].weekday(),
                    })
                    pnl_rows.append({'거래일자': r['거래일자'], '실현손익': pnl})
        hold_df = pd.DataFrame(hold_rows)
        pnl_df = pd.DataFrame(pnl_rows)
        return hold_df, pnl_df

    hold_df, sell_pnl_df = build_pattern_data(combined_df)

    if hold_df.empty:
        st.info("매도 이력이 없습니다.")
    else:
        # ── 요일별 승률
        st.markdown("#### 요일별 매도 승률")
        day_ko = {'Mon': '월', 'Tue': '화', 'Wed': '수', 'Thu': '목', 'Fri': '금', 'Sat': '토', 'Sun': '일'}
        hold_df['요일'] = hold_df['매도요일'].map(day_ko)
        day_order = ['월', '화', '수', '목', '금']
        day_group = hold_df.groupby('매도요일번호').agg(
            요일=('요일', 'first'),
            거래수=('실현손익', 'count'),
            승수=('실현손익', lambda x: (x > 0).sum()),
            평균손익=('실현손익', 'mean'),
        ).reset_index().sort_values('매도요일번호')
        day_group['승률(%)'] = (day_group['승수'] / day_group['거래수'] * 100).round(1)

        fig_day = go.Figure()
        bar_colors = ['#e74c3c' if v >= 50 else '#2980b9' for v in day_group['승률(%)']]
        fig_day.add_trace(go.Bar(
            x=day_group['요일'], y=day_group['승률(%)'],
            marker_color=bar_colors,
            text=day_group['승률(%)'].apply(lambda x: f"{x:.0f}%"),
            textposition='outside', name='승률',
        ))
        fig_day.add_hline(y=50, line_dash='dash', line_color='rgba(255,255,255,0.4)', annotation_text='50%')
        fig_day.update_layout(yaxis=dict(range=[0, 110]), template='plotly_dark', height=300)
        st.plotly_chart(fig_day, use_container_width=True)

        dg1, dg2 = st.columns(2)
        best_day = day_group.loc[day_group['승률(%)'].idxmax(), '요일']
        best_avg = day_group.loc[day_group['평균손익'].idxmax(), '요일']
        dg1.metric("최고 승률 요일", f"{best_day}요일", f"{day_group['승률(%)'].max():.0f}%")
        dg2.metric("평균손익 최고 요일", f"{best_avg}요일", f"₩{day_group['평균손익'].max():,.0f}")

        # ── 보유기간 분포
        st.markdown("#### 보유기간 분포")
        bins = [0, 1, 3, 7, 14, 30, 90, 180, 365, 99999]
        labels = ['당일', '2~3일', '4~7일', '8~14일', '15~30일', '1~3개월', '3~6개월', '6개월~1년', '1년+']
        hold_df['보유구간'] = pd.cut(hold_df['보유기간(일)'], bins=bins, labels=labels, right=True)
        hold_grp = hold_df.groupby('보유구간', observed=True).agg(
            거래수=('실현손익', 'count'),
            승수=('실현손익', lambda x: (x > 0).sum()),
            평균손익=('실현손익', 'mean'),
        ).reset_index()
        hold_grp['승률(%)'] = (hold_grp['승수'] / hold_grp['거래수'] * 100).round(1)

        fig_hold = go.Figure()
        fig_hold.add_trace(go.Bar(
            x=hold_grp['보유구간'].astype(str), y=hold_grp['거래수'],
            name='거래수', marker_color='#3498db', yaxis='y',
            text=hold_grp['거래수'], textposition='outside',
        ))
        fig_hold.add_trace(go.Scatter(
            x=hold_grp['보유구간'].astype(str), y=hold_grp['승률(%)'],
            mode='lines+markers', name='승률(%)',
            line=dict(color='#f1c40f', width=2), yaxis='y2',
        ))
        fig_hold.update_layout(
            yaxis=dict(title='거래 횟수'),
            yaxis2=dict(title='승률 (%)', overlaying='y', side='right', range=[0, 110]),
            template='plotly_dark', height=320,
            legend=dict(orientation='h', y=1.08),
        )
        st.plotly_chart(fig_hold, use_container_width=True)

        p1, p2, p3 = st.columns(3)
        p1.metric("평균 보유기간", f"{hold_df['보유기간(일)'].mean():.1f}일")
        p2.metric("중앙값 보유기간", f"{hold_df['보유기간(일)'].median():.0f}일")
        p3.metric("전체 매도 승률", f"{(hold_df['실현손익'] > 0).mean()*100:.1f}%")

        # ── 종목별 평균 보유기간 TOP
        st.markdown("#### 종목별 평균 보유기간")
        sym_hold = hold_df.groupby('종목명').agg(
            평균보유기간=('보유기간(일)', 'mean'),
            거래수=('실현손익', 'count'),
            승률=('실현손익', lambda x: (x > 0).mean() * 100),
        ).round(1).sort_values('평균보유기간', ascending=False).head(20)
        st.dataframe(sym_hold.style.format({'평균보유기간': '{:.1f}', '승률': '{:.1f}%'}),
                     use_container_width=True, height=300)
        st.divider()
        stock_selector_popup("종목 상세 팝업", sorted(combined_df['종목명'].dropna().unique().tolist()), key="popup_tab9")


# ════════════════════════════════════════
# Tab 10: 리스크 분석
# ════════════════════════════════════════
with tab10:
    st.subheader("⚠️ 리스크 분석")
    st.caption("종목 집중도, 최대 낙폭(MDD), 손익비를 분석합니다.")

    if positions_df.empty:
        st.info("데이터가 없습니다.")
    else:
        # ── 종목 집중도
        st.markdown("#### 보유 종목 집중도 (보유원가 기준)")
        hold_pos = positions_df[positions_df['잔고수량'] > 0].copy()

        if hold_pos.empty:
            st.info("현재 보유 중인 종목이 없습니다.")
        else:
            total_cost = hold_pos['보유원가'].sum()
            hold_pos['비중(%)'] = (hold_pos['보유원가'] / total_cost * 100).round(2)
            hold_pos = hold_pos.sort_values('비중(%)', ascending=False)

            fig_pie = go.Figure(go.Pie(
                labels=hold_pos['종목명'],
                values=hold_pos['보유원가'],
                hole=0.4,
                textinfo='label+percent',
                marker=dict(line=dict(color='#1a1a2e', width=1.5)),
            ))
            fig_pie.update_layout(template='plotly_dark', height=380,
                                  showlegend=False)
            st.plotly_chart(fig_pie, use_container_width=True)

            top1_weight = hold_pos['비중(%)'].iloc[0]
            top3_weight = hold_pos['비중(%)'].head(3).sum()
            hhi = ((hold_pos['비중(%)'] / 100) ** 2).sum()  # 허핀달-허쉬만 지수
            r1, r2, r3 = st.columns(3)
            r1.metric("TOP1 종목 비중", f"{top1_weight:.1f}%",
                      delta="⚠️ 집중 위험" if top1_weight > 50 else "✅ 양호")
            r2.metric("TOP3 종목 비중", f"{top3_weight:.1f}%")
            r3.metric("집중도 지수(HHI)", f"{hhi:.3f}",
                      delta="높을수록 집중" if hhi > 0.3 else None)

        # ── MDD (누적 실현손익 기준)
        st.markdown("#### 최대 낙폭 (MDD) — 누적 실현손익 기준")

        @st.cache_data
        def calc_mdd(_df):
            if _df.empty:
                return pd.DataFrame(), 0.0
            rows = []
            for sym_key in _df['종목키'].unique():
                detail = calculate_trade_detail(_df, sym_key)
                sells = detail[detail['매매유형'] == 'SELL'][['거래일자', '실현손익']].copy()
                rows.append(sells)
            if not rows:
                return pd.DataFrame(), 0.0
            ts = pd.concat(rows).groupby('거래일자')['실현손익'].sum().reset_index().sort_values('거래일자')
            ts['누적손익'] = ts['실현손익'].cumsum()
            ts['고점'] = ts['누적손익'].cummax()
            ts['낙폭'] = ts['누적손익'] - ts['고점']
            mdd = ts['낙폭'].min()
            return ts, mdd

        mdd_ts, mdd_val = calc_mdd(combined_df)

        if not mdd_ts.empty:
            fig_mdd = go.Figure()
            fig_mdd.add_trace(go.Scatter(
                x=mdd_ts['거래일자'], y=mdd_ts['누적손익'],
                mode='lines', name='누적 실현손익',
                line=dict(color='#f1c40f', width=2),
            ))
            fig_mdd.add_trace(go.Scatter(
                x=mdd_ts['거래일자'], y=mdd_ts['고점'],
                mode='lines', name='최고점',
                line=dict(color='#2ecc71', width=1, dash='dot'),
            ))
            fig_mdd.add_trace(go.Scatter(
                x=mdd_ts['거래일자'], y=mdd_ts['낙폭'],
                mode='lines', name='낙폭',
                fill='tozeroy', fillcolor='rgba(231,76,60,0.2)',
                line=dict(color='#e74c3c', width=1), yaxis='y2',
            ))
            fig_mdd.update_layout(
                yaxis=dict(title='누적손익 (원)'),
                yaxis2=dict(title='낙폭 (원)', overlaying='y', side='right'),
                template='plotly_dark', height=350,
                legend=dict(orientation='h', y=1.08),
            )
            st.plotly_chart(fig_mdd, use_container_width=True)
            m1, m2 = st.columns(2)
            m1.metric("최대 낙폭 (MDD)", f"₩{mdd_val:,.0f}")
            mdd_pct = (mdd_val / mdd_ts['고점'].max() * 100) if mdd_ts['고점'].max() != 0 else 0
            m2.metric("MDD 비율", f"{mdd_pct:.1f}%")

        # ── 손익비
        st.markdown("#### 손익비 분석")

        @st.cache_data
        def calc_profit_loss_ratio(_df):
            rows = []
            for sym_key in _df['종목키'].unique():
                detail = calculate_trade_detail(_df, sym_key)
                sells = detail[(detail['매매유형'] == 'SELL') & (detail['실현손익'] != 0)]['실현손익']
                rows.extend(sells.tolist())
            if not rows:
                return pd.Series(dtype=float)
            return pd.Series(rows)

        all_pnl = calc_profit_loss_ratio(combined_df)
        if not all_pnl.empty:
            wins  = all_pnl[all_pnl > 0]
            loses = all_pnl[all_pnl < 0]
            avg_win  = wins.mean()  if len(wins)  > 0 else 0
            avg_lose = abs(loses.mean()) if len(loses) > 0 else 0
            ratio = avg_win / avg_lose if avg_lose > 0 else float('inf')
            win_rate = len(wins) / len(all_pnl)
            # 기대값 = 승률×평균수익 - 패율×평균손실
            ev = win_rate * avg_win - (1 - win_rate) * avg_lose

            pl1, pl2, pl3, pl4 = st.columns(4)
            pl1.metric("평균 수익 (매도당)", f"₩{avg_win:,.0f}")
            pl2.metric("평균 손실 (매도당)", f"₩{avg_lose:,.0f}")
            pl3.metric("손익비 (Profit Factor)", f"{ratio:.2f}",
                       delta="✅ 우량" if ratio >= 1.5 else ("⚠️ 보통" if ratio >= 1 else "❌ 위험"))
            pl4.metric("매도당 기대손익", f"₩{ev:,.0f}")

            # 손익 분포 히스토그램
            fig_dist = go.Figure()
            fig_dist.add_trace(go.Histogram(
                x=all_pnl[all_pnl > 0], name='수익',
                marker_color='#e74c3c', opacity=0.75, nbinsx=30,
            ))
            fig_dist.add_trace(go.Histogram(
                x=all_pnl[all_pnl < 0], name='손실',
                marker_color='#2980b9', opacity=0.75, nbinsx=30,
            ))
            fig_dist.update_layout(
                barmode='overlay', xaxis_title='실현손익 (원)', yaxis_title='빈도',
                template='plotly_dark', height=300,
                legend=dict(orientation='h', y=1.08),
            )
            st.plotly_chart(fig_dist, use_container_width=True)


# ════════════════════════════════════════
# Tab 11: 목표 수익률 시뮬레이터
# ════════════════════════════════════════
with tab11:
    st.subheader("🎯 목표 수익률 시뮬레이터")
    st.caption("현재 잔고 보유원가 기준으로 목표 달성 시나리오를 계산합니다.")

    hold_pos_sim = positions_df[positions_df['잔고수량'] > 0].copy()
    total_holding_cost = hold_pos_sim['보유원가'].sum()
    total_realized = positions_df['실현손익'].sum()

    s1, s2 = st.columns(2)
    s1.metric("현재 보유원가 합계", f"₩{total_holding_cost:,.0f}")
    s2.metric("누적 실현손익", f"₩{total_realized:,.0f}")

    st.divider()
    sim1, sim2, sim3 = st.columns(3)
    target_amount   = sim1.number_input("목표 누적 실현손익 (원)", min_value=0,
                                         value=int(max(total_realized * 2, 10_000_000)),
                                         step=1_000_000, format="%d")
    monthly_add     = sim2.number_input("월 추가 투자금 (원)", min_value=0,
                                         value=1_000_000, step=100_000, format="%d")
    expected_return = sim3.number_input("예상 연 수익률 (%)", min_value=0.1,
                                         value=15.0, step=0.5, format="%.1f")

    remaining = target_amount - total_realized
    st.markdown(f"**목표까지 남은 금액: ₩{remaining:,.0f}**")

    if remaining <= 0:
        st.success("🎉 이미 목표를 달성했습니다!")
    else:
        # 월별 복리 시뮬레이션
        monthly_rate = expected_return / 100 / 12
        capital = total_holding_cost
        cumulative_pnl = total_realized
        sim_rows = []
        for month in range(1, 361):
            capital  += monthly_add
            monthly_profit = capital * monthly_rate
            cumulative_pnl += monthly_profit
            sim_rows.append({
                '월차': month,
                '자산': capital + cumulative_pnl,
                '누적손익': cumulative_pnl,
                '투자원금': capital,
            })
            if cumulative_pnl >= target_amount:
                break

        sim_df = pd.DataFrame(sim_rows)
        reached_month = sim_df[sim_df['누적손익'] >= target_amount]

        if not reached_month.empty:
            m = int(reached_month.iloc[0]['월차'])
            years, months = divmod(m, 12)
            st.success(f"🎯 예상 달성 시점: **{m}개월 후** ({years}년 {months}개월)")
        else:
            st.warning("360개월(30년) 내 달성이 어렵습니다. 수익률이나 추가 투자금을 높여보세요.")

        fig_sim = go.Figure()
        fig_sim.add_trace(go.Scatter(
            x=sim_df['월차'], y=sim_df['투자원금'],
            mode='lines', name='누적 투자원금',
            line=dict(color='#95a5a6', width=1.5, dash='dot'),
        ))
        fig_sim.add_trace(go.Scatter(
            x=sim_df['월차'], y=sim_df['누적손익'],
            mode='lines', name='누적 실현손익',
            line=dict(color='#f1c40f', width=2.5),
        ))
        fig_sim.add_hline(y=target_amount, line_dash='dash', line_color='#e74c3c',
                          annotation_text=f"목표 ₩{target_amount:,.0f}", annotation_position="top right")
        fig_sim.update_layout(
            xaxis_title='월차', yaxis_title='금액 (원)',
            template='plotly_dark', height=380,
            legend=dict(orientation='h', y=1.08),
        )
        st.plotly_chart(fig_sim, use_container_width=True)

        # 시나리오 비교 (수익률 ±5%)
        st.markdown("#### 수익률 시나리오 비교")
        scenario_rates = [expected_return - 5, expected_return, expected_return + 5]
        sc_cols = st.columns(len(scenario_rates))
        for i, rate in enumerate(scenario_rates):
            if rate <= 0:
                sc_cols[i].metric(f"연 {rate:.0f}%", "불가")
                continue
            mr = rate / 100 / 12
            cap = total_holding_cost
            cpnl = total_realized
            for mo in range(1, 361):
                cap += monthly_add
                cpnl += cap * mr
                if cpnl >= target_amount:
                    break
            else:
                mo = 999
            label = f"{'목표 초과' if mo == 999 else f'{mo}개월'}"
            sc_cols[i].metric(f"연 {rate:.0f}%", label,
                              delta="현재 시나리오" if rate == expected_return else None)


# ════════════════════════════════════════
# Tab 12: 벤치마크 비교
# ════════════════════════════════════════
with tab12:
    st.subheader("📊 벤치마크 비교")
    st.caption("내 수익률을 KOSPI / KOSDAQ 지수와 비교합니다. (야후 파이낸스 데이터 활용)")

    try:
        import yfinance as yf
        yf_available = True
    except ImportError:
        yf_available = False

    if not yf_available:
        st.warning("`yfinance` 패키지가 설치되어 있지 않습니다.\n\n```\npip install yfinance\n```\n\n설치 후 앱을 재시작하면 벤치마크 비교를 사용할 수 있습니다.")
    elif combined_df.empty:
        st.info("거래 데이터가 없습니다.")
    else:
        date_min = combined_df['거래일자'].min().date()
        date_max = combined_df['거래일자'].max().date()

        bm1, bm2 = st.columns(2)
        bm_start = bm1.date_input("비교 시작일", value=date_min, key="bm_start")
        bm_end   = bm2.date_input("비교 종료일", value=date_max, key="bm_end")

        bm_tickers = {
            'KOSPI': '^KS11',
            'KOSDAQ': '^KQ11',
        }

        @st.cache_data(ttl=3600)
        def fetch_benchmark(tickers, start, end):
            results = {}
            for name, tkr in tickers.items():
                try:
                    data = yf.download(tkr, start=str(start), end=str(end), progress=False, auto_adjust=True)
                    if not data.empty:
                        close = data['Close'].squeeze()
                        results[name] = (close / close.iloc[0] * 100 - 100).round(2)
                except Exception:
                    pass
            return results

        with st.spinner("벤치마크 데이터 불러오는 중..."):
            bm_data = fetch_benchmark(bm_tickers, bm_start, bm_end)

        # 내 포트폴리오 수익률 곡선 재계산
        eq_bm = build_equity_curve(combined_df)
        if not eq_bm.empty:
            eq_bm = eq_bm[(eq_bm['거래일자'].dt.date >= bm_start) &
                          (eq_bm['거래일자'].dt.date <= bm_end)]

        fig_bm = go.Figure()

        if not eq_bm.empty and eq_bm['누적투자원금'].iloc[0] != 0:
            base_inv = eq_bm['누적투자원금'].iloc[0]
            my_ret = (eq_bm['누적실현손익'] / base_inv * 100).round(2)
            fig_bm.add_trace(go.Scatter(
                x=eq_bm['거래일자'], y=my_ret,
                mode='lines', name='내 포트폴리오',
                line=dict(color='#f1c40f', width=2.5),
            ))

        colors_bm = {'KOSPI': '#e74c3c', 'KOSDAQ': '#3498db'}
        for name, series in bm_data.items():
            fig_bm.add_trace(go.Scatter(
                x=series.index, y=series.values,
                mode='lines', name=name,
                line=dict(color=colors_bm.get(name, '#aaa'), width=1.5, dash='dot'),
            ))

        fig_bm.add_hline(y=0, line_dash='dash', line_color='rgba(255,255,255,0.3)')
        fig_bm.update_layout(
            xaxis_title='날짜', yaxis_title='수익률 (%)',
            template='plotly_dark', height=420,
            legend=dict(orientation='h', y=1.08),
        )
        st.plotly_chart(fig_bm, use_container_width=True)

        # 수익률 요약 표
        summary_bm = []
        if not eq_bm.empty and eq_bm['누적투자원금'].iloc[0] != 0:
            my_total_ret = (eq_bm['누적실현손익'].iloc[-1] / eq_bm['누적투자원금'].iloc[0] * 100)
            summary_bm.append({'구분': '내 포트폴리오', '기간 수익률(%)': round(my_total_ret, 2)})
        for name, series in bm_data.items():
            summary_bm.append({'구분': name, '기간 수익률(%)': round(float(series.iloc[-1]), 2)})

        if summary_bm:
            sum_df = pd.DataFrame(summary_bm)

            def color_bm(val):
                try:
                    v = float(val)
                    if v > 0: return 'color:#e74c3c;font-weight:bold'
                    if v < 0: return 'color:#2980b9;font-weight:bold'
                except Exception:
                    pass
                return ''

            st.dataframe(
                sum_df.style.map(color_bm, subset=['기간 수익률(%)']).format({'기간 수익률(%)': '{:+.2f}%'}),
                use_container_width=True, height=150,
            )

            if len(summary_bm) > 1 and summary_bm[0]['구분'] == '내 포트폴리오':
                my_r = summary_bm[0]['기간 수익률(%)']
                for row in summary_bm[1:]:
                    diff = my_r - row['기간 수익률(%)']
                    icon = "📈 초과" if diff > 0 else "📉 미달"
                    st.metric(
                        f"vs {row['구분']}",
                        f"{diff:+.2f}%p",
                        delta=icon,
                        delta_color="normal" if diff > 0 else "inverse",
                    )

# ════════════════════════════════════════
# Tab 13: 매매 시나리오
# ════════════════════════════════════════
with tab13:
    st.subheader("🤖 매매 시나리오 분석")
    st.caption("현재 보유 종목 기준으로 익절/손절 시나리오, 최적 보유기간, 목표가 달성 알림을 분석합니다.")

    hold_pos_sc = positions_df[positions_df['잔고수량'] > 0].copy()

    if hold_pos_sc.empty:
        st.info("현재 보유 중인 종목이 없습니다.")
    else:
        hold_pos_sc['손익률(%)'] = (
            hold_pos_sc['실현손익'] / hold_pos_sc['누적매수금액'].replace(0, float('nan')) * 100
        ).round(2)

        sc_tab1, sc_tab2, sc_tab3 = st.tabs([
            "📌 목표가 / 손절가 설정",
            "⏱ 최적 보유기간",
            "📊 익절/손절 시뮬레이션",
        ])

        # ── sc_tab1: 목표가 / 손절가 설정 ──
        with sc_tab1:
            st.markdown("#### 종목별 목표가 / 손절가 설정")
            st.caption("현재 평균단가 기준으로 목표 수익률과 손절 수익률을 입력하면 목표가/손절가와 예상 손익을 계산합니다.")

            col_tp, col_sl = st.columns(2)
            target_pct  = col_tp.number_input("목표 수익률 (%)", min_value=0.1, value=10.0, step=0.5, format="%.1f")
            stoploss_pct = col_sl.number_input("손절 수익률 (%)", min_value=0.1, value=5.0,  step=0.5, format="%.1f")

            rows_tp = []
            for _, r in hold_pos_sc.iterrows():
                avg   = float(r['평균단가'])
                qty   = float(r['잔고수량'])
                cost  = float(r['보유원가'])
                tp    = avg * (1 + target_pct / 100)
                sl    = avg * (1 - stoploss_pct / 100)
                tp_pnl = (tp - avg) * qty
                sl_pnl = (sl - avg) * qty
                rows_tp.append({
                    '종목명':   r['종목명'],
                    '평균단가': avg,
                    '잔고수량': qty,
                    '보유원가': cost,
                    '목표가':   round(tp),
                    '손절가':   round(sl),
                    '목표 달성 시 손익':  round(tp_pnl),
                    '손절 시 손익':       round(sl_pnl),
                    '목표가 대비 필요 상승': f"+{target_pct:.1f}%",
                    '손절가 대비 하락폭':   f"-{stoploss_pct:.1f}%",
                })

            tp_df = pd.DataFrame(rows_tp)

            # 요약 지표
            t1, t2, t3, t4 = st.columns(4)
            t1.metric("보유 종목 수", f"{len(tp_df)}개")
            t2.metric("전체 목표 달성 시 총손익", f"₩{tp_df['목표 달성 시 손익'].sum():,.0f}")
            t3.metric("전체 손절 시 총손실", f"₩{tp_df['손절 시 손익'].sum():,.0f}")
            rr = abs(tp_df['목표 달성 시 손익'].sum() / tp_df['손절 시 손익'].sum()) if tp_df['손절 시 손익'].sum() != 0 else 0
            t4.metric("포트폴리오 손익비", f"{rr:.2f}:1")

            # 목표가/손절가 차트
            fig_tp = go.Figure()
            fig_tp.add_trace(go.Bar(
                name='목표가까지', x=tp_df['종목명'],
                y=tp_df['목표 달성 시 손익'],
                marker_color='#e74c3c', opacity=0.85,
                text=tp_df['목표가'].apply(lambda x: f"₩{x:,.0f}"),
                textposition='outside',
            ))
            fig_tp.add_trace(go.Bar(
                name='손절 시', x=tp_df['종목명'],
                y=tp_df['손절 시 손익'],
                marker_color='#2980b9', opacity=0.85,
                text=tp_df['손절가'].apply(lambda x: f"₩{x:,.0f}"),
                textposition='outside',
            ))
            fig_tp.update_layout(
                barmode='group', yaxis_title='예상 손익 (원)',
                template='plotly_dark', height=380,
                legend=dict(orientation='h', y=1.08),
            )
            st.plotly_chart(fig_tp, use_container_width=True)

            def _color_tp(val):
                try:
                    v = float(val)
                    if v > 0: return 'color:#e74c3c;font-weight:bold'
                    if v < 0: return 'color:#2980b9;font-weight:bold'
                except: pass
                return ''

            disp_cols_tp = ['종목명', '평균단가', '잔고수량', '보유원가', '목표가', '손절가',
                            '목표 달성 시 손익', '손절 시 손익']
            st.dataframe(
                tp_df[disp_cols_tp]
                .style
                .map(_color_tp, subset=['목표 달성 시 손익', '손절 시 손익'])
                .format({
                    '평균단가': '{:,.0f}', '잔고수량': '{:,.0f}', '보유원가': '{:,.0f}',
                    '목표가': '{:,.0f}', '손절가': '{:,.0f}',
                    '목표 달성 시 손익': '{:,.0f}', '손절 시 손익': '{:,.0f}',
                }),
                use_container_width=True, height=380,
            )
            csv_tp = tp_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button("📥 목표가/손절가 CSV", csv_tp, "목표가손절가.csv", "text/csv")

        # ── sc_tab2: 최적 보유기간 ──
        with sc_tab2:
            st.markdown("#### 과거 패턴 기반 최적 보유기간 분석")
            st.caption("과거 매도 이력에서 보유기간별 평균 손익률을 계산해 최적 보유기간을 도출합니다.")

            @st.cache_data
            def calc_optimal_hold(_df):
                rows = []
                for sym_key in _df['종목키'].unique():
                    detail = calculate_trade_detail(_df, sym_key)
                    detail = detail.sort_values('거래일자')
                    last_buy_date = None
                    avg_at_buy = None
                    for _, r in detail.iterrows():
                        if r.get('매매유형') == 'BUY':
                            last_buy_date = r['거래일자']
                            avg_at_buy = float(r.get('평균단가', 0))
                        elif r.get('매매유형') == 'SELL' and last_buy_date is not None:
                            hold_days = (r['거래일자'] - last_buy_date).days
                            pnl = float(r.get('실현손익', 0))
                            cost_basis = avg_at_buy * float(r.get('거래수량', 0)) if avg_at_buy else 0
                            pnl_pct = (pnl / cost_basis * 100) if cost_basis > 0 else 0
                            rows.append({
                                '종목명':   r['종목명'],
                                '종목키':   sym_key,
                                '보유기간': hold_days,
                                '실현손익': pnl,
                                '손익률':   pnl_pct,
                                '승리':     1 if pnl > 0 else 0,
                            })
                return pd.DataFrame(rows)

            opt_df = calc_optimal_hold(combined_df)

            if opt_df.empty:
                st.info("매도 이력이 없습니다.")
            else:
                bins  = [0, 1, 3, 7, 14, 30, 90, 180, 365, 99999]
                labels = ['당일', '2~3일', '4~7일', '8~14일', '15~30일', '1~3개월', '3~6개월', '6개월~1년', '1년+']
                opt_df['보유구간'] = pd.cut(opt_df['보유기간'], bins=bins, labels=labels, right=True)

                grp = opt_df.groupby('보유구간', observed=True).agg(
                    거래수=('실현손익', 'count'),
                    평균손익률=('손익률', 'mean'),
                    승률=('승리', 'mean'),
                    총손익=('실현손익', 'sum'),
                ).reset_index()
                grp['승률(%)'] = (grp['승률'] * 100).round(1)
                grp['평균손익률(%)'] = grp['평균손익률'].round(2)

                best_row = grp.loc[grp['평균손익률(%)'].idxmax()]
                best_win  = grp.loc[grp['승률(%)'].idxmax()]

                oh1, oh2, oh3 = st.columns(3)
                oh1.metric("평균손익률 최고 구간", str(best_row['보유구간']), f"{best_row['평균손익률(%)']:+.1f}%")
                oh2.metric("승률 최고 구간", str(best_win['보유구간']), f"{best_win['승률(%)']:.0f}%")
                oh3.metric("전체 분석 거래수", f"{len(opt_df)}건")

                fig_oh = go.Figure()
                bar_colors = ['#e74c3c' if v >= 0 else '#2980b9' for v in grp['평균손익률(%)']]
                fig_oh.add_trace(go.Bar(
                    x=grp['보유구간'].astype(str), y=grp['평균손익률(%)'],
                    name='평균손익률(%)', marker_color=bar_colors,
                    text=grp['평균손익률(%)'].apply(lambda x: f"{x:+.1f}%"),
                    textposition='outside',
                ))
                fig_oh.add_trace(go.Scatter(
                    x=grp['보유구간'].astype(str), y=grp['승률(%)'],
                    mode='lines+markers', name='승률(%)',
                    line=dict(color='#f1c40f', width=2), yaxis='y2',
                ))
                fig_oh.add_hline(y=0, line_dash='dash', line_color='rgba(255,255,255,0.3)')
                fig_oh.update_layout(
                    yaxis=dict(title='평균손익률 (%)'),
                    yaxis2=dict(title='승률 (%)', overlaying='y', side='right', range=[0, 110]),
                    template='plotly_dark', height=360,
                    legend=dict(orientation='h', y=1.08),
                )
                st.plotly_chart(fig_oh, use_container_width=True)

                # 현재 보유 종목별 권장 보유기간
                st.markdown("#### 현재 보유 종목 — 과거 패턴 기반 권장 구간")
                rec_rows = []
                for _, r in hold_pos_sc.iterrows():
                    sym = r['종목키']
                    sym_hist = opt_df[opt_df['종목키'] == sym]
                    if sym_hist.empty:
                        rec_rows.append({'종목명': r['종목명'], '과거 매도수': 0,
                                         '권장 보유구간': '데이터 없음', '해당구간 평균손익률': '-', '해당구간 승률': '-'})
                        continue
                    sg = sym_hist.groupby('보유구간', observed=True).agg(
                        평균손익률=('손익률', 'mean'), 승률=('승리', 'mean'), 거래수=('실현손익', 'count')
                    ).reset_index()
                    if sg.empty:
                        rec_rows.append({'종목명': r['종목명'], '과거 매도수': len(sym_hist),
                                         '권장 보유구간': '데이터 부족', '해당구간 평균손익률': '-', '해당구간 승률': '-'})
                        continue
                    best = sg.loc[sg['평균손익률'].idxmax()]
                    rec_rows.append({
                        '종목명':          r['종목명'],
                        '과거 매도수':     len(sym_hist),
                        '권장 보유구간':   str(best['보유구간']),
                        '해당구간 평균손익률': f"{best['평균손익률']:+.1f}%",
                        '해당구간 승률':   f"{best['승률']*100:.0f}%",
                    })
                st.dataframe(pd.DataFrame(rec_rows), use_container_width=True, height=300)

        # ── sc_tab3: 익절/손절 시뮬레이션 ──
        with sc_tab3:
            st.markdown("#### 보유 종목별 익절/손절 시나리오 시뮬레이션")
            st.caption("현재가를 입력하면 각 시나리오별 최종 손익을 시뮬레이션합니다.")

            stock_list_sc = sorted(hold_pos_sc['종목명'].tolist())
            sel_sc = st.selectbox("종목 선택", stock_list_sc, key="sc_stock_sel")

            if sel_sc:
                pos_sc = hold_pos_sc[hold_pos_sc['종목명'] == sel_sc].iloc[0]
                avg_sc  = float(pos_sc['평균단가'])
                qty_sc  = float(pos_sc['잔고수량'])
                cost_sc = float(pos_sc['보유원가'])
                realized_sc = float(pos_sc['실현손익'])

                sc1, sc2, sc3 = st.columns(3)
                current_price = sc1.number_input("현재가 (원)", min_value=1,
                                                  value=int(avg_sc), step=100, format="%d")
                tp_pct_sim  = sc2.number_input("익절 목표 (%)", min_value=0.1, value=10.0, step=0.5)
                sl_pct_sim  = sc3.number_input("손절 기준 (%)", min_value=0.1, value=5.0,  step=0.5)

                unrealized = (current_price - avg_sc) * qty_sc
                tp_price   = avg_sc * (1 + tp_pct_sim / 100)
                sl_price   = avg_sc * (1 - sl_pct_sim / 100)
                tp_pnl_sim = (tp_price - avg_sc) * qty_sc
                sl_pnl_sim = (sl_price - avg_sc) * qty_sc

                st.divider()
                ms1, ms2, ms3, ms4 = st.columns(4)
                ms1.metric("평균단가", f"₩{avg_sc:,.0f}")
                ms2.metric("현재 평가손익", f"₩{unrealized:,.0f}",
                           delta=f"{unrealized/cost_sc*100:+.2f}%" if cost_sc else None,
                           delta_color="normal" if unrealized >= 0 else "inverse")
                ms3.metric("익절가", f"₩{tp_price:,.0f}", delta=f"+{tp_pct_sim:.1f}%")
                ms4.metric("손절가", f"₩{sl_price:,.0f}", delta=f"-{sl_pct_sim:.1f}%", delta_color="inverse")

                # 시나리오 비교 표
                scenarios = [
                    {"시나리오": "즉시 매도 (현재가)", "매도가": current_price,
                     "매도 시 손익": round((current_price - avg_sc) * qty_sc),
                     "누적 총손익 (실현+미실현)": round(realized_sc + (current_price - avg_sc) * qty_sc)},
                    {"시나리오": f"익절 ({tp_pct_sim:+.1f}%)", "매도가": round(tp_price),
                     "매도 시 손익": round(tp_pnl_sim),
                     "누적 총손익 (실현+미실현)": round(realized_sc + tp_pnl_sim)},
                    {"시나리오": f"손절 (-{sl_pct_sim:.1f}%)", "매도가": round(sl_price),
                     "매도 시 손익": round(sl_pnl_sim),
                     "누적 총손익 (실현+미실현)": round(realized_sc + sl_pnl_sim)},
                    {"시나리오": "본전 매도", "매도가": round(avg_sc),
                     "매도 시 손익": 0,
                     "누적 총손익 (실현+미실현)": round(realized_sc)},
                ]
                sc_df = pd.DataFrame(scenarios)

                def _color_sc(val):
                    try:
                        v = float(val)
                        if v > 0: return 'color:#e74c3c;font-weight:bold'
                        if v < 0: return 'color:#2980b9;font-weight:bold'
                    except: pass
                    return ''

                st.dataframe(
                    sc_df.style
                    .map(_color_sc, subset=['매도 시 손익', '누적 총손익 (실현+미실현)'])
                    .format({'매도가': '{:,.0f}', '매도 시 손익': '{:,.0f}',
                             '누적 총손익 (실현+미실현)': '{:,.0f}'}),
                    use_container_width=True, height=210,
                )

                # 가격 범위별 손익 곡선
                import numpy as np
                prices = np.linspace(avg_sc * 0.7, avg_sc * 1.5, 100)
                pnls   = (prices - avg_sc) * qty_sc

                fig_sc = go.Figure()
                fig_sc.add_trace(go.Scatter(
                    x=prices, y=pnls, mode='lines', name='매도 시 손익',
                    line=dict(width=2.5, color='#f1c40f'),
                    fill='tozeroy',
                    fillcolor='rgba(231,76,60,0.15)',
                ))
                fig_sc.add_vline(x=current_price, line_dash='dash', line_color='#95a5a6',
                                 annotation_text=f"현재가 ₩{current_price:,.0f}")
                fig_sc.add_vline(x=tp_price, line_dash='dot', line_color='#e74c3c',
                                 annotation_text=f"익절 ₩{round(tp_price):,.0f}")
                fig_sc.add_vline(x=sl_price, line_dash='dot', line_color='#2980b9',
                                 annotation_text=f"손절 ₩{round(sl_price):,.0f}")
                fig_sc.add_hline(y=0, line_dash='dash', line_color='rgba(255,255,255,0.3)')
                fig_sc.update_layout(
                    xaxis_title='매도 가격 (원)', yaxis_title='손익 (원)',
                    template='plotly_dark', height=360,
                )
                st.plotly_chart(fig_sc, use_container_width=True)
