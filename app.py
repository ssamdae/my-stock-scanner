import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
import yfinance as yf
import socket  # 💡 무한 대기 방지용 소켓 라이브러리 추가
from concurrent.futures import ThreadPoolExecutor, as_completed

# 💡 [핵심] 글로벌 소켓 타임아웃을 7초로 제한 (네트워크 먹통으로 인한 스레드 멈춤 원천 차단)
socket.setdefaulttimeout(7)

# 1. 페이지 설정
st.set_page_config(page_title="돌파", layout="wide")
st.title("🚀돌파")


# 2. 텔레그램 전송 함수
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        response = requests.post(url, data=payload, timeout=5)
        return response.status_code == 200
    except Exception:
        return False


# 개별 종목 분석용 워커 함수 (멀티스레드)
def analyze_single_stock(row, start_date, end_date):
    if not row or not row[0]: 
        return None
        
    ticker = row[0].strip()
    name = row[1].strip()
    df = None
    
    try:
        # 1. pykrx 시도
        df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
        
        # 2. 백업: yfinance 시도 (pykrx 실패 시)
        if df is None or df.empty or len(df) < 224:
            for suffix in [".KS", ".KQ"]:
                # 💡 threads=False 옵션을 주어 멀티스레드 내부에서 데드락이 걸리는 현상 방지
                df_yf = yf.download(
                    ticker + suffix, 
                    start=(datetime.now() - timedelta(days=450)), 
                    end=datetime.now(), 
                    progress=False, 
                    show_errors=False,
                    threads=False,
                    timeout=5
                )
                if not df_yf.empty and len(df_yf) >= 224:
                    df = df_yf.rename(columns={'Close': '종가', 'Volume': '거래량'})
                    break

        if df is not None and not df.empty and len(df) >= 224:
            if 'Volume' in df.columns: df = df.rename(columns={'Volume': '거래량'})
            if 'Close' in df.columns: df = df.rename(columns={'Close': '종가'})

            close_series = df['종가'].iloc[:, 0] if isinstance(df['종가'], pd.DataFrame) else df['종가']
            vol_series = df['거래량'].iloc[:, 0] if isinstance(df['거래량'], pd.DataFrame) else df['거래량']

            ma120 = close_series.rolling(120).mean().iloc[-1]
            ma224 = close_series.rolling(224).mean().iloc[-1]
            upper_ma = max(ma120, ma224)

            prev_close = close_series.iloc[-2]
            last_close = close_series.iloc[-1]
            
            prev_vol = vol_series.iloc[-2]
            last_vol = vol_series.iloc[-1]
            vol_ratio = (last_vol / prev_vol * 100) if prev_vol > 0 else 0

            is_breakout = prev_close < upper_ma < last_close
            is_vol_surge = vol_ratio >= 200

            if is_breakout and is_vol_surge:
                return {
                    '종목명': name, 
                    '현재가': f"{int(last_close):,}",
                    '거래량비율': f"{vol_ratio:.1f}%",
                    '테마1': row[2].strip() if len(row) > 2 else "",
                    '테마2': row[3].strip() if len(row) > 3 else "",
                    '테마3': row[4].strip() if len(row) > 4 else ""
                }
    except Exception:
        pass
    return None


# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        csv_path = os.path.expanduser("~/my-stock-scanner/watchlist.csv")
        
        if not os.path.exists(csv_path):
            st.error(f"❌ 서버 내에 watchlist.csv 파일이 없습니다. 경로를 확인해 주세요: {csv_path}")
            st.stop()
            
        with st.spinner('서버 로컬 CSV 파일에서 종목을 불러오는 중...'):
            df_stocks = pd.read_csv(csv_path, dtype={'티कर': str}, encoding='utf-8-sig').fillna('')
            rows = df_stocks.values.tolist()
            
            if not rows:
                st.warning("분석할 종목 데이터가 없습니다.")
                st.stop()

        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=450)).strftime("%Y%m%d")

        # 💡 사양과 API 제한을 고려해 max_workers를 5로 안전하게 하향 조정
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(analyze_single_stock, row, start_date, end_date): row for row in rows}
            
            completed_count = 0
            for future in as_completed(futures):
                completed_count += 1
                row = futures[future]
                name = row[1].strip() if len(row) > 1 else "Unknown"
                
                progress_bar.progress(completed_count / len(rows))
                status_text.text(f"진행 중 ({completed_count}/{len(rows)}): {name}")
                
                try:
                    # 💡 혹시 모를 내부 예외 상황도 5초 타임아웃으로 방어
                    result = future.result(timeout=5)
                    if result is not None:
                        matched.append(result)
                except Exception:
                    pass

        status_text.empty()
        progress_bar.empty()

        if matched:
            res_df = pd.DataFrame(matched)
            
            for t in ['테마1', '테마2', '테마3']:
                counts = res_df[res_df[t] != ''][t].value_counts()
                res_df[f'{t}_빈도'] = res_df[t].map(counts).fillna(0)
            
            res_df = res_df.sort_values(
                by=['테마1_빈도', '테마1', '테마2_빈도', '테마2', '테마3_빈도', '종목명'], 
                ascending=[False, True, False, True, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}개의 돌파 종목 발견! (기준일: {end_date})")
            
            display_df = res_df[['종목명', '현재가', '거래량비율', '테마1', '테마2', '테마3']]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            if btn_tele:
                msg = f"<b>🚀 [강력 돌파 포착: {end_date}]</b>\n"
                msg += f"상단 이평선 돌파 + 거래량 200%↑\n"
                msg += f"총 <b>{len(res_df)}건</b>\n\n"
                
                for _, r in res_df.iterrows():
                    theme_list = [t for t in [r['테마1'], r['테마2'], r['테마3']] if t.strip()]
                    theme_str = ", ".join(theme_list)
                    msg += f"• <b>{r['종목명']}</b> (🔥{r['거래량비율']}) | {theme_str}\n"
                
                if send_telegram_msg(msg):
                    st.toast("텔레그램 전송 완료!")
                else:
                    st.error("텔레그램 전송 실패")
        else:
            st.warning("조건(돌파 + 거래량 2배)에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"❌ 시스템 오류: {str(e)}")
