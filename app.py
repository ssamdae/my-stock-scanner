import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
import yfinance as yf

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


# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        csv_path = os.path.expanduser("~/my-stock-scanner/watchlist.csv")
        
        if not os.path.exists(csv_path):
            st.error(f"❌ 서버 내에 watchlist.csv 파일이 없습니다. 경로 확인 필수: {csv_path}")
            st.stop()
            
        with st.spinner('서버 로컬 CSV 파일에서 종목을 불러오는 중...'):
            df_stocks = pd.read_csv(csv_path, dtype={'티커': str}, encoding='utf-8-sig').fillna('')
            rows = df_stocks.values.tolist()
            
            if not rows:
                st.warning("분석할 종목 데이터가 없습니다.")
                st.stop()

        with st.spinner('시장 데이터 동기화 중...'):
            kospi_tickers = set(stock.get_market_ticker_list(market="KOSPI"))

        # 야후 파이낸스용 일괄 요청 티커 배열 만들기
        yf_tickers = []
        ticker_to_row = {}  # 데이터 파싱 시 테마 매칭용 딕셔너리
        
        for row in rows:
            if not row or not row[0]: continue
            
            # 앞뒤 공백을 자른 뒤 문자열로 강제 변환
            ticker = str(row[0]).strip()
            
            # 대한민국 주식 티커 6자리 숫자 형태 검증 (유령 공백 행 완벽 필터링)
            if len(ticker) != 6 or not ticker.isdigit(): 
                continue
                
            suffix = ".KS" if ticker in kospi_tickers else ".KQ"
            yf_ticker = ticker + suffix
            
            # CSV 내 중복 등록 종목 중복 스캔 방지
            if yf_ticker not in yf_tickers:
                yf_tickers.append(yf_ticker)
                ticker_to_row[yf_ticker] = row

        # 데이터 다운로드 실행
        with st.spinner(f'🚀 야후 파이낸스에서 {len(yf_tickers)}개 종목 대량 멀티 데이터 다운로드 중...'):
            end_date_dt = datetime.now()
            # 💡 [반영 사항] 불필요한 과거 데이터를 줄이고 안전 마진만 남긴 360일 설정
            start_date_dt = end_date_dt - timedelta(days=360)
            
            df_all = yf.download(
                tickers=yf_tickers,
                start=start_date_dt,
                end=end_date_dt,
                group_by='ticker',
                progress=False,
                show_errors=False
            )

        if df_all.empty:
            st.error("데이터를 불러오지 못했습니다. 야후 파이낸스 통신을 확인해 주세요.")
            st.stop()

        # 거대 DataFrame을 메모리 상에서 초고속 루프 연산
        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_tickers = len(yf_tickers)
        for idx, yf_ticker in enumerate(yf_tickers):
            progress_bar.progress((idx + 1) / total_tickers)
            row = ticker_to_row[yf_ticker]
            name = row[1].strip()
            status_text.text(f"초고속 연산 중 ({idx+1}/{total_tickers}): {name}")
            
            try:
                if yf_ticker in df_all.columns.levels[0]:
                    df_stock = df_all[yf_ticker].dropna(subset=['Close'])
                else:
                    if total_tickers == 1:
                        df_stock = df_all.dropna(subset=['Close'])
                    else:
                        continue
                
                if len(df_stock) < 224: 
                    continue
                
                close_series = df_stock['Close']
                vol_series = df_stock['Volume']

                # 이평선 연산 (120일선, 224일선)
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
                    matched.append({
                        '종목명': name, 
                        '현재가': f"{int(last_close):,}",
                        '거래량비율': f"{vol_ratio:.1f}%",
                        '테마1': row[2].strip() if len(row) > 2 else "",
                        '테마2': row[3].strip() if len(row) > 3 else "",
                        '테마3': row[4].strip() if len(row) > 4 else ""
                    })
            except Exception:
                continue

        status_text.empty()
        progress_bar.empty()

        if matched:
            res_df = pd.DataFrame(matched)
            
            for t in ['테마1', '테마2', '테마3']:
                counts = res_df[res_df[t] != ''][t].value_counts()
                res_df[f'{t}_빈도'] = res_df[t].map(counts).fillna(0)
            
            res_df = res_df.sort_values(
                by=
