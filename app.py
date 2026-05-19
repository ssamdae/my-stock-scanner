import streamlit as st
import pandas as pd
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. 페이지 설정 (심플하고 전문적인 레이아웃)
st.set_page_config(page_title="시장 전체 돌파 스캐너", layout="wide")
st.title("🚀 시장 전체 실시간 돌파 레이더")
st.caption("KOSPI/KOSDAQ 전 종목을 실시간으로 분석하여 120-224일 이평선 돌파 종목을 추출합니다.")

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

# 💡 네이버 금융에서 전체 종목 티커 수집 (KOSPI/KOSDAQ)
def get_all_tickers_naver():
    tickers = []
    # sosok 0: KOSPI, 1: KOSDAQ
    for sosok in [0, 1]:
        for page in range(1, 45): 
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, timeout=5)
                soup = BeautifulSoup(res.text, 'html.parser')
                links = soup.select('a.tltle')
                if not links: break
                for link in links:
                    href = link.get('href')
                    if href and 'code=' in href:
                        ticker = href.split('code=')[-1]
                        name = link.text.strip()
                        if "스팩" not in name and not name.endswith("우") and not name.endswith("우B"):
                            tickers.append((ticker, name))
            except Exception:
                continue
    return tickers

# 💡 개별 종목 분석 워커 함수
def analyze_single_stock_naver(item):
    ticker, name = item
    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={ticker}&timeframe=day&count=360&requestType=0"
    try:
        res = requests.get(url, timeout=2)
        if res.status_code != 200: return None
        root = ET.fromstring(res.text)
        items = root.findall('.//item')
        if len(items) < 224: return None
        data_list = []
        for item in items:
            data_str = item.get('data')
            parts = data_str.split('|')
            if len(parts) == 6:
                data_list.append({'Close': float(parts[4]), 'Volume': float(parts[5])})
        df_stock = pd.DataFrame(data_list)
        close_series = df_stock['Close']
        vol_series = df_stock['Volume']
        ma120 = close_series.rolling(120).mean().iloc[-1]
        ma224 = close_series.rolling(224).mean().iloc[-1]
        upper_ma = max(ma120, ma224)
        prev_close = close_series.iloc[-2]
        last_close = close_series.iloc[-1]
        prev_vol = vol_series.iloc[-2]
        last_vol = vol_series.iloc[-1]
        vol_ratio = (last_vol / prev_vol * 100) if prev_vol > 0 else 0
        if prev_close < upper_ma < last_close and vol_ratio >= 200:
            return {'종목명': name, '현재가': f"{int(last_close):,}", '거래량비율': round(float(vol_ratio), 1)}
    except Exception:
        pass
    return None

# 3. 분석 실행 UI
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 전 종목 스캔 (웹 결과)", use_container_width=True)
btn_tele = col2.button("🔔 전 종목 스캔 + 텔레그램 보고", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('📡 네이버 금융에서 전 종목 리스트 수집 중...'):
            market_tickers = get_all_tickers_naver()
        
        if not market_tickers:
            st.error("종목 리스트 수집에 실패했습니다.")
            st.stop()

        total_len = len(market_tickers)
        st.info(f"✅ 총 {total_len}개 종목 포착. 정밀 분석을 시작합니다. (약 30초 소요)")

        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 💡 초고속 병렬 연산 (ThreadPool 20개 가동)
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures
