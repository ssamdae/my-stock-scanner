import streamlit as st
from pykrx import stock
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 1. 페이지 설정 및 제목
st.set_page_config(page_title="120-224 스캐너", layout="wide")
st.title("📈 120-224 샌드위치 분석기 (가독성 최적화 버전)")

# 2. 텔레그램 전송 함수 (가격 정보 완전 제거)
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=5)
    except:
        pass

# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('데이터를 분석 중입니다...'):
            # [A] 구글 시트 데이터 로드
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]

            # [B] 이중 백업 로직 (안정적인 티커 확보)
            ticker_map = {}
            valid_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            
            try:
                for i in range(7):
                    d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                    temp_tickers = stock.get_market_ticker_list(date=d, market="ALL")
                    if temp_tickers:
                        ticker_map = {stock.get_market_ticker_name(t): t for t in temp_tickers}
                        valid_date = d
                        break
                
                if not ticker_map:
                    df_krx = fdr.StockListing('KRX')
                    ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
                    valid_date = datetime.now().strftime("%Y%m%d")
            except: pass

            if not ticker_map:
                st.error("❌ 종목 정보를 가져오지 못했습니다.")
                st.stop()

        # [C] 분석 루프
        matched = []
        progress = st.progress(0)
        start_date = (datetime.now() - timedelta(days=450)).strftime("%Y%m%d")

        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            progress.progress((i + 1) / len(rows))
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                    if len(df) >= 224:
                        ma120 = df['종가'].rolling(120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(224).mean().iloc[-1]
                        close = df['종가'].iloc[-1]

                        # 샌드위치 조건 체크
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            # '현재가' 데이터를 리스트에서 제외
                            matched.append({
                                '종목명': name, 
                                '테마1': row[1] if len(row) > 1 else "",
                                '테마2': row[2] if len(row) > 2 else "",
                                '테마3': row[3] if len(row) > 3 else "",
                                '120일선': int(ma120),
                                '224일선': int(ma224)
                            })
                    time.sleep(0.05)
                except: continue

        # [D] 결과 출력 및 정렬
        if matched:
            res_df = pd.DataFrame(matched)
            
            # 빈도수 계산 및 정렬
            f1, f2, f3 = res_df['테마1'].value_counts(), res_df['테마2'].value_counts(), res_df['테마3'].value_counts()
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df['빈도2'] = res_df['테마2'].map(f2).fillna(0)
            res_df['빈도3'] = res_df['테마3'].map(f3).fillna(0)
            
            res_df = res_df.sort_values(
                by=['빈도1', '빈도2', '빈도3', '테마1', '종목명'], 
                ascending=[False, False, False, True, True]
            ).drop(columns=['빈도1', '빈도2', '빈도3'])
            
            st.success(f"✅ 총 {len(res_df)}건 포착 (기준일: {valid_date})")
            st.dataframe(res_df, use_container_width=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 포착: {valid_date}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    themes = f"{r['테마1']}"
                    if r['테마2']: themes += f", {r['테마2']}"
                    if r['테마3']: themes += f", {r['테마3']}"
                    msg += f"• <b>{r['종목명']}</b> | {themes}\n"
                send_telegram_msg(msg)
        else:
            st.warning(f"조건에 맞는 종목이 없습니다. (기준일: {valid_date})")

    except Exception as e:
        st.error(f"오류 발생: {e}")
