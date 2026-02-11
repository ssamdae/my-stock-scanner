import streamlit as st
from pykrx import stock
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 페이지 설정
st.set_page_config(page_title="120-224 스캐너", layout="wide")
st.title("📈 120-224 샌드위치 분석기 (오류 수정 버전)")

def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=5)
    except: pass

col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('종목 정보를 불러오는 중...'):
            # 1. 구글 시트 데이터 로드
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
            gc = gspread.authorize(creds)
            rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]

            # 2. [수정] KRX 종목 리스트 확보 (FDR 대신 pykrx 사용으로 안정성 확보)
            try:
                # KOSPI, KOSDAQ 종목 리스트 결합
                tickers = stock.get_market_ticker_list(market="ALL")
                ticker_map = {stock.get_market_ticker_name(t): t for t in tickers}
            except Exception as e:
                st.error(f"종목 리스트 확보 실패 (pykrx): {e}")
                st.stop()

        matched = []
        progress = st.progress(0)
        target_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=450)).strftime("%Y%m%d")

        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            progress.progress((i + 1) / len(rows))
            
            if ticker:
                try:
                    # 데이터 호출
                    df = stock.get_market_ohlcv_by_date(start_date, target_date, ticker)
                    
                    if len(df) >= 224:
                        ma120 = df['종가'].rolling(120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(224).mean().iloc[-1]
                        close = df['종가'].iloc[-1]

                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                '종목명': name, 
                                '테마1': row[1] if len(row) > 1 else "미분류",
                                '현재가': int(close)
                            })
                    time.sleep(0.05) # 서버 부하 방지
                except: continue

        # 결과 출력 로직
        if matched:
            res_df = pd.DataFrame(matched)
            counts = res_df['테마1'].value_counts()
            res_df['빈도수'] = res_df['테마1'].map(counts)
            res_df = res_df.sort_values(by=['빈도수', '테마1', '종목명'], ascending=[False, True, True]).drop(columns=['빈도수'])
            
            st.success(f"✅ 총 {len(res_df)}건 발견")
            st.dataframe(res_df, use_container_width=True)

            if btn_tele:
                msg = f"<b>🔔 [분석 완료]</b>\n포착된 종목: <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    msg += f"• <b>{r['종목명']}</b> | {r['테마1']}\n"
                send_telegram_msg(msg)
        else:
            st.warning("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"시스템 전체 오류: {e}")
