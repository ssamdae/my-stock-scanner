import streamlit as st
import yfinance as yf # yfinance 추가
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 1. 페이지 설정
st.set_page_config(page_title="120-224 스캐너", layout="wide")
st.title("📈 120-224 샌드위치 분석기")

# 2. 텔레그램 전송 함수
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=5)
    except: pass

# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('데이터를 분석 중입니다...'):
            # [A] 구글 시트 연결
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]

            # [B] 티커 맵 구성 (FDR 활용 - 종목 리스트 획득용)
            # yfinance는 '005930.KS' 형식을 사용하므로 변환이 필요합니다.
            try:
                df_krx = fdr.StockListing('KRX')
                ticker_map = {}
                for _, row_krx in df_krx.iterrows():
                    # 코스피는 .KS, 코스닥은 .KQ 접미사 추가
                    suffix = ".KS" if row_krx['Market'] == 'KOSPI' else ".KQ"
                    ticker_map[row_krx['Name']] = row_krx['Code'] + suffix
            except Exception as e:
                st.error(f"종목 리스트 로드 실패: {e}")
                st.stop()

        # [C] 종목 분석 루프
        matched = []
        progress = st.progress(0)
        
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            progress.progress((i + 1) / len(rows))
            
            if ticker:
                try:
                    # yfinance로 데이터 가져오기 (차단에 훨씬 강함)
                    # 120일, 224일 이평선을 위해 1년치 데이터 요청
                    stock_data = yf.Ticker(ticker)
                    df = stock_data.history(period="1y")
                    
                    if len(df) >= 224:
                        ma120 = df['Close'].rolling(120).mean().iloc[-1]
                        ma224 = df['Close'].rolling(224).mean().iloc[-1]
                        close = df['Close'].iloc[-1]

                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                '종목명': name, 
                                '테마1': row[1].strip() if len(row) > 1 else "",
                                '테마2': row[2].strip() if len(row) > 2 else "",
                                '테마3': row[3].strip() if len(row) > 3 else ""
                            })
                    # 요청 간격을 두어 안정성 확보
                    time.sleep(0.5)
                except:
                    continue # 에러 발생 시 해당 종목은 건너뛰고 계속 진행

        # [D] 결과 출력 및 정렬
        if matched:
            res_df = pd.DataFrame(matched)
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            f2 = res_df[res_df['테마2'] != '']['테마2'].value_counts()
            f3 = res_df[res_df['테마3'] != '']['테마3'].value_counts()
            
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df['빈도2'] = res_df['테마2'].map(f2).fillna(0)
            res_df['빈도3'] = res_df['테마3'].map(f3).fillna(0)
            
            res_df = res_df.sort_values(
                by=['빈도1', '테마1', '빈도2', '테마2', '빈도3', '종목명'], 
                ascending=[False, True, False, True, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}건 발견")
            st.dataframe(res_df.drop(columns=['빈도1', '빈도2', '빈도3']), use_container_width=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 분석 결과]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    themes = f"{r['테마1']}"
                    if r['테마2']: themes += f", {r['테마2']}"
                    if r['테마3']: themes += f", {r['테마3']}"
                    msg += f"• <b>{r['종목명']}</b> | {themes}\n"
                send_telegram_msg(msg)
                st.toast("텔레그램 전송 완료!")
        else:
            st.warning("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"오류 발생: {e}")
