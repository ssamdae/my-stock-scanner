import streamlit as st
from pykrx import stock
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
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]

            ticker_map = {}
            valid_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            for i in range(7):
                d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                temp = stock.get_market_ticker_list(date=d, market="ALL")
                if temp:
                    ticker_map = {stock.get_market_ticker_name(t): t for t in temp}
                    valid_date = d
                    break
            
            if not ticker_map:
                df_krx = fdr.StockListing('KRX')
                ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()

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

                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                '종목명': name, 
                                '티커': ticker,
                                '테마1': row[1].strip() if len(row) > 1 else "",
                                '테마2': row[2].strip() if len(row) > 2 else "",
                                '테마3': row[3].strip() if len(row) > 3 else ""
                            })
                    time.sleep(0.05)
                except: continue

        # [D] 결과 출력 및 정렬 (수정된 로직)
        if matched:
            res_df = pd.DataFrame(matched)
            
            # 1. 실제 값이 있는 테마들만 빈도 계산 (빈 문자열 제외)
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            f2 = res_df[res_df['테마2'] != '']['테마2'].value_counts()
            f3 = res_df[res_df['테마3'] != '']['테마3'].value_counts()
            
            # 2. 빈도 컬럼 생성 (빈 문자열은 빈도를 0으로 처리)
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df['빈도2'] = res_df['테마2'].map(f2).fillna(0)
            res_df['빈도3'] = res_df['테마3'].map(f3).fillna(0)
            
            # 3. 계층적 정렬
            # - 빈도1 내림차순 -> 테마1 이름으로 그룹화
            # - 그 안에서 빈도2 내림차순 -> 테마2 이름으로 그룹화
            # - 그 안에서 빈도3 내림차순
            res_df = res_df.sort_values(
                by=['빈도1', '테마1', '빈도2', '테마2', '빈도3', '종목명'], 
                ascending=[False, True, False, True, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}건 발견 (기준일: {valid_date})")
            
            # 보조 컬럼 제외 후 출력
            display_df = res_df.drop(columns=['티커', '빈도1', '빈도2', '빈도3'])
            st.dataframe(display_df, use_container_width=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 포착: {valid_date}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    themes = f"{r['테마1']}"
                    if r['테마2']: themes += f", {r['테마2']}"
                    if r['테마3']: themes += f", {r['테마3']}"
                    msg += f"• <b>{r['종목명']}</b> | {themes}\n"
                send_telegram_msg(msg)
                st.toast("텔레그램 메시지가 전송되었습니다!")
        else:
            st.warning(f"조건에 맞는 종목이 없습니다. (기준일: {valid_date})")

    except Exception as e:
        st.error(f"오류 발생: {e}")
