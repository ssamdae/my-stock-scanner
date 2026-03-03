import streamlit as st
import yfinance as yf
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

            # [B] 티커 맵 구성 (다중 백업 및 404 방지)
            ticker_map = {}
            found_list = False

            # 시도 1: GitHub KRX 리스트 (Bypass 시도)
            try:
                url = "https://raw.githubusercontent.com/FinanceData/KRX/master/KRX_list.csv"
                df_krx = pd.read_csv(url)
                if not df_krx.empty:
                    found_list = True
                    st.info("✅ 외부 리스트를 통해 종목 정보를 불러왔습니다.")
            except:
                # 시도 2: fdr 기본 서버
                try:
                    df_krx = fdr.StockListing('KRX')
                    if not df_krx.empty:
                        found_list = True
                        st.info("✅ 공식 서버를 통해 종목 정보를 불러왔습니다.")
                except: pass

            if found_list:
                for _, r in df_krx.iterrows():
                    # 컬럼명 유연성 확보 (Symbol 또는 Code)
                    code_val = r['Symbol'] if 'Symbol' in r else r['Code']
                    market_val = r['Market'] if 'Market' in r else ""
                    
                    # yfinance 형식 지정 (.KS / .KQ)
                    suffix = ".KS" if "KOSPI" in str(market_val).upper() else ".KQ"
                    code = str(code_val).zfill(6)
                    ticker_map[r['Name']] = code + suffix
            else:
                st.error("종목 리스트를 가져오는 데 실패했습니다. 잠시 후 다시 시도해 주세요.")
                st.stop()

        # [C] 종목 분석 루프
        matched = []
        progress = st.progress(0)
        total_rows = len(rows)

        for i, row in enumerate(rows):
            if not row or not row[0]: continue
            
            name = row[0].strip()
            ticker = ticker_map.get(name)
            progress.progress((i + 1) / total_rows)
            
            if ticker:
                try:
                    # yfinance로 데이터 수집
                    stock_obj = yf.Ticker(ticker)
                    df = stock_obj.history(period="1y")
                    
                    if len(df) >= 224:
                        # 이동평균선 및 종가 추출
                        ma120 = df['Close'].rolling(120).mean().iloc[-1]
                        ma224 = df['Close'].rolling(224).mean().iloc[-1]
                        close = df['Close'].iloc[-1]

                        # 120선과 224선 사이 샌드위치 조건
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                '종목명': name, 
                                '티커': ticker,
                                '테마1': row[1].strip() if len(row) > 1 else "",
                                '테마2': row[2].strip() if len(row) > 2 else "",
                                '테마3': row[3].strip() if len(row) > 3 else ""
                            })
                    time.sleep(0.3) # 차단 방지를 위한 지연
                except:
                    continue

        # [D] 결과 정렬 및 출력
        if matched:
            res_df = pd.DataFrame(matched)
            
            # 테마 빈도 계산 및 정렬 로직
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            f2 = res_df[res_df['테마2'] != '']['테마2'].value_counts()
            f3 = res_df[res_df['테마3'] != '']['테마3'].value_counts()
            
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df['빈도2'] = res_df['테마2'].map(f2).fillna(0)
            res_df['빈도3'] = res_df['테마3'].map(f3).fillna(0)
            
            # 빈도수 높은 순 정렬
            res_df = res_df.sort_values(
                by=['빈도1', '테마1', '빈도2', '테마2', '빈도3', '종목명'], 
                ascending=[False, True, False, True, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}개 종목이 조건에 맞습니다.")
            display_df = res_df.drop(columns=['티커', '빈도1', '빈도2', '빈도3'])
            st.dataframe(display_df, use_container_width=True)

            if btn_tele:
                today_str = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")
                msg = f"<b>🔔 [120-224 샌드위치: {today_str}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    themes = f"{r['테마1']}"
                    if r['테마2']: themes += f", {r['테마2']}"
                    if r['테마3']: themes += f", {r['테마3']}"
                    msg += f"• <b>{r['종목명']}</b> | {themes}\n"
                send_telegram_msg(msg)
                st.toast("텔레그램 알림을 보냈습니다.")
        else:
            st.warning("조건을 만족하는 종목이 없습니다.")

    except Exception as e:
        st.error(f"오류 발생: {e}")
