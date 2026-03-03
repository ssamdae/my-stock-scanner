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
st.set_page_config(page_title="F열 자동 완성 스캐너", layout="wide")
st.title("🛡️ F열 티커 기록 & 120-224 스캐너")

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
btn_web = col1.button("🖥️ 분석 시작 (F열 자동 완성)", use_container_width=True)
btn_tele = col2.button("🔔 분석 + 텔레그램 알림", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('1단계: 구글 시트 연결 중...'):
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            worksheet = gc.open("관심종목").get_worksheet(0)
            all_data = worksheet.get_all_values()
            rows = all_data[1:] # 데이터 (제목 제외)

        # F열(인덱스 5)이 비어있는지 체크
        # 리스트 길이가 6보다 짧거나 6번째 항목이 비어있는 경우를 모두 확인합니다.
        needs_ticker = any(len(row) < 6 or not row[5] for row in rows)

        if needs_ticker:
            with st.spinner('2단계: 미싱 티커 찾는 중 (F열 업데이트 시도)...'):
                df_master = pd.DataFrame()
                urls = [
                    "https://raw.githubusercontent.com/FinanceData/KRX/main/KRX_list.csv",
                    "https://raw.githubusercontent.com/FinanceData/KRX/master/KRX_list.csv"
                ]
                for url in urls:
                    try:
                        df_master = pd.read_csv(url)
                        if not df_master.empty: break
                    except: continue
                
                if df_master.empty:
                    try: df_master = fdr.StockListing('KRX')
                    except:
                        st.error("종목 리스트 서버 접속 실패. F열에 직접 티커를 입력해 주세요.")
                        st.stop()

                # 매핑 사전 구축
                master_map = {}
                for _, r in df_master.iterrows():
                    code_val = r['Symbol'] if 'Symbol' in r else r['Code']
                    market_val = r['Market'] if 'Market' in r else ""
                    suffix = ".KS" if "KOSPI" in str(market_val).upper() else ".KQ"
                    master_map[r['Name'].strip()] = str(code_val).zfill(6) + suffix

                # 시트 업데이트 (티커가 없는 행만 F열에 기록)
                for i, row in enumerate(rows):
                    # F열(인덱스 5)이 비어있는지 확인
                    if len(row) < 6 or not row[5]:
                        name = row[0].strip()
                        new_ticker = master_map.get(name)
                        if new_ticker:
                            # 구글 시트의 F열(6번째 열)에 티커 기록
                            worksheet.update_cell(i + 2, 6, new_ticker)
                            
                            # 현재 메모리상의 rows 데이터도 분석을 위해 동기화
                            # 행 데이터가 F열까지 없을 경우 확장해줍니다.
                            while len(row) < 6:
                                row.append("")
                            row[5] = new_ticker
                            time.sleep(0.2) # Google API Quota 준수

        with st.spinner('3단계: 샌드위치 분석 중...'):
            matched = []
            progress = st.progress(0)
            
            for i, row in enumerate(rows):
                name = row[0].strip()
                # F열(인덱스 5)에서 티커를 읽어옵니다.
                ticker = row[5].strip() if len(row) > 5 else None
                
                progress.progress((i + 1) / len(rows))
                
                if ticker and ticker != "찾을 수 없음":
                    try:
                        df = yf.Ticker(ticker).history(period="1y")
                        if len(df) >= 224:
                            ma120 = df['Close'].rolling(120).mean().iloc[-1]
                            ma224 = df['Close'].rolling(224).mean().iloc[-1]
                            close = df['Close'].iloc[-1]

                            if (ma224 < close < ma120) or (ma120 < close < ma224):
                                matched.append({
                                    '종목명': name, 
                                    '티커': ticker,
                                    '테마1': row[1] if len(row) > 1 else "", # 필요시 테마 열 번호도 조정하세요
                                    '테마2': row[2] if len(row) > 2 else ""
                                })
                        time.sleep(0.2)
                    except: continue

        # 결과 출력
        if matched:
            res_df = pd.DataFrame(matched)
            st.success(f"✅ 총 {len(res_df)}건 발견!")
            st.dataframe(res_df, use_container_width=True)
            if btn_tele:
                msg = f"<b>🔔 분석 완료: {len(res_df)}건</b>"
                send_telegram_msg(msg)
        else:
            st.warning("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"오류 발생: {e}")
