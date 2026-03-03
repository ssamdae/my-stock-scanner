import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 1. 페이지 설정
st.set_page_config(page_title="자동 변환 스캐너", layout="wide")
st.title("🔄 실시간 변환 & 120-224 스캐너")

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
btn_web = col1.button("🖥️ 분석 시작 (자동 변환 포함)", use_container_width=True)
btn_tele = col2.button("🔔 분석 + 텔레그램 알림", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('1단계: 구글 시트에서 종목명 로드 중...'):
            # [A] 구글 시트 연결
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            worksheet = gc.open("관심종목").get_worksheet(0)
            rows = worksheet.get_all_values()[1:] # 제목행 제외

        with st.spinner('2단계: 종목명 -> 티커 실시간 변환 중...'):
            # [B] 컨버트 로직 (차단 방지를 위해 GitHub 미러 데이터 사용)
            # 이 주소는 KRX 공식 서버가 아니므로 Streamlit에서 차단되지 않습니다.
            ticker_url = "https://raw.githubusercontent.com/FinanceData/KRX/master/KRX_list.csv"
            df_master = pd.read_csv(ticker_url)
            
            # 마스터 리스트를 이용한 변환 사전(Dictionary) 생성
            master_map = {}
            for _, r in df_master.iterrows():
                suffix = ".KS" if r['Market'] == 'KOSPI' else ".KQ"
                code = str(r['Symbol']).zfill(6)
                master_map[r['Name'].strip()] = code + suffix

        with st.spinner('3단계: 샌드위치 조건 분석 중...'):
            matched = []
            progress = st.progress(0)
            
            for i, row in enumerate(rows):
                name = row[0].strip()
                ticker = master_map.get(name) # 실시간 매칭
                
                progress.progress((i + 1) / len(rows))
                
                if ticker:
                    try:
                        # yfinance 분석
                        stock_obj = yf.Ticker(ticker)
                        df = stock_obj.history(period="1y")
                        
                        if len(df) >= 224:
                            ma120 = df['Close'].rolling(120).mean().iloc[-1]
                            ma224 = df['Close'].rolling(224).mean().iloc[-1]
                            close = df['Close'].iloc[-1]

                            if (ma224 < close < ma120) or (ma120 < close < ma224):
                                matched.append({
                                    '종목명': name, 
                                    '티커': ticker,
                                    '테마1': row[1].strip() if len(row) > 1 else "",
                                    '테마2': row[2].strip() if len(row) > 2 else "",
                                    '테마3': row[3].strip() if len(row) > 3 else ""
                                })
                        time.sleep(0.3) # API 부하 방지
                    except: continue

        # [C] 결과 출력
        if matched:
            res_df = pd.DataFrame(matched)
            # 정렬 및 출력 (기존 로직 유지)
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df = res_df.sort_values(by=['빈도1', '종목명'], ascending=[False, True])
            
            st.success(f"✅ 총 {len(res_df)}건 발견!")
            st.dataframe(res_df.drop(columns=['빈도1']), use_container_width=True)

            if btn_tele:
                msg = f"<b>🔔 [자동 변환 스캔 완료]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    msg += f"• <b>{r['종목명']}</b> ({r['티커']}) | {r['테마1']}\n"
                send_telegram_msg(msg)
                st.toast("텔레그램 전송 완료!")
        else:
            st.warning("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"오류 발생: {e}")
        if "404" in str(e):
            st.info("💡 종목 리스트 서버(GitHub)에 일시적인 문제가 있습니다. 잠시 후 다시 시도해 주세요.")
