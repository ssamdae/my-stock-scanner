import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 1. 페이지 설정
st.set_page_config(page_title="무적 스캐너", layout="wide")
st.title("🛡️ 서버 차단 우회형 120-224 스캐너")

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
btn_run = st.button("🚀 분석 시작 (F열에 티커가 있는 종목 우선)", use_container_width=True)

if btn_run:
    try:
        with st.spinner('1단계: 구글 시트 데이터 로드 중...'):
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            worksheet = gc.open("관심종목").get_worksheet(0)
            rows = worksheet.get_all_values()[1:] # 제목 제외

        # [A] 미싱 티커 자동 완성 시도 (실패해도 중단하지 않음)
        missing_rows = [i for i, r in enumerate(rows) if len(r) < 6 or not r[5]]
        
        if missing_rows:
            st.info(f"💡 {len(missing_rows)}개 종목의 티커가 F열에 없습니다. 자동 찾기를 시도합니다...")
            try:
                # 차단에 가장 강한 네이버 금융 경로 직접 활용 시도
                url = "https://raw.githubusercontent.com/FinanceData/KRX/main/KRX_list.csv"
                df_master = pd.read_csv(url)
                master_map = {r['Name']: str(r['Symbol']).zfill(6) + (".KS" if r['Market'] == 'KOSPI' else ".KQ") 
                              for _, r in df_master.iterrows()}
                
                updated_count = 0
                for idx in missing_rows:
                    name = rows[idx][0].strip()
                    new_ticker = master_map.get(name)
                    if new_ticker:
                        worksheet.update_cell(idx + 2, 6, new_ticker)
                        while len(rows[idx]) < 6: rows[idx].append("")
                        rows[idx][5] = new_ticker
                        updated_count += 1
                        time.sleep(0.2)
                st.success(f"✅ {updated_count}개 종목의 티커를 자동으로 채웠습니다!")
            except:
                st.warning("⚠️ 외부 서버 차단으로 인해 티커 자동 완성에 실패했습니다. 분석 가능한 종목만 먼저 진행합니다.")

        # [B] 실제 분석 루프
        matched = []
        progress = st.progress(0)
        
        # 티커가 있는 종목들만 필터링
        valid_rows = [r for r in rows if len(r) >= 6 and r[5] and ".K" in r[5]]
        
        if not valid_rows:
            st.error("❌ 분석할 티커가 하나도 없습니다. 구글 시트 F열에 '005930.KS'와 같이 티커를 입력해 주세요.")
        else:
            for i, row in enumerate(valid_rows):
                name, ticker = row[0], row[5]
                progress.progress((i + 1) / len(valid_rows))
                
                try:
                    # yfinance 데이터 호출 (이 과정은 차단될 확률이 매우 낮음)
                    df = yf.Ticker(ticker).history(period="1y")
                    if len(df) >= 224:
                        ma120 = df['Close'].rolling(120).mean().iloc[-1]
                        ma224 = df['Close'].rolling(224).mean().iloc[-1]
                        close = df['Close'].iloc[-1]

                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                '종목명': name, '티커': ticker,
                                '테마1': row[1] if len(row) > 1 else "",
                                '테마2': row[2] if len(row) > 2 else ""
                            })
                    time.sleep(0.2)
                except: continue

            # [C] 결과 출력
            if matched:
                res_df = pd.DataFrame(matched)
                st.success(f"🎯 총 {len(res_df)}개 종목 포착!")
                st.dataframe(res_df, use_container_width=True)
                
                # 텔레그램 전송
                msg = f"<b>🔔 120-224 분석 완료</b>\n포착: {len(res_df)}건"
                send_telegram_msg(msg)
            else:
                st.warning("🔍 조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"❌ 치명적 오류 발생: {e}")
