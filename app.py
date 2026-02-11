import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# --- [1. 페이지 설정 및 제목] ---
st.set_page_config(page_title="주식 샌드위치 스캐너", layout="wide")
st.title("📈 관심종목 분석기")

# --- [2. 텔레그램 전송 함수] ---
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload)
    except Exception as e:
        st.error(f"텔레그램 전송 실패: {e}")

# --- [3. 구글 시트 인증 함수] ---
def get_gspread_client():
    creds_info = st.secrets["gcp_service_account"]
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

# --- [4. 화면 레이아웃: 버튼 배치] ---
col1, col2 = st.columns(2)
btn_web_only = col1.button("🖥️ 웹으로만 결과 보기", use_container_width=True)
btn_with_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

# --- [5. 메인 분석 로직 실행] ---
if btn_web_only or btn_with_tele:
    send_notification = True if btn_with_tele else False
    
    try:
        with st.spinner('구글 시트에서 종목을 불러오는 중...'):
            gc = get_gspread_client()
            # 시트 이름 수정: "관심종목"
            spreadsheet = gc.open("관심종목")
            worksheet = spreadsheet.get_worksheet(0)
            all_data = worksheet.get_all_values()
            rows = all_data[1:]
        
        today = datetime.now().strftime("%Y%m%d")
        
        try:
            all_tickers = stock.get_market_ticker_list(today, market="ALL")
        except:
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            all_tickers = stock.get_market_ticker_list(yesterday, market="ALL")
            
        ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}

        matched_results = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, row in enumerate(rows):
            name = row[0]
            t1, t2, t3 = (row[1:4] + ["", "", ""])[:3]
            
            status_text.text(f"분석 중: {name} ({i+1}/{len(rows)})")
            ticker = ticker_map.get(name)
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date("20240101", today, ticker)
                    if df is not None and not df.empty and len(df) >= 224:
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        current_close = df['종가'].iloc[-1]
                        
                        if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                            matched_results.append([name, t1, t2, t3])
                except:
                    continue
            
            progress_bar.progress((i + 1) / len(rows))
            time.sleep(0.05)

        status_text.empty()
        
        if matched_results:
            res_df = pd.DataFrame(matched_results, columns=["종목명", "테마1", "테마2", "테마3"])
            
            # 빈도수 정렬 로직
            t1_counts = res_df['테마1'].value_counts()
            t2_counts = res_df['테마2'].value_counts()
            t3_counts = res_df['테마3'].value_counts()
            res_df['t1_cnt'] = res_df['테마1'].map(t1_counts)
            res_df['t2_cnt'] = res_df['테마2'].map(t2_counts)
            res_df['t3_cnt'] = res_df['테마3'].map(t3_counts)
            
            res_df = res_df.sort_values(
                by=['t1_cnt', '테마1', 't2_cnt', '테마2', 't3_cnt', '테마3', '종목명'],
                ascending=[False, True, False, True, False, True, True]
            )
            final_df = res_df.drop(columns=['t1_cnt', 't2_cnt', 't3_cnt'])
            
            if send_notification:
                now_time = datetime.now().strftime("%Y-%m-%d %H:%M")
                msg = f"<b>🔔 [분석 완료] {now_time}</b>\n건수: <b>{len(final_df)}건</b>\n\n"
                for _, row in final_df.iterrows():
                    msg += f"• <b>{row['종목명']}</b> | {row['테마1']}\n"
                send_telegram_msg(msg)
                st.toast("✅ 텔레그램 알림 전송 완료!")

            st.success(f"분석 완료! 총 {len(final_df)}건 발견.")
            st.dataframe(final_df, use_container_width=True)
        else:
            if send_notification:
                send_telegram_msg(f"✅ {today} 분석 결과: 만족하는 종목이 없습니다.")
            st.warning("현재 조건에 맞는 종목이 없습니다.")
            
    except Exception as e:
        st.error(f"시스템 오류: {e}")
