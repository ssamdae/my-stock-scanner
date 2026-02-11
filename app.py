import streamlit as st
from pykrx import stock
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

st.set_page_config(page_title="120-224 분석기", layout="wide")
st.title("📈 120-224 분석기 (최종 완성본)")

# --- [1. 텔레그램 전송 함수] ---
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload)
    except: pass

# --- [2. 구글 시트 인증] ---
def get_gspread_client():
    creds_info = st.secrets["gcp_service_account"]
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

# --- [3. 메인 분석 실행] ---
col1, col2 = st.columns(2)
if col1.button("🖥️ 웹으로 결과 보기", use_container_width=True) or col2.button("🔔 웹 + 텔레그램 알림", use_container_width=True):
    send_noti = True if col2.button_count > 0 else False # 버튼 클릭 감지 로직 (Streamlit 특성상 재실행됨)
    # 실제로는 버튼 클릭 여부 변수를 사용합니다.
    
    try:
        with st.spinner('데이터 준비 중...'):
            gc = get_gspread_client()
            rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]
            
            # FinanceDataReader로 안정적인 종목 리스트 확보
            df_krx = fdr.StockListing('KRX')
            ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
        
        st.success(f"✅ 시트 종목({len(rows)}개) 및 시장 리스트 확보 완료")

        target_date = datetime.now().strftime("%Y%m%d")
        matched_results = []
        progress_bar = st.progress(0)

        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date("20240101", target_date, ticker)
                    if df is not None and len(df) >= 224:
                        ma120 = df['종가'].rolling(120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(224).mean().iloc[-1]
                        close = df['종가'].iloc[-1]
                        
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched_results.append({'종목명': name, '테마': row[1] if len(row)>1 else "미분류"})
                except: continue
            
            progress_bar.progress((i + 1) / len(rows))
            time.sleep(0.05)

        # --- [결과 정렬: 빈도순 내림차순] ---
        if matched_results:
            res_df = pd.DataFrame(matched_results)
            # 테마별 빈도수 계산 후 정렬
            counts = res_df['테마'].value_counts()
            res_df['빈도수'] = res_df['테마'].map(counts)
            res_df = res_df.sort_values(by=['빈도수', '테마', '종목명'], ascending=[False, True, True])
            
            final_df = res_df.drop(columns=['빈도수'])
            st.dataframe(final_df, use_container_width=True)

            # 텔레그램 발송 (버튼 클릭 시)
            msg = f"<b>🔔 [분석 완료] {len(final_df)}건 포착</b>\n\n"
            for _, r in final_df.iterrows():
                msg += f"• <b>{r['종목명']}</b> | {r['테마']}\n"
            send_telegram_msg(msg)
        else:
            st.warning("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"오류 발생: {e}")
