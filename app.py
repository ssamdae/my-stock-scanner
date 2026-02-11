import streamlit as st
from pykrx import stock
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# --- [1. 페이지 설정 및 제목] ---
st.set_page_config(page_title="120-224 분석기", layout="wide")
st.title("📈 120-224 분석기 (최종 완료본)")

# --- [2. 텔레그램 전송 함수] ---
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload)
    except: pass

# --- [3. 구글 시트 인증 함수] ---
def get_gspread_client():
    creds_info = st.secrets["gcp_service_account"]
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

# --- [4. 화면 레이아웃 및 버튼 처리] ---
col1, col2 = st.columns(2)
# TypeError를 유발하던 코드를 표준 Streamlit 버튼 방식으로 수정했습니다.
btn_web_only = col1.button("🖥️ 웹으로만 결과 보기", use_container_width=True)
btn_with_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web_only or btn_with_tele:
    try:
        with st.spinner('데이터 준비 중...'):
            # 구글 시트 연결
            gc = get_gspread_client()
            spreadsheet = gc.open("관심종목")
            worksheet = spreadsheet.get_worksheet(0)
            rows = worksheet.get_all_values()[1:]
            
            # KRX 서버 에러를 방지하기 위해 FinanceDataReader를 메인으로 사용합니다.
            df_krx = fdr.StockListing('KRX')
            ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
        
        st.info(f"✅ 구글 시트에서 {len(rows)}개 종목을 가져왔습니다.")

        # 날짜 설정 (최근 영업일 기준)
        target_date = datetime.now().strftime("%Y%m%d")
        matched_results = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        # 분석 루프
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            status_text.text(f"분석 중: {name} ({i+1}/{len(rows)})")
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date("20240101", target_date, ticker)
                    if df is not None and not df.empty and len(df) >= 224:
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        current_close = df['종가'].iloc[-1]
                        
                        if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                            matched_results.append({
                                '종목명': name,
                                '테마1': row[1] if len(row) > 1 else "미분류",
                                '테마2': row[2] if len(row) > 2 else "",
                                '테마3': row[3] if len(row) > 3 else ""
                            })
                except: continue
            
            progress_bar.progress((i + 1) / len(rows))
            time.sleep(0.01)

        status_text.empty()
        
        # --- [5. 결과 처리 및 빈도순 정렬] ---
        if matched_results:
            res_df = pd.DataFrame(matched_results)
            
            # 테마1 기준으로 빈도수 계산
            theme_counts = res_df['테마1'].value_counts()
            res_df['빈도수'] = res_df['테마1'].map(theme_counts)
            
            # 정렬 순서: 1. 빈도수(내림차순) -> 2. 테마명(오름차순) -> 3. 종목명(오름차순)
            res_df = res_df.sort_values(by=['빈도수', '테마1', '종목명'], ascending=[False, True, True])
            
            # 출력용 데이터프레임 정리
            final_df = res_df.drop(columns=['빈도수'])
            
            st.success(f"✅ 분석 완료! 총 {len(final_df)}건 발견.")
            st.dataframe(final_df, use_container_width=True)
            
            # 텔레그램 전송
            if btn_with_tele:
                msg = f"<b>🔔 [분석 완료]</b>\n포착된 종목: <b>{len(final_df)}건</b>\n\n"
                for _, r in final_df.iterrows():
                    msg += f"• <b>{r['종목명']}</b> | {r['테마1']}\n"
                send_telegram_msg(msg)
                st.toast("✅ 텔레그램으로 전송되었습니다.")
        else:
            st.warning("⚠️ 현재 조건에 맞는 종목이 없습니다.")
            
    except Exception as e:
        st.error(f"시스템 오류 발생: {e}")
