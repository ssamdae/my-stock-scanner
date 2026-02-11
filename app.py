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
st.title("📈 관심종목 분석기 (진단 모드)")

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

# --- [4. 화면 레이아웃] ---
col1, col2 = st.columns(2)
btn_web_only = col1.button("🖥️ 웹으로만 결과 보기", use_container_width=True)
btn_with_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

# --- [5. 메인 분석 로직] ---
if btn_web_only or btn_with_tele:
    send_notification = True if btn_with_tele else False
    
    try:
        with st.spinner('구글 시트 로딩 중...'):
            gc = get_gspread_client()
            spreadsheet = gc.open("관심종목")
            worksheet = spreadsheet.get_worksheet(0)
            rows = worksheet.get_all_values()[1:]
        
        st.info(f"✅ 구글 시트에서 {len(rows)}개 종목을 가져왔습니다.")

        # [날짜 및 티커 리스트 확보 로직 강화]
        today = datetime.now().strftime("%Y%m%d")
        all_tickers = []
        
        # 오늘 날짜 리스트 시도, 실패 시 최근 5일 중 데이터 있는 날 찾기
        for i in range(5):
            target_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            all_tickers = stock.get_market_ticker_list(target_date, market="ALL")
            if all_tickers:
                st.success(f"📅 분석 기준일: {target_date} (시장 종목 {len(all_tickers)}개 확인)")
                break
        
        if not all_tickers:
            st.error("❌ 시장 종목 리스트를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.")
            st.stop()

        ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}
        matched_results = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()

        # 분석 루프
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            # 진행 상태 표시
            status_text.text(f"분석 중: {name} ({i+1}/{len(rows)})")
            
            if ticker:
                try:
                    # 데이터 확보 시도
                    df = stock.get_market_ohlcv_by_date("20240101", target_date, ticker)
                    if df is not None and not df.empty and len(df) >= 224:
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        current_close = df['종가'].iloc[-1]
                        
                        if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                            theme1 = row[1] if len(row) > 1 else "미지정"
                            matched_results.append([name, theme1, row[2] if len(row)>2 else "", row[3] if len(row)>3 else ""])
                except Exception as e:
                    continue
            
            progress_bar.progress((i + 1) / len(rows))
            # 너무 빨리 넘어가면 서버 부하가 걸리므로 미세한 지연 추가
            time.sleep(0.02)

        status_text.empty()
        
        # 결과 처리
        if matched_results:
            res_df = pd.DataFrame(matched_results, columns=["종목명", "테마1", "테마2", "테마3"])
            # (기존 정렬 로직 생략 - 필요시 추가 가능)
            st.success(f"✅ 분석 완료! 총 {len(matched_results)}건 발견.")
            st.dataframe(res_df, use_container_width=True)
            
            if send_notification:
                msg = f"<b>🔔 [분석 완료]</b>\n총 {len(matched_results)}건 포착되었습니다."
                send_telegram_msg(msg)
        else:
            st.warning("⚠️ 분석 결과 조건에 맞는 종목이 없습니다.")
            if send_notification:
                send_telegram_msg("✅ 분석 완료: 조건 만족 종목 없음")
            
    except Exception as e:
        st.error(f"시스템 오류: {e}")
