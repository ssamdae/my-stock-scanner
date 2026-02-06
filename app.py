import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime
import time
from tqdm import tqdm
import gspread
from google.oauth2.service_account import Credentials

# --- [페이지 설정] ---
st.set_page_config(page_title="주식 샌드위치 스캐너", layout="wide")
st.title("📈 내 관심종목 이동평균선 분석기")
st.write("120일선과 224일선 사이에 위치한 종목을 실시간으로 찾아냅니다.")

# --- [구글 시트 인증 함수] ---
# 서버에서는 코랩처럼 브라우저 로그인이 안 되므로 '서비스 계정' 키를 사용해야 합니다.
def get_gspread_client():
    # Streamlit Secrets에서 보안 정보를 가져옵니다.
    creds_info = st.secrets["gcp_service_account"]
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def pad_korean(text, width):
    if text is None: text = ""
    actual_len = sum(2 if ord(c) > 127 else 1 for c in text)
    return text + ' ' * max(0, width - actual_len)

# --- [메인 로직] ---
if st.button("🔍 분석 시작하기"):
    try:
        gc = get_gspread_client()
        # 시트 이름은 '내관심종목'으로 가정합니다.
        spreadsheet = gc.open("관심종목")
        worksheet = spreadsheet.get_worksheet(0)
        all_data = worksheet.get_all_values()
        rows = all_data[1:]
        
        today = datetime.now().strftime("%Y%m%d")
        all_tickers = stock.get_market_ticker_list(today, market="ALL")
        ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}

        matched_results = []
        
        # Streamlit 전용 진행바
        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, row in enumerate(rows):
            name = row[0]
            t1, t2, t3 = (row[1:4] + ["", "", ""])[:3] # 데이터가 부족해도 오류 방지
            
            status_text.text(f"분석 중: {name}...")
            ticker = ticker_map.get(name)
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date("20240101", today, ticker)
                    if len(df) >= 224:
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        current_close = df['종가'].iloc[-1]
                        
                        if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                            matched_results.append([name, t1, t2, t3])
                except:
                    pass
            
            # 진행률 업데이트
            progress_bar.progress((i + 1) / len(rows))
            time.sleep(0.05)

        status_text.success(f"분석 완료! 총 {len(matched_results)}건의 종목을 찾았습니다.")

        # --- [결과 출력] ---
        if matched_results:
            # 정렬: 테마1 -> 테마2 -> 테마3 -> 종목명
            matched_results.sort(key=lambda x: (x[1], x[2], x[3], x[0]))
            
            # 데이터프레임으로 변환하여 표로 출력
            res_df = pd.DataFrame(matched_results, columns=["종목명", "테마1", "테마2", "테마3"])
            st.table(res_df) # 모바일에서 가독성이 좋은 표 형태
        else:
            st.warning("조건에 부합하는 종목이 없습니다.")
            
    except Exception as e:

        st.error(f"오류 발생: {e}")
