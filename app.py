import streamlit as st
from pykrx import stock
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests
import yfinance as yf

# 1. 페이지 설정
st.set_page_config(page_title="120-224 스캐너 PRO", layout="wide")
st.title("📈 120-224 샌드위치 분석기")
st.markdown("---")

# 2. 텔레그램 전송 함수
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        response = requests.post(url, data=payload, timeout=5)
        return response.status_code == 200
    except Exception:
        return False

# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('구글 시트에서 데이터를 불러오는 중...'):
            # [A] 구글 시트 연결
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            
            # 시트 열기 (A: 티커, B: 종목명, C~E: 테마)
            sheet = gc.open("관심종목").get_worksheet(0)
            sheet_data = sheet.get_all_values()
            
            if len(sheet_data) <= 1:
                st.warning("분석할 종목 데이터가 없습니다. 시트를 확인해주세요.")
                st.stop()
            
            rows = sheet_data[1:]  # 헤더 제외

        # [B] 분석 설정
        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 날짜 설정
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=450)).strftime("%Y%m%d")

        # [C] 종목 분석 루프
        for i, row in enumerate(rows):
            if not row or not row[0]: continue
            
            ticker = row[0].strip() # A열: 티커
            name = row[1].strip()   # B열: 종목명
            
            progress_bar.progress((i + 1) / len(rows))
            status_text.text(f"분석 중: {name} ({ticker})")
            
            try:
                # 1차 시도: pykrx
                df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
                
                # 2차 시도 (백업): yfinance
                if df is None or df.empty or len(df) < 224:
                    yf_ticker = f"{ticker}.KS" if int(ticker[0]) < 5 else f"{ticker}.KQ"
                    df_yf = yf.download(yf_ticker, start=(datetime.now() - timedelta(days=450)), end=datetime.now(), progress=False)
                    if not df_yf.empty:
                        df = df_yf.rename(columns={'Close': '종가'})

                if df is not None and not df.empty and len(df) >= 224:
                    ma120 = df['종가'].rolling(120).mean().iloc[-1]
                    ma224 = df['종가'].rolling(224).mean().iloc[-1]
                    close = df['종가'].iloc[-1]

                    # 샌드위치 조건 판별
                    if (ma224 < close < ma120) or (ma120 < close < ma224):
                        matched.append({
                            '종목명': name, 
                            '티커': ticker,
                            '테마1': row[2].strip() if len(row) > 2 else "",
                            '테마2': row[3].strip() if len(row) > 3 else "",
                            '테마3': row[4].strip() if len(row) > 4 else ""
                        })
                
                time.sleep(0.1)
                
            except Exception:
                continue

        status_text.empty()

        # [D] 결과 분석 및 출력
        if matched:
            res_df = pd.DataFrame(matched)
            
            # 테마 빈도 계산 (정렬용)
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            f2 = res_df[res_df['테마2'] != '']['테마2'].value_counts()
            
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df['빈도2'] = res_df['테마2'].map(f2).fillna(0)
            
            # 빈도순 정렬
            res_df = res_df.sort_values(
                by=['빈도1', '테마1', '빈도2', '종목명'], 
                ascending=[False, True, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}개의 종목이 샌드위치 구간에 있습니다.")
            
            # [수정 포인트] 표시할 열 선택 및 인덱스 제거
            # 보여줄 열만 필터링: 종목명, 테마1, 테마2, 테마3
            display_df = res_df[['종목명', '테마1', '테마2', '테마3']]
            
            # hide_index=True 를 사용하여 첫 번째 숫자 열 삭제
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            # [E] 텔레그램 전송
            if btn_tele:
                msg = f"<b>🔔 [샌드위치 포착: {end_date}]</b>\n총 <b>{len(res_df)}건</b> 발견\n\n"
                for _, r in res_df.iterrows():
                    themes = f"#{r['테마1']}"
                    if r['테마2']: themes += f" #{r['테마2']}"
                    msg += f"• <b>{r['종목명']}</b> | {themes}\n"
                
                if send_telegram_msg(msg):
                    st.toast("텔레그램 메시지 전송 완료!")
                else:
                    st.error("텔레그램 전송 실패")
        else:
            st.warning(f"조건에 맞는 종목이 없습니다. (기준일: {end_date})")

    except Exception as e:
        st.error(f"❌ 오류 발생: {str(e)}")
