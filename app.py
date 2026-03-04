import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests
import yfinance as yf

# 1. 페이지 설정 (2026년 기준 최신 문법 반영)
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
btn_web = col1.button("🖥️ 웹으로 결과 보기", width='stretch')
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", width='stretch')

if btn_web or btn_tele:
    try:
        with st.spinner('구글 시트에서 데이터를 불러오는 중...'):
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            sheet = gc.open("관심종목").get_worksheet(0)
            sheet_data = sheet.get_all_values()
            
            if len(sheet_data) <= 1:
                st.warning("분석할 종목 데이터가 없습니다.")
                st.stop()
            rows = sheet_data[1:]

        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=450)).strftime("%Y%m%d")

        for i, row in enumerate(rows):
            if not row or not row[0]: continue
            
            ticker = row[0].strip()
            name = row[1].strip()
            
            progress_bar.progress((i + 1) / len(rows))
            status_text.text(f"분석 중: {name} ({ticker})")
            
            df = None
            try:
                # 1. pykrx 시도
                df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
                
                # 2. 백업: yfinance 시도
                if df is None or df.empty or len(df) < 224:
                    for suffix in [".KS", ".KQ"]:
                        df_yf = yf.download(ticker + suffix, start=(datetime.now() - timedelta(days=450)), end=datetime.now(), progress=False, show_errors=False)
                        if not df_yf.empty and len(df_yf) >= 224:
                            df = df_yf.rename(columns={'Close': '종가'})
                            break

                if df is not None and not df.empty and len(df) >= 224:
                    # 데이터 차원 보정 (yfinance 대응)
                    close_series = df['종가'].iloc[:, 0] if isinstance(df['종가'], pd.DataFrame) else df['종가']

                    ma120 = close_series.rolling(120).mean().iloc[-1]
                    ma224 = close_series.rolling(224).mean().iloc[-1]
                    last_close = close_series.iloc[-1]

                    if (ma224 < last_close < ma120) or (ma120 < last_close < ma224):
                        matched.append({
                            '종목명': name, 
                            '테마1': row[2].strip() if len(row) > 2 else "",
                            '테마2': row[3].strip() if len(row) > 3 else "",
                            '테마3': row[4].strip() if len(row) > 4 else ""
                        })
                
                time.sleep(0.7) # 안정적인 수집을 위한 지연시간
                
            except Exception:
                continue

        status_text.empty()

        if matched:
            res_df = pd.DataFrame(matched)
            
            # --- 고도화된 정렬 로직 ---
            # 1. 각 테마별 전체 빈도 계산 (공백 제외)
            for t in ['테마1', '테마2', '테마3']:
                counts = res_df[res_df[t] != ''][t].value_counts()
                res_df[f'{t}_빈도'] = res_df[t].map(counts).fillna(0)
            
            # 2. 다중 조건 정렬 (핵심!)
            # 테마1빈도(내림) -> 테마1이름(오름) -> 테마2빈도(내림) -> 테마2이름(오름) -> 테마3빈도(내림) -> 종목명(오름)
            res_df = res_df.sort_values(
                by=['테마1_빈도', '테마1', '테마2_빈도', '테마2', '테마3_빈도', '종목명'], 
                ascending=[False, True, False, True, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}개의 종목 발견 (기준일: {end_date})")
            
            # 웹 화면 표시
            display_df = res_df[['종목명', '테마1', '테마2', '테마3']]
            st.dataframe(display_df, width='stretch', hide_index=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 포착: {end_date}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                
                for _, r in res_df.iterrows():
                    # 테마 1, 2, 3 전체 합치기 (비어있지 않은 것만)
                    theme_list = [t for t in [r['테마1'], r['테마2'], r['테마3']] if t.strip()]
                    theme_str = ", ".join(theme_list)
                    
                    msg += f"• <b>{r['종목명']}</b> | {theme_str}\n"
                
                if send_telegram_msg(msg):
                    st.toast("텔레그램 전송 완료!")
                else:
                    st.error("텔레그램 전송에 실패했습니다.")
        else:
            st.warning("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"❌ 시스템 오류: {str(e)}")
