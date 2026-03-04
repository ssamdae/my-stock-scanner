import streamlit as st
from pykrx import stock
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
                
                # 2. pykrx 실패 시 yfinance 백업
                if df is None or df.empty or len(df) < 224:
                    for suffix in [".KS", ".KQ"]:
                        df_yf = yf.download(ticker + suffix, start=(datetime.now() - timedelta(days=450)), end=datetime.now(), progress=False, show_errors=False)
                        if not df_yf.empty and len(df_yf) >= 224:
                            df = df_yf.rename(columns={'Close': '종가'})
                            break

                if df is not None and not df.empty and len(df) >= 224:
                    if isinstance(df['종가'], pd.DataFrame):
                        close_series = df['종가'].iloc[:, 0]
                    else:
                        close_series = df['종가']

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
                
                time.sleep(1.0) # Rate Limit 방지
                
            except Exception:
                continue

        status_text.empty()

        if matched:
            res_df = pd.DataFrame(matched)
            
            # [수정] 테마1, 테마2, 테마3 각각의 빈도 계산
            for t in ['테마1', '테마2', '테마3']:
                counts = res_df[res_df[t] != ''][t].value_counts()
                res_df[f'{t}_빈도'] = res_df[t].map(counts).fillna(0)
            
            # [수정] 정렬 로직: 테마1 빈도 -> 테마2 빈도 -> 테마3 빈도 -> 종목명
            res_df = res_df.sort_values(
                by=['테마1_빈도', '테마2_빈도', '테마3_빈도', '종목명'], 
                ascending=[False, False, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}개의 종목 발견 (기준일: {end_date})")
            
            # 웹 화면 표시 (인덱스 제거 및 테마만 표시)
            display_df = res_df[['종목명', '테마1', '테마2', '테마3']]
            st.dataframe(display_df, width='stretch', hide_index=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 포착: {end_date}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                
                for _, r in res_df.iterrows():
                    # [수정] 테마 1, 2, 3 중 데이터가 있는 것만 합치기
                    themes = [r['테마1'], r['테마2'], r['테마3']]
                    theme_str = ", ".join([t for t in themes if t.strip()])
                    
                    msg += f"• <b>{r['종목명']}</b> | {theme_str}\n"
                
                if send_telegram_msg(msg):
                    st.toast("텔레그램 전송 완료!")
                else:
                    st.error("텔레그램 전송에 실패했습니다.")
        else:
            st.warning("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"❌ 시스템 오류: {str(e)}")
