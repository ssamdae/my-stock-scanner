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
# 최신 문법 적용: use_container_width -> width='stretch'
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
                
                # 2. pykrx 실패 시 yfinance 백업 (시장 자동 판별 로직 개선)
                if df is None or df.empty or len(df) < 224:
                    # 코스피/코스닥 둘 다 시도하여 데이터가 있는 쪽을 선택
                    for suffix in [".KS", ".KQ"]:
                        df_yf = yf.download(ticker + suffix, start=(datetime.now() - timedelta(days=450)), end=datetime.now(), progress=False, show_errors=False)
                        if not df_yf.empty and len(df_yf) >= 224:
                            df = df_yf.rename(columns={'Close': '종가'})
                            break

                if df is not None and not df.empty and len(df) >= 224:
                    # 종가 컬럼이 MultiIndex인 경우 처리 (yfinance 최신 버전 대응)
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
                            '테마3': row[4].strip() if len(row) > 4 else "",
                            'sort_key': row[2].strip() if len(row) > 2 else "" # 정렬용
                        })
                
                # [중요] Rate Limit 방지를 위해 대기 시간 조절 (0.1 -> 0.5)
                # 종목 수가 많다면 1.0까지 늘리는 것을 권장합니다.
                time.sleep(1.0)
                
            except Exception as e:
                continue

        status_text.empty()

        if matched:
            res_df = pd.DataFrame(matched)
            
            # 테마 빈도 계산 및 정렬
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            res_df['빈도'] = res_df['테마1'].map(f1).fillna(0)
            res_df = res_df.sort_values(by=['빈도', '테마1', '종목명'], ascending=[False, True, True])
            
            st.success(f"✅ 총 {len(res_df)}개의 종목 발견")
            
            # 요청사항: 종목명, 테마1, 테마2, 테마3만 표시 + 인덱스 삭제
            display_df = res_df[['종목명', '테마1', '테마2', '테마3']]
            st.dataframe(display_df, width='stretch', hide_index=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 포착: {end_date}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    msg += f"• <b>{r['종목명']}</b> | {r['테마1']}\n"
                send_telegram_msg(msg)
        else:
            st.warning("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"❌ 시스템 오류: {str(e)}")
