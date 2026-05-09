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
st.set_page_config(page_title="돌파", layout="wide")
st.title("🚀돌파")
st.markdown("""
- **돌파 조건**: 전일 종가가 120선/224선 아래에 있다가, 오늘 종가가 상단 이평선을 돌파
- **거래량 조건**: 현재 거래량이 전일 전체 거래량 대비 **200%(2배)** 이상 발생
---
""")

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
        # 이평선 계산을 위해 넉넉히 450일치 데이터 확보
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
                
                # 2. 백업: yfinance 시도 (pykrx 실패 시)
                if df is None or df.empty or len(df) < 224:
                    for suffix in [".KS", ".KQ"]:
                        df_yf = yf.download(ticker + suffix, start=(datetime.now() - timedelta(days=450)), end=datetime.now(), progress=False, show_errors=False)
                        if not df_yf.empty and len(df_yf) >= 224:
                            df = df_yf.rename(columns={'Close': '종가', 'Volume': '거래량'})
                            break

                if df is not None and not df.empty and len(df) >= 224:
                    # 데이터 컬럼 이름 통일 (yfinance는 영문일 수 있음)
                    if 'Volume' in df.columns: df = df.rename(columns={'Volume': '거래량'})
                    if 'Close' in df.columns: df = df.rename(columns={'Close': '종가'})

                    # 데이터 차원 보정 (Multi-index 대응)
                    close_series = df['종가'].iloc[:, 0] if isinstance(df['종가'], pd.DataFrame) else df['종가']
                    vol_series = df['거래량'].iloc[:, 0] if isinstance(df['거래량'], pd.DataFrame) else df['거래량']

                    # 이평선 계산
                    ma120 = close_series.rolling(120).mean().iloc[-1]
                    ma224 = close_series.rolling(224).mean().iloc[-1]
                    upper_ma = max(ma120, ma224) # 두 이평선 중 더 높은 선

                    # 가격 및 거래량 조건 확인
                    prev_close = close_series.iloc[-2]
                    last_close = close_series.iloc[-1]
                    
                    prev_vol = vol_series.iloc[-2]
                    last_vol = vol_series.iloc[-1]
                    vol_ratio = (last_vol / prev_vol * 100) if prev_vol > 0 else 0

                    # 최종 조건: 상단 이평선 돌파 AND 거래량 200% 이상
                    is_breakout = prev_close < upper_ma < last_close
                    is_vol_surge = vol_ratio >= 200

                    if is_breakout and is_vol_surge:
                        matched.append({
                            '종목명': name, 
                            '현재가': f"{int(last_close):,}",
                            '거래량비율': f"{vol_ratio:.1f}%",
                            '테마1': row[2].strip() if len(row) > 2 else "",
                            '테마2': row[3].strip() if len(row) > 3 else "",
                            '테마3': row[4].strip() if len(row) > 4 else ""
                        })
                
                time.sleep(0.5) # API 과부하 방지
                
            except Exception:
                continue

        status_text.empty()
        progress_bar.empty()

        if matched:
            res_df = pd.DataFrame(matched)
            
            # 테마 빈도 기반 정렬 로직 (기존 유지)
            for t in ['테마1', '테마2', '테마3']:
                counts = res_df[res_df[t] != ''][t].value_counts()
                res_df[f'{t}_빈도'] = res_df[t].map(counts).fillna(0)
            
            res_df = res_df.sort_values(
                by=['테마1_빈도', '테마1', '테마2_빈도', '테마2', '테마3_빈도', '종목명'], 
                ascending=[False, True, False, True, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}개의 돌파 종목 발견! (기준일: {end_date})")
            
            # 웹 화면 표시
            display_df = res_df[['종목명', '현재가', '거래량비율', '테마1', '테마2', '테마3']]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            if btn_tele:
                msg = f"<b>🚀 [강력 돌파 포착: {end_date}]</b>\n"
                msg += f"상단 이평선 돌파 + 거래량 200%↑\n"
                msg += f"총 <b>{len(res_df)}건</b>\n\n"
                
                for _, r in res_df.iterrows():
                    theme_list = [t for t in [r['테마1'], r['테마2'], r['테마3']] if t.strip()]
                    theme_str = ", ".join(theme_list)
                    msg += f"• <b>{r['종목명']}</b> (🔥{r['거래량비율']}) | {theme_str}\n"
                
                if send_telegram_msg(msg):
                    st.toast("텔레그램 전송 완료!")
                else:
                    st.error("텔레그램 전송 실패")
        else:
            st.warning("조건(돌파 + 거래량 2배)에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"❌ 시스템 오류: {str(e)}")
