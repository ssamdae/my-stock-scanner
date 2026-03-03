import streamlit as st
from pykrx import stock
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 1. 페이지 설정
st.set_page_config(page_title="120-224 스캐너", layout="wide")
st.title("📈 120-224 샌드위치 분석기")

# 2. 텔레그램 전송 함수
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        # 에러 발생 시 로그 확인을 위해 응답 체크 추가
        response = requests.post(url, data=payload, timeout=5)
        return response.status_code == 200
    except Exception as e:
        st.error(f"텔레그램 전송 실패: {e}")
        return False

# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('데이터를 분석 중입니다...'):
            # [A] 구글 시트 연결
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            sheet_data = gc.open("관심종목").get_worksheet(0).get_all_values()
            
            if len(sheet_data) <= 1:
                st.warning("구글 시트에 분석할 종목 데이터가 없습니다.")
                st.stop()
                
            rows = sheet_data[1:]

            # [B] 티커 맵 구성 (휴장일 대응)
            ticker_map = {}
            valid_date = datetime.now().strftime("%Y%m%d")
            
            # 최근 10일간을 뒤져서 가장 최근 장이 열렸던 날짜를 찾음
            found_market_day = False
            for i in range(10):
                d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                temp = stock.get_market_ticker_list(date=d, market="ALL")
                if temp:
                    ticker_map = {stock.get_market_ticker_name(t): t for t in temp}
                    valid_date = d
                    found_market_day = True
                    break
            
            if not found_market_day:
                st.error("최근 시장 데이터를 가져올 수 없습니다. API 연결 상태를 확인하세요.")
                st.stop()

        # [C] 종목 분석 루프
        matched = []
        progress = st.progress(0)
        start_date = (datetime.now() - timedelta(days=450)).strftime("%Y%m%d")

        for i, row in enumerate(rows):
            if not row or not row[0]: continue
            
            name = row[0].strip()
            ticker = ticker_map.get(name)
            progress.progress((i + 1) / len(rows))
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                    
                    # 데이터가 비어있는지 반드시 체크 (Expecting value 오류 방지)
                    if df is None or df.empty or len(df) < 224:
                        continue
                        
                    ma120 = df['종가'].rolling(120).mean().iloc[-1]
                    ma224 = df['종가'].rolling(224).mean().iloc[-1]
                    close = df['종가'].iloc[-1]

                    if (ma224 < close < ma120) or (ma120 < close < ma224):
                        matched.append({
                            '종목명': name, 
                            '티커': ticker,
                            '테마1': row[1].strip() if len(row) > 1 else "",
                            '테마2': row[2].strip() if len(row) > 2 else "",
                            '테마3': row[3].strip() if len(row) > 3 else ""
                        })
                    time.sleep(0.05)
                except Exception:
                    continue # 개별 종목 오류 시 건너뜀

        # [D] 결과 출력 및 정렬
        if matched:
            res_df = pd.DataFrame(matched)
            
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            f2 = res_df[res_df['테마2'] != '']['테마2'].value_counts()
            f3 = res_df[res_df['테마3'] != '']['테마3'].value_counts()
            
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df['빈도2'] = res_df['테마2'].map(f2).fillna(0)
            res_df['빈도3'] = res_df['테마3'].map(f3).fillna(0)
            
            res_df = res_df.sort_values(
                by=['빈도1', '테마1', '빈도2', '테마2', '빈도3', '종목명'], 
                ascending=[False, True, False, True, False, True]
            )
            
            st.success(f"✅ 총 {len(res_df)}건 발견 (기준일: {valid_date})")
            
            display_df = res_df.drop(columns=['티커', '빈도1', '빈도2', '빈도3'])
            st.dataframe(display_df, use_container_width=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 포착: {valid_date}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    themes = f"{r['테마1']}"
                    if r['테마2']: themes += f", {r['테마2']}"
                    if r['테마3']: themes += f", {r['테마3']}"
                    msg += f"• <b>{r['종목명']}</b> | {themes}\n"
                
                if send_telegram_msg(msg):
                    st.toast("텔레그램 메시지가 전송되었습니다!")
                else:
                    st.error("텔레그램 전송에 실패했습니다. 설정을 확인하세요.")
        else:
            st.warning(f"조건에 맞는 종목이 없습니다. (기준일: {valid_date})")

    except Exception as e:
        st.error(f"오류 발생: {e}")
        # 치명적 오류 발생 시 텔레그램으로도 알림 (선택 사항)
        if btn_tele:
            send_telegram_msg(f"⚠️ 스캐너 실행 중 오류 발생:\n{str(e)[:100]}")
