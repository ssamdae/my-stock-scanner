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
        with st.spinner('데이터 소스를 연결 중입니다...'):
            # [A] 구글 시트 연결
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            sheet_data = gc.open("관심종목").get_worksheet(0).get_all_values()
            
            if len(sheet_data) <= 1:
                st.warning("분석할 종목 데이터가 없습니다.")
                st.stop()
            rows = sheet_data[1:]

            # [B] 티커 맵 구성 (차단 회피 및 다중 백업)
            ticker_map = {}
            valid_date = ""
            found_market_day = False

            # 시도 1: pykrx (가장 정확하지만 차단에 취약)
            try:
                for i in range(10):
                    d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                    temp = stock.get_market_ticker_list(date=d, market="ALL")
                    if temp and len(temp) > 500:
                        ticker_map = {stock.get_market_ticker_name(t): t for t in temp}
                        valid_date = d
                        found_market_day = True
                        break
                time.sleep(0.5) 
            except: pass

            # 시도 2: FinanceDataReader (pykrx 실패 시)
            if not found_market_day:
                try:
                    df_krx = fdr.StockListing('KRX')
                    if df_krx is not None and not df_krx.empty:
                        ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
                        valid_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
                        found_market_day = True
                        st.info("💡 보조 데이터 서버(FDR)를 통해 종목을 불러왔습니다.")
                except: pass

            if not found_market_day:
                st.error("⚠️ 데이터 서버(KRX/Naver)로부터 응답이 없습니다. (휴장일 혹은 IP 차단)")
                st.info("잠시 후 다시 시도하거나, 로컬 환경에서 실행해 보세요.")
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
                    # 데이터 로드 시 빈 값 여부를 엄격히 체크
                    df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                    
                    if df is not None and not df.empty and len(df) >= 224:
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
                    # 차단 방지를 위해 요청 간격을 조금 늘림 (0.05 -> 0.2)
                    time.sleep(0.2) 
                except Exception:
                    continue 

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
                    st.error("텔레그램 전송에 실패했습니다.")
        else:
            st.warning(f"조건에 맞는 종목이 없습니다. (기준일: {valid_date})")

    except Exception as e:
        error_msg = str(e)
        st.error(f"오류 발생: {error_msg}")
        # 오류가 'Expecting value'인 경우 사용자에게 상황 설명 추가
        if "Expecting value" in error_msg:
            st.info("💡 현재 데이터 서버에서 올바른 값을 보내주지 않고 있습니다. 주로 휴장일이거나 서버 IP가 일시 차단되었을 때 발생합니다.")
        
        if btn_tele:
            send_telegram_msg(f"⚠️ [스캐너 에러]\n{error_msg[:100]}")
