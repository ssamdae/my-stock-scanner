import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 1. 페이지 설정
st.set_page_config(page_title="120-224 스캐너", layout="wide")
st.title("📈 120-224 샌드위치 분석기 (안정화 버전)")

# 2. 텔레그램 전송 함수
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=5)
    except: pass

# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('종목 리스트를 불러오는 중...'):
            # [A] 구글 시트 연결
            try:
                creds = Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"], 
                    scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
                )
                gc = gspread.authorize(creds)
                rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]
            except Exception as e:
                st.error(f"구글 시트 연결 실패: {e}")
                st.stop()

            # [B] 티커 맵 구성 (KRX 직접 접속 대신 GitHub 미러 활용하여 차단 우회)
            ticker_map = {}
            try:
                # KRX 종목 리스트 미러 사이트 (차단되지 않음)
                url = "https://raw.githubusercontent.com/FinanceData/KRX/master/KRX_list.csv"
                df_krx = pd.read_csv(url)
                
                for _, r in df_krx.iterrows():
                    # 코스피/코스닥 구분하여 yfinance용 접미사 추가
                    suffix = ".KS" if r['Market'] == 'KOSPI' else ".KQ"
                    # 종목코드를 6자리 문자열로 맞춤 (예: 5930 -> 005930)
                    code = str(r['Symbol']).zfill(6)
                    ticker_map[r['Name']] = code + suffix
                
                st.info(f"✅ {len(ticker_map)}개 종목 정보를 성공적으로 로드했습니다.")
            except Exception as e:
                st.error(f"종목 리스트 로드 실패 (Bypass 실패): {e}")
                st.stop()

        # [C] 종목 분석 루프
        matched = []
        progress = st.progress(0)
        
        # 분석 진행
        total_rows = len(rows)
        for i, row in enumerate(rows):
            if not row or not row[0]: continue
            
            name = row[0].strip()
            ticker = ticker_map.get(name)
            progress.progress((i + 1) / total_rows)
            
            if ticker:
                try:
                    # yfinance로 1년치 데이터 호출 (차단에 강함)
                    stock_obj = yf.Ticker(ticker)
                    df = stock_obj.history(period="1y")
                    
                    if len(df) >= 224:
                        # 이평선 및 종가 계산 (yfinance는 Close 컬럼 사용)
                        ma120 = df['Close'].rolling(120).mean().iloc[-1]
                        ma224 = df['Close'].rolling(224).mean().iloc[-1]
                        close = df['Close'].iloc[-1]

                        # 샌드위치 조건 (120선과 224선 사이에 주가 위치)
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                '종목명': name, 
                                '테마1': row[1].strip() if len(row) > 1 else "",
                                '테마2': row[2].strip() if len(row) > 2 else "",
                                '테마3': row[3].strip() if len(row) > 3 else ""
                            })
                    # 서버 부하 방지를 위한 약간의 대기
                    time.sleep(0.3)
                except:
                    continue # 개별 종목 에러 시 건너뜀

        # [D] 결과 출력 및 정렬
        if matched:
            res_df = pd.DataFrame(matched)
            
            # 테마별 빈도 계산
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            f2 = res_df[res_df['테마2'] != '']['테마2'].value_counts()
            f3 = res_df[res_df['테마3'] != '']['테마3'].value_counts()
            
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df['빈도2'] = res_df['테마2'].map(f2).fillna(0)
            res_df['빈도3'] = res_df['테마3'].map(f3).fillna(0)
            
            # 빈도 및 테마명 기준 정렬
            res_df = res_df.sort_values(
                by=['빈도1', '테마1', '빈도2', '테마2', '빈도3', '종목명'], 
                ascending=[False, True, False, True, False, True]
            )
            
            st.success(f"🎊 총 {len(res_df)}개 종목 포착!")
            
            # 최종 화면 출력용 데이터프레임
            display_df = res_df.drop(columns=['빈도1', '빈도2', '빈도3'])
            st.dataframe(display_df, use_container_width=True)

            if btn_tele:
                # 현재 날짜 (KST 기준)
                today_str = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")
                msg = f"<b>🔔 [120-224 샌드위치: {today_str}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    themes = f"{r['테마1']}"
                    if r['테마2']: themes += f", {r['테마2']}"
                    if r['테마3']: themes += f", {r['테마3']}"
                    msg += f"• <b>{r['종목명']}</b> | {themes}\n"
                
                send_telegram_msg(msg)
                st.toast("🚀 텔레그램 메시지를 전송했습니다!")
        else:
            st.warning("조건에 만족하는 종목이 발견되지 않았습니다.")

    except Exception as e:
        st.error(f"⚠️ 시스템 오류 발생: {e}")
        if btn_tele:
            send_telegram_msg(f"❌ 스캐너 오류 발생:\n{str(e)[:100]}")
