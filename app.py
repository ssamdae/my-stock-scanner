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
            # [A] 구글 시트 연결 및 데이터 로드
            try:
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
            except Exception as ge:
                st.error(f"구글 시트 로드 실패: {ge}")
                st.stop()

            # [B] 티커 맵 구성 (재시도 및 백업 로직 포함)
            ticker_map = {}
            valid_date = datetime.now().strftime("%Y%m%d")
            found_market_day = False

            # 1단계: pykrx로 최근 영업일 찾기 (최대 3회 재시도)
            for attempt in range(3):
                for i in range(10):
                    d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                    try:
                        temp = stock.get_market_ticker_list(date=d, market="ALL")
                        if temp and len(temp) > 500: # 유의미한 수량의 종목이 있을 때만 인정
                            ticker_map = {stock.get_market_ticker_name(t): t for t in temp}
                            valid_date = d
                            found_market_day = True
                            break
                    except:
                        continue
                if found_market_day: break
                time.sleep(1.5) # 실패 시 잠시 대기 후 재시도

            # 2단계: pykrx 실패 시 FinanceDataReader로 대체
            if not found_market_day or not ticker_map:
                st.info("Pykrx 데이터를 가져오지 못해 백업 서버(FDR)를 연결합니다...")
                try:
                    df_krx = fdr.StockListing('KRX')
                    # 종목명을 Key로, 종목코드를 Value로 맵핑
                    ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
                    # 날짜는 안전하게 전일로 설정
                    valid_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
                    found_market_day = True
                except Exception as fe:
                    st.error(f"모든 데이터 소스 연결 실패: {fe}")
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
                    # 데이터 로드 (Expecting value 오류 방지를 위해 결과 존재 여부 체크)
                    df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                    
                    if df is not None and not df.empty and len(df) >= 224:
                        ma120 = df['종가'].rolling(120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(224).mean().iloc[-1]
                        close = df['종가'].iloc[-1]

                        # 샌드위치 조건 체크
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                '종목명': name, 
                                '티커': ticker,
                                '테마1': row[1].strip() if len(row) > 1 else "",
                                '테마2': row[2].strip() if len(row) > 2 else "",
                                '테마3': row[3].strip() if len(row) > 3 else ""
                            })
                    time.sleep(0.05) # 서버 부하 방지용 짧은 대기
                except:
                    continue 

        # [D] 결과 출력 및 정렬
        if matched:
            res_df = pd.DataFrame(matched)
            
            # 빈도 계산 (실제 값이 있는 경우만)
            f1 = res_df[res_df['테마1'] != '']['테마1'].value_counts()
            f2 = res_df[res_df['테마2'] != '']['테마2'].value_counts()
            f3 = res_df[res_df['테마3'] != '']['테마3'].value_counts()
            
            res_df['빈도1'] = res_df['테마1'].map(f1).fillna(0)
            res_df['빈도2'] = res_df['테마2'].map(f2).fillna(0)
            res_df['빈도3'] = res_df['테마3'].map(f3).fillna(0)
            
            # 정렬 순서: 빈도1(내림) -> 테마1명 -> 빈도2(내림) -> 테마2명 -> 종목명
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
        # 전체 실행 과정 중 에러 발생 시 출력 및 텔레그램 알림 시도
        error_full_msg = f"❌ 스캐너 실행 중 오류 발생: {e}"
        st.error(error_full_msg)
        if btn_tele:
            send_telegram_msg(f"⚠️ [스캐너 에러 발생]\n{str(e)[:150]}")
