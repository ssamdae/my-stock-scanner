import streamlit as st
from pykrx import stock
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 1. 페이지 설정 및 제목
st.set_page_config(page_title="120-224 스캐너", layout="wide")
st.title("📈 120-224 샌드위치 분석기 (이중 백업 버전)")

# 2. 텔레그램 전송 함수
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=5)
    except:
        pass

# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        # ---------------------------------------------------------
        # [해결 방법 B] 데이터 수집 이중화 및 종목 리스트 확보 단계
        # ---------------------------------------------------------
        with st.spinner('데이터 소스를 이중 점검하며 종목 정보를 불러오는 중...'):
            # [A] 구글 시트 데이터 로드
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]

            # [B] 이중 백업 로직: pykrx 실패 시 fdr로 시도
            ticker_map = {}
            # 기본 분석 기준일은 어제로 설정 (오늘 장 마감 전일 경우 대비)
            valid_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            
            try:
                # 1차 시도: pykrx로 최근 7일 중 데이터가 있는 날짜 찾기
                for i in range(7):
                    d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                    temp_tickers = stock.get_market_ticker_list(date=d, market="ALL")
                    if temp_tickers:
                        ticker_map = {stock.get_market_ticker_name(t): t for t in temp_tickers}
                        valid_date = d
                        break
                
                # 2차 시도: 만약 pykrx가 여전히 비어있다면 FinanceDataReader로 보완
                if not ticker_map:
                    df_krx = fdr.StockListing('KRX')
                    ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
                    valid_date = datetime.now().strftime("%Y%m%d")
                    
            except Exception as e:
                st.warning(f"⚠️ 일부 데이터 소스 접근 실패, 대안을 탐색합니다: {e}")

            if not ticker_map:
                st.error("❌ 모든 데이터 소스(pykrx, FDR)에서 종목 정보를 가져오지 못했습니다. 라이브러리 업데이트나 서버 상태 확인이 필요합니다.")
                st.stop()
        # ---------------------------------------------------------

        # [C] 분석 루프 시작
        matched = []
        error_logs = []
        progress = st.progress(0)
        start_date = (datetime.now() - timedelta(days=450)).strftime("%Y%m%d")

        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            progress.progress((i + 1) / len(rows))
            
            if ticker:
                try:
                    # 종가 데이터 호출
                    df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                    
                    if len(df) >= 224:
                        ma120 = df['종가'].rolling(120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(224).mean().iloc[-1]
                        close = df['종가'].iloc[-1]

                        # 샌드위치 조건 (120일선과 224일선 사이)
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                '종목명': name, 
                                '테마1': row[1] if len(row) > 1 else "미분류",
                                '현재가': int(close),
                                '120일선': int(ma120),
                                '224일선': int(ma224)
                            })
                    time.sleep(0.05)
                except Exception as e:
                    error_logs.append(f"❌ {name} 분석 중 오류: {e}")
                    continue

        # [D] 결과 출력
        if matched:
            res_df = pd.DataFrame(matched)
            counts = res_df['테마1'].value_counts()
            res_df['빈도수'] = res_df['테마1'].map(counts)
            res_df = res_df.sort_values(by=['빈도수', '테마1', '종목명'], ascending=[False, True, True]).drop(columns=['빈0수'])
            
            st.success(f"✅ 총 {len(res_df)}건 발견 (기준일: {valid_date})")
            st.dataframe(res_df, use_container_width=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 포착: {valid_date}]</b>\n총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    msg += f"• <b>{r['종목명']}</b> | {r['테마1']} ({r['현재가']:,}원)\n"
                send_telegram_msg(msg)
        else:
            st.warning(f"조건에 맞는 종목이 없습니다. (기준일: {valid_date})")

        if error_logs:
            with st.expander("🔍 상세 오류 로그"):
                for log in error_logs:
                    st.write(log)

    except Exception as e:
        st.error(f"시스템 전체 오류: {e}")
