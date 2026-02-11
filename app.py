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
st.title("📈 120-224 샌드위치 분석기")

# 2. 텔레그램 전송 함수
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        st.error(f"텔레그램 전송 실패: {e}")

# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('구글 시트 및 KRX 종목 정보를 불러오는 중...'):
            # 구글 시트 인증 및 로드
            creds = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            gc = gspread.authorize(creds)
            
            # 구글 시트 파일명 확인 필수
            sheet = gc.open("관심종목").get_worksheet(0)
            rows = sheet.get_all_values()[1:]
            
            # KRX 전체 종목 리스트 확보 (티커 매칭용)
            df_krx = fdr.StockListing('KRX')
            ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()

        if not rows:
            st.warning("구글 시트에 분석할 종목 데이터가 없습니다.")
            st.stop()

        matched = []
        error_logs = [] # 분석 실패 로그 저장용
        progress = st.progress(0)
        
        # 날짜 설정 (최근 400일 데이터를 가져와서 224일 이평선 확보)
        target_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")

        # 4. 분석 루프 시작
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            # 진행 상태 업데이트
            progress.progress((i + 1) / len(rows))
            
            # [체크 1] 티커 존재 여부
            if not ticker:
                error_logs.append(f"❓ 티커 미발견: {name}")
                continue

            try:
                # [체크 2] 주가 데이터 로드
                df = stock.get_market_ohlcv_by_date(start_date, target_date, ticker)
                
                if df.empty:
                    error_logs.append(f"🚫 데이터 없음: {name}({ticker})")
                    continue
                
                if len(df) < 224:
                    error_logs.append(f"📉 데이터 부족({len(df)}건): {name}")
                    continue

                # 핵심 지표 계산
                ma120 = df['종가'].rolling(120).mean().iloc[-1]
                ma224 = df['종가'].rolling(224).mean().iloc[-1]
                close = df['종가'].iloc[-1]

                # 샌드위치 조건 체크
                if (ma224 < close < ma120) or (ma120 < close < ma224):
                    matched.append({
                        '종목명': name, 
                        '테마1': row[1] if len(row) > 1 else "미분류",
                        '현재가': int(close),
                        '120일선': int(ma120),
                        '224일선': int(ma224)
                    })
                
                # API 과부하 방지 (매우 짧은 대기)
                time.sleep(0.05)

            except Exception as e:
                error_logs.append(f"❌ 오류 발생({name}): {e}")
                continue

        # 5. 결과 출력
        if matched:
            res_df = pd.DataFrame(matched)
            # 테마1 빈도순 정렬 로직
            counts = res_df['테마1'].value_counts()
            res_df['빈도수'] = res_df['테마1'].map(counts)
            res_df = res_df.sort_values(by=['빈도수', '테마1', '종목명'], ascending=[False, True, True]).drop(columns=['빈도수'])
            
            st.success(f"✅ 총 {len(res_df)}개의 샌드위치 종목을 발견했습니다!")
            st.dataframe(res_df, use_container_width=True)

            if btn_tele:
                msg = f"<b>🔔 [샌드위치 스캔 완료]</b>\n포착 종목: <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    msg += f"• <b>{r['종목명']}</b> ({r['현재가']:,}원) | {r['테마1']}\n"
                send_telegram_msg(msg)
        else:
            st.warning("조건에 맞는 종목이 오늘 한 건도 발견되지 않았습니다.")

        # 6. 디버깅 정보 (평소에는 접어둠)
        with st.expander("🔍 분석 프로세스 로그 확인 (오류 원인 파악)"):
            if error_logs:
                for log in error_logs:
                    st.write(log)
            else:
                st.write("모든 종목이 정상적으로 프로세스를 통과했습니다.")

    except Exception as e:
        st.error(f"시스템 전체 오류: {e}")
