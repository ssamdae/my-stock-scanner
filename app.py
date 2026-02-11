import streamlit as st
from pykrx import stock
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# --- [1. 페이지 설정] ---
st.set_page_config(page_title="120-224 분석기", layout="wide")
st.title("📈 120-224 분석기 (최종 완성본)")

# --- [2. 텔레그램 전송 함수] ---
def send_telegram_msg(message):
    try:
        token = st.secrets["telegram"]["bot_token"]
        chat_id = st.secrets["telegram"]["chat_id"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=payload)
    except:
        pass

# --- [3. 구글 시트 인증] ---
def get_gspread_client():
    creds_info = st.secrets["gcp_service_account"]
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

# --- [4. 화면 레이아웃 및 버튼 처리] ---
col1, col2 = st.columns(2)
# 버튼 클릭 상태를 변수에 직접 저장하여 TypeError를 방지합니다.
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    send_noti = True if btn_tele else False
    
    try:
        with st.spinner('데이터 준비 중...'):
            # 구글 시트 데이터 로드
            gc = get_gspread_client()
            spreadsheet = gc.open("관심종목")
            worksheet = spreadsheet.get_worksheet(0)
            rows = worksheet.get_all_values()[1:]
            
            # 안정적인 시장 종목 리스트 확보 (FinanceDataReader)
            df_krx = fdr.StockListing('KRX')
            ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
        
        st.info(f"✅ 구글 시트에서 {len(rows)}개 종목을 성공적으로 가져왔습니다.")

        target_date = datetime.now().strftime("%Y%m%d")
        matched_results = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        # 분석 루프
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            status_text.text(f"분석 중: {name} ({i+1}/{len(rows)})")
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date("20240101", target_date, ticker)
                    if df is not None and not df.empty and len(df) >= 224:
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        close = df['종가'].iloc[-1]
                        
                        # 이동평균선 샌드위치 조건
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched_results.append({
                                '종목명': name,
                                '테마1': row[1] if len(row) > 1 else "미분류",
                                '테마2': row[2] if len(row) > 2 else "",
                                '테마3': row[3] if len(row) > 3 else ""
                            })
                except:
                    continue
            
            progress_bar.progress((i + 1) / len(rows))
            time.sleep(0.01)

        status_text.empty()

        # --- [5. 결과 처리 및 빈도순 정렬] ---
        if matched_results:
            res_df = pd.DataFrame(matched_results)
            
            # 테마별 빈도수 계산
            t1_counts = res_df['테마1'].value_counts()
            t2_counts = res_df['테마2'].value_counts()
            t3_counts = res_df['테마3'].value_counts()
            
            res_df['t1_cnt'] = res_df['테마1'].map(t1_counts)
            res_df['t2_cnt'] = res_df['테마2'].map(t2_counts)
            res_df['t3_cnt'] = res_df['테마3'].map(t3_counts)
            
            # 정렬: 테마1 빈도(내림) -> 테마2 빈도(내림) -> 테마3 빈도(내림) -> 종목명(오름)
            res_df = res_df.sort_values(
                by=['t1_cnt', 't2_cnt', 't3_cnt', '종목명'],
                ascending=[False, False, False, True]
            )
            
            # 임시 빈도수 컬럼 제거 후 출력
            final_df = res_df.drop(columns=['t1_cnt', 't2_cnt', 't3_cnt'])
            
            st.success(f"✅ 분석 완료! 총 {len(final_df)}건이 포착되었습니다.")
            st.dataframe(final_df, use_container_width=True)

            if send_noti:
                now_time = datetime.now().strftime("%Y-%m-%d %H:%M")
                msg = f"<b>🔔 [분석 완료] {now_time}</b>\n"
                msg += f"포착된 종목: <b>{len(final_df)}건</b>\n\n"
                for _, r in final_df.iterrows():
                    msg += f"• <b>{r['종목명']}</b> | {r['테마1']}\n"
                send_telegram_msg(msg)
                st.toast("텔레그램으로 전송되었습니다!")
        else:
            st.warning("현재 조건에 맞는 종목이 없습니다.")
            if send_noti:
                send_telegram_msg(f"✅ {target_date} 분석 결과: 조건 만족 종목 없음")
            
    except Exception as e:
        st.error(f"시스템 오류 발생: {e}")
