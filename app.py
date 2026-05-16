import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. 페이지 설정
st.set_page_config(page_title="돌파", layout="wide")
st.title("🚀돌파")


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


# 개별 종목 분석용 워커 함수 (ThreadPoolExecutor에 의해 병렬 실행)
def analyze_single_stock(row, start_date, end_date):
    if not row or not row[0]: 
        return None
        
    ticker = row[0].strip()
    name = row[1].strip()
    df = None
    
    try:
        # 1. pykrx 시도
        df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
        
        # 2. 백업: yfinance 시도 (pykrx 실패 시)
        if df is None or df.empty or len(df) < 224:
            for suffix in [".KS", ".KQ"]:
                df_yf = yf.download(
                    ticker + suffix, 
                    start=(datetime.now() - timedelta(days=450)), 
                    end=datetime.now(), 
                    progress=False, 
                    show_errors=False
                )
                if not df_yf.empty and len(df_yf) >= 224:
                    df = df_yf.rename(columns={'Close': '종가', 'Volume': '거래량'})
                    break

        if df is not None and not df.empty and len(df) >= 224:
            if 'Volume' in df.columns: df = df.rename(columns={'Volume': '거래량'})
            if 'Close' in df.columns: df = df.rename(columns={'Close': '종가'})

            # 데이터 차원 보정 (Multi-index 대응)
            close_series = df['종가'].iloc[:, 0] if isinstance(df['종가'], pd.DataFrame) else df['종가']
            vol_series = df['거래량'].iloc[:, 0] if isinstance(df['거래량'], pd.DataFrame) else df['거래량']

            # 이평선 계산 (120일선, 224일선)
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
                return {
                    '종목명': name, 
                    '현재가': f"{int(last_close):,}",
                    '거래량비율': f"{vol_ratio:.1f}%",
                    '테마1': row[2].strip() if len(row) > 2 else "",
                    '테마2': row[3].strip() if len(row) > 3 else "",
                    '테마3': row[4].strip() if len(row) > 4 else ""
                }
    except Exception:
        pass
    return None


# 3. 분석 실행 버튼
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 웹으로 결과 보기", use_container_width=True)
btn_tele = col2.button("🔔 웹 + 텔레그램 알림 받기", use_container_width=True)

if btn_web or btn_tele:
    try:
        # 💡 반영 내용 1: 정확한 프로젝트 폴더 내 영어 파일명 경로 지정
        csv_path = os.path.expanduser("~/my-stock-scanner/watchlist.csv")
        
        if not os.path.exists(csv_path):
            st.error(f"❌ 서버 내에 watchlist.csv 파일이 없습니다. 경로를 확인해 주세요: {csv_path}")
            st.stop()
            
        with st.spinner('서버 로컬 CSV 파일에서 종목을 불러오는 중...'):
            # 💡 반영 내용 2: 엑셀 UTF-8 BOM 한글 깨짐 방지(utf-8-sig) 및 티커 문자열 유지 적용
            df_stocks = pd.read_csv(csv_path, dtype={'티커': str}, encoding='utf-8-sig').fillna('')
            rows = df_stocks.values.tolist()
            
            if not rows:
                st.warning("분석할 종목 데이터가 없습니다.")
                st.stop()

        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=450)).strftime("%Y%m%d")

        # 💡 멀티스레딩 병렬 스캔 실행 (최대 8개 스레드 동시 가동)
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(analyze_single_stock, row, start_date, end_date): row for row in rows}
            
            completed_count = 0
            for future in as_completed(futures):
                completed_count += 1
                row = futures[future]
                name = row[1].strip() if len(row) > 1 else "Unknown"
                
                # 메인 스레드에서 UI 실시간 업데이트
                progress_bar.progress(completed_count / len(rows))
                status_text.text(f"진행 중 ({completed_count}/{len(rows)}): {name}")
                
                try:
                    result = future.result()
                    if result is not None:
                        matched.append(result)
                except Exception:
                    pass

        status_text.empty()
        progress_bar.empty()

        if matched:
            res_df = pd.DataFrame(matched)
            
            # 테마 빈도 기반 정렬 로직
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
