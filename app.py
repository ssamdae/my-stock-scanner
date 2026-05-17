import streamlit as st
import pandas as pd
from datetime import datetime
import os
import requests
import xml.etree.ElementTree as ET  # 💡 네이버 대량 XML 데이터를 빛의 속도로 파싱할 엔진
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


# 💡 개별 종목 네이버 초고속 차트 API 수집 및 분석 워커 함수
def analyze_single_stock_naver(row):
    if not row or not row[0]: 
        return None
        
    ticker = str(row[0]).strip()
    if len(ticker) != 6 or not ticker.isdigit(): 
        return None
        
    name = row[1].strip()
    
    # 💡 [핵심] 네이버 차트용 일별 데이터 주소 (최근 360 영업일 분량을 단 1방에 요청)
    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={ticker}&timeframe=day&count=360&requestType=0"
    
    try:
        # 네이버 내부망이라 2초 타임아웃만 주어도 광속으로 응답합니다.
        res = requests.get(url, timeout=2)
        if res.status_code != 200: 
            return None
            
        # XML 구조 고속 해체
        root = ET.fromstring(res.text)
        items = root.findall('.//item')
        if len(items) < 224: 
            return None
            
        data_list = []
        for item in items:
            data_str = item.get('data')
            parts = data_str.split('|') # 형식: "날짜|시가|고가|저가|종가|거래량"
            if len(parts) == 6:
                data_list.append({
                    'Close': float(parts[4]),
                    'Volume': float(parts[5])
                })
        
        df_stock = pd.DataFrame(data_list)
        if len(df_stock) < 224: 
            return None
            
        close_series = df_stock['Close']
        vol_series = df_stock['Volume']

        # 샌드위치 이평선 및 돌파 조건 연산
        ma120 = close_series.rolling(120).mean().iloc[-1]
        ma224 = close_series.rolling(224).mean().iloc[-1]
        upper_ma = max(ma120, ma224)

        prev_close = close_series.iloc[-2]
        last_close = close_series.iloc[-1]
        
        prev_vol = vol_series.iloc[-2]
        last_vol = vol_series.iloc[-1]
        vol_ratio = (last_vol / prev_vol * 100) if prev_vol > 0 else 0

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
        csv_path = os.path.expanduser("~/my-stock-scanner/watchlist.csv")
        
        if not os.path.exists(csv_path):
            st.error(f"❌ 서버 내에 watchlist.csv 파일이 없습니다. 경로 확인 필수: {csv_path}")
            st.stop()
            
        with st.spinner('서버 로컬 CSV 파일에서 종목을 불러오는 중...'):
            df_stocks = pd.read_csv(csv_path, dtype={'티커': str}, encoding='utf-8-sig').fillna('')
            rows = df_stocks.values.tolist()
            
            if not rows:
                st.warning("분석할 종목 데이터가 없습니다.")
                st.stop()

        # 중복 종목 완벽 필터링 처리
        unique_rows = []
        seen_tickers = set()
        for row in rows:
            if not row or not row[0]: continue
            ticker = str(row[0]).strip()
            if len(ticker) == 6 and ticker.isdigit() and ticker not in seen_tickers:
                seen_tickers.add(ticker)
                unique_rows.append(row)

        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_len = len(unique_rows)
        st.info(f"⚡ 외부 락 유발 라이브러리 전면 제거 완료. 네이버 fchart망을 통해 {total_len}개 종목 스캔을 전개합니다.")

        # 💡 [핵심] 네이버 전용망은 트래픽 수용량이 크므로 스레드 15개를 동시 가동해 3초대 컷을 냅니다.
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = {executor.submit(analyze_single_stock_naver, row): row for row in unique_rows}
            
            completed_count = 0
            for future in as_completed(futures):
                completed_count += 1
                row = futures[future]
                name = row[1].strip() if len(row) > 1 else "Unknown"
                
                # 메인 UI 실시간 스크롤
                progress_bar.progress(completed_count / total_len)
                status_text.text(f"네이버 실시간 주가 검증 중 ({completed_count}/{total_len}): {name}")
                
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
            
            for t in ['테마1', '테마2', '테마3']:
                counts = res_df[res_df[t] != ''][t].value_counts()
                res_df[f'{t}_빈도'] = res_df[t].map(counts).fillna(0)
            
            res_df = res_df.sort_values(by=['테마1_빈도', '테마1', '테마2_빈도', '테마2', '테마3_빈도', '종목명'], ascending=[False, True, False, True, False, True])
            
            st.success(f"✅ 총 {len(res_df)}개의 돌파 종목 발견! (기준일: {datetime.now().strftime('%Y%m%d')})")
            
            display_df = res_df[['종목명', '현재가', '거래량비율', '테마1', '테마2', '테마3']]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            if btn_tele:
                msg = f"<b>🚀 [강력 돌파 포착: {datetime.now().strftime('%Y%m%d')}]</b>\n"
                msg += f"상단 이평선 돌파 + 거래량 200%↑\n"
                msg += f"총 <b>{len(res_df)}건</b>\n\n"
                
                for _, r in res_df.iterrows():
                    theme_list = [t for t in [r['테마1'], r['테마2'], r['테마3']] if t.strip()]
                    theme_str = ", ".join(theme_list)
                    msg += f"• <b>{r['종목명']}</b> (🔥{r['거래량비율']}) | {theme_str}\n"
                
                if send_telegram_msg(msg):
                    st.toast("텔레그램 전송 완료!")
                else:
                    st.error("텔레error람 전송 실패")
        else:
            st.warning("조건(돌파 + 거래량 2배)에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"❌ 시스템 오류: {str(e)}")
