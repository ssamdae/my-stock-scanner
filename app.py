import streamlit as st
import pandas as pd
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. 페이지 설정
st.set_page_config(page_title="코스피/코스닥 돌파 스캐너", layout="wide")
st.title("🚀 순수 주식 실시간 돌파 레이더")
st.caption("KOSPI/KOSDAQ 전체 종목 중 자산운용사 ETF 브랜드, 파생상품, 리츠, 우선주, 스팩을 완벽하게 필터링합니다.")

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

# 💡 [초강력 필터 보정] 웹 대시보드에서도 ETF 브랜드 및 파생상품 완벽 제거
def get_all_tickers_naver():
    tickers = []
    
    blacklist = (
        "KODEX", "TIGER", "RISE", "ACE", "ARIRANG", "KBSTAR", 
        "HANARO", "KOSEF", "SOL", "KOACT", "TIMEFOLIO", "PLUS", 
        "WON", "마이티", "히어로즈", "TREX", "UNICORN", 
        "ETF", "ETN", "리츠", "REIT", "스팩", "인프라", "펀드", "투자회사",
        "(H)", "(합성)", "레버리지", "인버스", "선물", "콜", "풋"
    )

    # sosok 0: KOSPI, 1: KOSDAQ
    for sosok in [0, 1]:
        for page in range(1, 45): 
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, timeout=5)
                soup = BeautifulSoup(res.text, 'html.parser')
                links = soup.select('a.tltle')
                if not links: break
                for link in links:
                    href = link.get('href')
                    if href and 'code=' in href:
                        ticker = href.split('code=')[-1]
                        name = link.text.strip()
                        
                        # ❌ [1차 방어벽] 우선주 제거
                        if name.endswith("우") or name.endswith("우B") or name.endswith("우C"): 
                            continue
                            
                        # ❌ [2차 방어벽] 블랙리스트 포함 항목 제거
                        if any(bad_word in name for bad_word in blacklist):
                            continue
                        
                        tickers.append((ticker, name))
            except Exception:
                continue
    return tickers

# 개별 종목 분석 워커 함수
def analyze_single_stock_naver(item):
    ticker, name = item
    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={ticker}&timeframe=day&count=360&requestType=0"
    try:
        res = requests.get(url, timeout=2)
        if res.status_code != 200: return None
        root = ET.fromstring(res.text)
        items = root.findall('.//item')
        if len(items) < 224: return None
        data_list = []
        for item in items:
            data_str = item.get('data')
            parts = data_str.split('|')
            if len(parts) == 6:
                data_list.append({'Close': float(parts[4]), 'Volume': float(parts[5])})
        df_stock = pd.DataFrame(data_list)
        close_series = df_stock['Close']
        vol_series = df_stock['Volume']
        ma120 = close_series.rolling(120).mean().iloc[-1]
        ma224 = close_series.rolling(224).mean().iloc[-1]
        upper_ma = max(ma120, ma224)
        prev_close = close_series.iloc[-2]
        last_close = close_series.iloc[-1]
        prev_vol = vol_series.iloc[-2]
        last_vol = vol_series.iloc[-1]
        vol_ratio = (last_vol / prev_vol * 100) if prev_vol > 0 else 0
        if prev_close < upper_ma < last_close and vol_ratio >= 200:
            return {'종목명': name, '현재가': f"{int(last_close):,}", '거래량비율': round(float(vol_ratio), 1)}
    except Exception:
        pass
    return None

# 3. 분석 실행 UI
col1, col2 = st.columns(2)
btn_web = col1.button("🖥️ 순수 주식 전수 스캔 (웹 결과)", use_container_width=True)
btn_tele = col2.button("🔔 순수 주식 전수 스캔 + 텔레그램 보고", use_container_width=True)

if btn_web or btn_tele:
    try:
        with st.spinner('📡 네이버 금융에서 순수 기업 리스트 추출 중...'):
            market_tickers = get_all_tickers_naver()
        
        if not market_tickers:
            st.error("종목 리스트 수집에 실패했습니다.")
            st.stop()

        total_len = len(market_tickers)
        st.info(f"✅ 총 {total_len}개 순수 기업 종목 확보. 정밀 분석을 시작합니다. (약 25초 소요)")

        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(analyze_single_stock_naver, item): item for item in market_tickers}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 50 == 0:
                    progress_bar.progress(completed / total_len)
                    status_text.text(f"시장 전수 조사 중... ({completed}/{total_len})")
                
                res = future.result()
                if res: matched.append(res)

        status_text.empty()
        progress_bar.empty()

        today_str = datetime.now().strftime('%Y-%m-%d')
        
        if matched:
            res_df = pd.DataFrame(matched).sort_values(by='거래량비율', ascending=False)
            st.success(f"🎯 총 {len(res_df)}개의 주도주 돌파 종목을 발견했습니다!")
            st.dataframe(res_df, use_container_width=True, hide_index=True)

            if btn_tele:
                msg = f"<b>⏰ [순수 주식 자동 스캔 리포트: {today_str}]</b>\n"
                msg += f"상단 이평선 돌파 + 거래량 200%↑\n"
                msg += f"총 <b>{len(res_df)}건</b>\n\n"
                for _, r in res_df.iterrows():
                    msg += f"• <b>{r['종목명']}</b> (🔥{r['거래량비율']}%)\n"
                if send_telegram_msg(msg): st.toast("텔레그램 발송 완료!")
        else:
            st.warning("💡 현재 조건에 일치하는 돌파 종목이 존재하지 않습니다.")
            if btn_tele:
                msg = f"<b>⏰ [순수 주식 자동 스캔 리포트: {today_str}]</b>\n\n💡 현재 조건에 일치하는 돌파 종목이 존재하지 않습니다."
                send_telegram_msg(msg)
                st.toast("조건 불일치 보고 전송 완료")

    except Exception as e:
        st.error(f"시스템 오류 발생: {str(e)}")
