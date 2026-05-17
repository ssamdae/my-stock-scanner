import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
import yfinance as yf

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

        with st.spinner('시장 데이터 동기화 중...'):
            kospi_tickers = set(stock.get_market_ticker_list(market="KOSPI"))

        yf_tickers = []
        ticker_to_row = {}
        
        for row in rows:
            if not row or not row[0]: continue
            
            ticker = str(row[0]).strip()
            
            # 대한민국 주식 티커 6자리 숫자 형태 검증
            if len(ticker) != 6 or not ticker.isdigit(): 
                continue
                
            suffix = ".KS" if ticker in kospi_tickers else ".KQ"
            yf_ticker = ticker + suffix
            
            if yf_ticker not in yf_tickers:
                yf_tickers.append(yf_ticker)
                ticker_to_row[yf_ticker] = row

        # 💡 [방어 레이어 1] 추출된 티커가 존재하지 않을 경우 야후 파이낸스 요청 전 차단
        if not yf_tickers:
            st.error("❌ CSV 파일에서 유효한 6자리 주식 티커를 단 하나도 찾지 못했습니다. '티커' 컬럼 데이터의 상태나 파일 인코딩을 점검해 주세요.")
            st.stop()
            
        st.info(f"📦 총 {len(yf_tickers)}개의 유효 종목을 야후 파이낸스에 청구합니다.")

        # 데이터 다운로드 실행 (360일 최적화)
        df_all = pd.DataFrame()
        try:
            with st.spinner(f'🚀 야후 파이낸스에서 {len(yf_tickers)}개 종목 대량 멀티 데이터 다운로드 중...'):
                end_date_dt = datetime.now()
                start_date_dt = end_date_dt - timedelta(days=360)
                
                df_all = yf.download(
                    tickers=yf_tickers,
                    start=start_date_dt,
                    end=end_date_dt,
                    group_by='ticker',
                    progress=False,
                    show_errors=False
                )
        # 💡 [방어 레이어 2] 야후 파이낸스 패키지 내부 가공 오류(IndexError 등) 원천 격리
        except Exception as yf_err:
            st.error(f"❌ 야후 파이낸스 엔진 내부 통신/조립 오류 발생: {str(yf_err)}")
            st.info("💡 해결법: 오라클 서버 터미널에서 [ pip install --upgrade yfinance ] 명령어를 실행해 라이브러리를 최신판으로 업데이트해 주십시오.")
            st.stop()

        if df_all is None or df_all.empty:
            st.error("❌ 야후 파이낸스로부터 반환된 데이터셋이 텅 비어 있습니다. 잠시 후 다시 시도해 주세요.")
            st.stop()

        # 거대 DataFrame을 메모리 상에서 초고속 루프 연산
        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 💡 [방어 레이어 3] 멀티인덱스 컬럼 구조가 안전하게 정상 생성되었는지 확인
        is_multi_index = isinstance(df_all.columns, pd.MultiIndex)
        total_tickers = len(yf_tickers)
        
        for idx, yf_ticker in enumerate(yf_tickers):
            progress_bar.progress((idx + 1) / total_tickers)
            row = ticker_to_row[yf_ticker]
            name = row[1].strip()
            status_text.text(f"초고속 연산 중 ({idx+1}/{total_tickers}): {name}")
            
            try:
                # 멀티인덱스 구조인 경우 안전하게 슬라이싱
                if is_multi_index and (yf_ticker in df_all.columns.levels[0]):
                    df_stock = df_all[yf_ticker].dropna(subset=['Close'])
                elif not is_multi_index and total_tickers == 1:
                    df_stock = df_all.dropna(subset=['Close'])
                else:
                    continue
                
                if len(df_stock) < 224: 
                    continue
                
                close_series = df_stock['Close']
                vol_series = df_stock['Volume']

                # 이평선 연산 (120일선, 224일선)
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
                    matched.append({
                        '종목명': name, 
                        '현재가': f"{int(last_close):,}",
                        '거래량비율': f"{vol_ratio:.1f}%",
                        '테마1': row[2].strip() if len(row) > 2 else "",
                        '테마2': row[3].strip() if len(row) > 3 else "",
                        '테마3': row[4].strip() if len(row) > 4 else ""
                    })
            except Exception:
                continue

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
                    st.error("텔레그램 전송 실패")
        else:
            st.warning("조건(돌파 + 거래량 2배)에 맞는 종목이 없습니다.")

    except Exception as e:
        st.error(f"❌ 시스템 오류: {str(e)}")
