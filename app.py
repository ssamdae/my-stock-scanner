import streamlit as st
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

        # 💡 [핵심 개선] 불안정한 pykrx 외부 통신을 차단하고, 야후 파이낸스용 .KS / .KQ 듀얼 티커 배열을 생성합니다.
        yf_tickers = []
        ticker_to_row = {}
        requested_set = set()
        
        for row in rows:
            if not row or not row[0]: continue
            ticker = str(row[0]).strip()
            
            if len(ticker) != 6 or not ticker.isdigit(): 
                continue
            
            # 한 종목당 코스피(.KS), 코스닥(.KQ) 후보를 둘 다 등록하여 한 번에 청구합니다.
            for suffix in [".KS", ".KQ"]:
                target = ticker + suffix
                if target not in requested_set:
                    yf_tickers.append(target)
                    requested_set.add(target)
                    ticker_to_row[target] = row

        if not yf_tickers:
            st.error("❌ CSV 파일에서 유효한 주식 티커를 찾지 못했습니다.")
            st.stop()

        # 데이터 다운로드 실행 (360일 최적화 세팅)
        with st.spinner(f'🚀 야후 파이낸스에서 {len(yf_tickers) // 2}개 종목 데이터 일괄 다운로드 중...'):
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

        if df_all is None or df_all.empty:
            st.error("❌ 야후 파이낸스로부터 데이터를 수집하지 못했습니다.")
            st.stop()

        # 거대 DataFrame을 메모리 상에서 초고속 루프 연산
        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        is_multi_index = isinstance(df_all.columns, pd.MultiIndex)
        
        # 중복 연산을 방지하기 위해 순수 종목 리스트 기준으로 루프를 돕니다.
        unique_tickers = list(dict.fromkeys([str(r[0]).strip() for r in rows if r and r[0] and len(str(r[0]).strip()) == 6]))
        total_len = len(unique_tickers)
        
        for idx, ticker in enumerate(unique_tickers):
            progress_bar.progress((idx + 1) / total_len)
            
            df_stock = None
            current_row = None
            
            # 💡 생성된 대량 데이터셋에서 .KS와 .KQ 중 데이터가 정상적으로 존재하는 유효 시장을 자동 판별합니다.
            for suffix in [".KS", ".KQ"]:
                target = ticker + suffix
                current_row = ticker_to_row.get(target)
                
                if is_multi_index and (target in df_all.columns.levels[0]):
                    df_candidate = df_all[target].dropna(subset=['Close'])
                    if len(df_candidate) >= 224:
                        df_stock = df_candidate
                        break
                elif not is_multi_index:
                    df_candidate = df_all.dropna(subset=['Close'])
                    if len(df_candidate) >= 224:
                        df_stock = df_candidate
                        break
            
            if df_stock is None or current_row is None:
                continue
                
            name = current_row[1].strip()
            status_text.text(f"초고속 연산 중 ({idx+1}/{total_len}): {name}")
            
            try:
                close_series = df_stock['Close']
                vol_series = df_stock['Volume']

                # 이평선 연산
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
                        '테마1': current_row[2].strip() if len(current_row) > 2 else "",
                        '테마2': current_row[3].strip() if len(current_row) > 3 else "",
                        '테마3': current_row[4].strip() if len(current_row) > 4 else ""
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
