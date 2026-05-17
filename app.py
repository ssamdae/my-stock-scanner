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

        # 야후 파이낸스용 티커 배열 생성 (.KS / .KQ 듀얼 세팅)
        yf_tickers = []
        ticker_to_row = {}
        requested_set = set()
        
        for row in rows:
            if not row or not row[0]: continue
            ticker = str(row[0]).strip()
            
            if len(ticker) != 6 or not ticker.isdigit(): 
                continue
            
            for suffix in [".KS", ".KQ"]:
                target = ticker + suffix
                if target not in requested_set:
                    yf_tickers.append(target)
                    requested_set.add(target)
                    ticker_to_row[target] = row

        if not yf_tickers:
            st.error("❌ CSV 파일에서 유효한 주식 티커를 찾지 못했습니다.")
            st.stop()

        # 💡 [핵심 개선 1] 944개 티커를 50개씩 안전하게 쪼개서 순차 다운로드 (데드락 원천 차단)
        chunk_size = 50
        ticker_chunks = [yf_tickers[i:i + chunk_size] for i in range(0, len(yf_tickers), chunk_size)]
        
        df_list = []
        end_date_dt = datetime.now()
        start_date_dt = end_date_dt - timedelta(days=360)
        
        total_chunks = len(ticker_chunks)
        download_status = st.empty()
        download_progress = st.progress(0)

        for chunk_idx, chunk in enumerate(ticker_chunks):
            # 다운로드 진행 상황을 실시간으로 화면에 노출하여 멈춤 여부 인지 가능하게 처리
            download_status.info(f"🚀 야후 파이낸스 데이터 안전 다운로드 중 ({chunk_idx + 1}/{total_chunks} 그룹)...")
            download_progress.progress((chunk_idx + 1) / total_chunks)
            
            try:
                # 💡 [핵심 개선 2] threads=False 로 설정하여 1 OCPU 서버 락 현상 방지 및 10초 타임아웃 강제
                df_chunk = yf.download(
                    tickers=chunk,
                    start=start_date_dt,
                    end=end_date_dt,
                    group_by='ticker',
                    progress=False,
                    threads=False,
                    timeout=10
                )
                if df_chunk is not None and not df_chunk.empty:
                    df_list.append(df_chunk)
            except Exception:
                # 특정 그룹에서 일시 에러가 나도 전체가 죽지 않고 패스하도록 방어
                continue

        download_status.empty()
        download_progress.empty()

        if not df_list:
            st.error("❌ 야후 파이낸스로부터 데이터를 단 하나도 수집하지 못했습니다. 통신 상태를 확인해 주세요.")
            st.stop()

        # 💡 분할 다운로드된 데이터프레임들을 컬럼(옆) 방향으로 안전하게 병합
        with st.spinner('📦 수집된 시장 데이터 통합 가공 중...'):
            df_all = pd.concat(df_list, axis=1)

        # 거대 DataFrame을 메모리 상에서 초고속 루프 연산
        matched = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        is_multi_index = isinstance(df_all.columns, pd.MultiIndex)
        
        unique_tickers = list(dict.fromkeys([str(r[0]).strip() for r in rows if r and r[0] and len(str(r[0]).strip()) == 6]))
        total_len = len(unique_tickers)
        
        for idx, ticker in enumerate(unique_tickers):
            progress_bar.progress((idx + 1) / total_len)
            
            df_stock = None
            current_row = None
            
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
            
            # 웹 화면 표시
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
