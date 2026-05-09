import os, json, time, requests
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

def run_analysis():
    try:
        # 1. 인증 및 구글 시트 로드
        creds_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
        creds = Credentials.from_service_account_info(
            creds_json, 
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(creds)
        
        sheet = gc.open("관심종목").get_worksheet(0)
        rows = sheet.get_all_values()[1:] # 헤더 제외
        
        if not rows:
            print("분석할 종목이 없습니다.")
            return

        # 2. 날짜 설정 (KST 기준)
        now = datetime.utcnow() + timedelta(hours=9)
        valid_date = now.strftime("%Y%m%d")
        start_date = (now - timedelta(days=450)).strftime("%Y%m%d")

        matched = []

        # 3. 분석 루프
        for row in rows:
            if not row or not row[0]: continue
            
            ticker = row[0].strip()
            name = row[1].strip()
            
            print(f"분석 중: {name} ({ticker})...")
            
            df = None
            try:
                # [방법 1] pykrx 시도
                df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                
                # [방법 2] yfinance 백업
                if df is None or df.empty or len(df) < 224:
                    for suffix in [".KS", ".KQ"]:
                        df_yf = yf.download(f"{ticker}{suffix}", start=(now - timedelta(days=450)), end=now, progress=False, show_errors=False)
                        if not df_yf.empty and len(df_yf) >= 224:
                            df = df_yf.rename(columns={'Close': '종가', 'Volume': '거래량'})
                            break

                # 4. 새로운 조건 계산 (상단 이평 돌파 + 거래량 2배)
                if df is not None and not df.empty and len(df) >= 224:
                    # 컬럼명 통일
                    if 'Volume' in df.columns: df = df.rename(columns={'Volume': '거래량'})
                    if 'Close' in df.columns: df = df.rename(columns={'Close': '종가'})

                    # 데이터 차원 보정 (Multi-index 대응)
                    close_s = df['종가'].iloc[:, 0] if isinstance(df['종가'], pd.DataFrame) else df['종가']
                    vol_s = df['거래량'].iloc[:, 0] if isinstance(df['거래량'], pd.DataFrame) else df['거래량']

                    ma120 = close_s.rolling(120).mean().iloc[-1]
                    ma224 = close_s.rolling(224).mean().iloc[-1]
                    upper_ma = max(ma120, ma224)
                    
                    prev_close = close_s.iloc[-2]
                    last_close = close_s.iloc[-1]
                    prev_vol = vol_s.iloc[-2]
                    last_vol = vol_s.iloc[-1]
                    
                    vol_ratio = (last_vol / prev_vol * 100) if prev_vol > 0 else 0

                    # 조건 검증
                    if (prev_close < upper_ma < last_close) and (vol_ratio >= 200):
                        matched.append({
                            'name': name,
                            'vol_ratio': f"{vol_ratio:.1f}%",
                            't1': row[2].strip() if len(row) > 2 else "",
                            't2': row[3].strip() if len(row) > 3 else "",
                            't3': row[4].strip() if len(row) > 4 else ""
                        })
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error {name}: {e}")
                continue

        # 5. 결과 전송
        if matched:
            df_res = pd.DataFrame(matched)
            # 테마 빈도 정렬
            f1 = df_res[df_res['t1'] != '']['t1'].value_counts()
            df_res['b1'] = df_res['t1'].map(f1).fillna(0)
            df_res = df_res.sort_values(by=['b1', 't1', 'name'], ascending=[False, True, True])
            
            msg = f"<b>🚀 [강력 돌파 리포트] {valid_date}</b>\n"
            msg += f"상단 이평 돌파 + 거래량 200%↑\n"
            msg += f"총 <b>{len(df_res)}건</b> 발견\n\n"
            
            for _, r in df_res.iterrows():
                themes = f"#{r['t1']}"
                if r['t2']: themes += f" #{r['t2']}"
                if r['t3']: themes += f" #{r['t3']}"
                msg += f"• <b>{r['name']}</b> (🔥{r['vol_ratio']}) | {themes}\n"
            
            token = os.environ['TELEGRAM_BOT_TOKEN']
            chat_id = os.environ['TELEGRAM_CHAT_ID']
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, data={"chat_id": chat_id, "text": msg[:4000], "parse_mode": "HTML"})
            print(f"전송 완료: {len(df_res)}건")
        else:
            print("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        print(f"Main Error: {e}")

if __name__ == "__main__":
    run_analysis()
