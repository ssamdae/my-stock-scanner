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
        
        # 시트 데이터 가져오기 (A: 티커, B: 종목명, C~E: 테마)
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
            
            ticker = row[0].strip() # A열: 티커
            name = row[1].strip()   # B열: 종목명
            
            print(f"분석 중: {name} ({ticker})...")
            
            df = None
            try:
                # [방법 1] pykrx 시도
                df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                
                # [방법 2] pykrx 실패 시 yfinance 백업
                if df is None or df.empty or len(df) < 224:
                    for suffix in [".KS", ".KQ"]:
                        df_yf = yf.download(f"{ticker}{suffix}", start=(now - timedelta(days=450)), end=now, progress=False, show_errors=False)
                        if not df_yf.empty and len(df_yf) >= 224:
                            # yfinance 최신 버전의 MultiIndex 종가 처리
                            if isinstance(df_yf['Close'], pd.DataFrame):
                                close_series = df_yf['Close'].iloc[:, 0]
                            else:
                                close_series = df_yf['Close']
                            
                            df = pd.DataFrame({'종가': close_series})
                            break

                # 4. 샌드위치 조건 계산
                if df is not None and not df.empty and len(df) >= 224:
                    ma120 = df['종가'].rolling(120).mean().iloc[-1]
                    ma224 = df['종가'].rolling(224).mean().iloc[-1]
                    close = df['종가'].iloc[-1]

                    if (ma224 < close < ma120) or (ma120 < close < ma224):
                        matched.append({
                            'name': name, 
                            't1': row[2].strip() if len(row) > 2 else "",
                            't2': row[3].strip() if len(row) > 3 else "",
                            't3': row[4].strip() if len(row) > 4 else ""
                        })
                
                # 서버 차단 방지를 위한 지연 시간 (GitHub Actions 환경은 IP 공유로 인해 0.5초 이상 권장)
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error analyzing {name}: {e}")
                continue

        # 5. 결과 정렬 및 텔레그램 전송
        if matched:
            df_res = pd.DataFrame(matched)
            # 테마1 빈도 기준 정렬
            f1 = df_res[df_res['t1'] != '']['t1'].value_counts()
            df_res['b1'] = df_res['t1'].map(f1).fillna(0)
            df_res = df_res.sort_values(by=['b1', 't1', 'name'], ascending=[False, True, True])
            
            msg = f"<b>🔔 [샌드위치 리포트] {valid_date}</b>\n총 <b>{len(df_res)}건</b> 발견\n\n"
            for _, r in df_res.iterrows():
                themes = f"#{r['t1']}"
                if r['t2']: themes += f" #{r['t2']}"
                if r['t3']: themes += f" #{r['t3']}"
                msg += f"• <b>{r['name']}</b> | {themes}\n"
            
            # 텔레그램 API 호출
            token = os.environ['TELEGRAM_BOT_TOKEN']
            chat_id = os.environ['TELEGRAM_CHAT_ID']
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            
            # 메시지 길이가 너무 길 경우를 대비해 분할 전송 (필요 시)
            requests.post(url, data={"chat_id": chat_id, "text": msg[:4000], "parse_mode": "HTML"})
            print(f"전송 완료: {len(df_res)}건")
        else:
            print("조건에 맞는 종목이 없습니다.")

    except Exception as e:
        print(f"Main Error: {e}")

if __name__ == "__main__":
    run_analysis()
