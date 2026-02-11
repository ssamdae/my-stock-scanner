import os, json, time, requests
import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

def run_analysis():
    try:
        # 설정 로드
        creds_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
        bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]
        
        gc = gspread.authorize(Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']))
        rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]
        
        df_krx = fdr.StockListing('KRX')
        ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
        
        now = datetime.now() + timedelta(hours=9)
        target_date = now.strftime("%Y%m%d")
        matched = []

        for row in rows:
            name = row[0].strip()
            ticker = ticker_map.get(name)
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date("20240101", target_date, ticker)
                    if df is not None and len(df) >= 224:
                        ma120, ma224, close = df['종가'].rolling(120).mean().iloc[-1], df['종가'].rolling(224).mean().iloc[-1], df['종가'].iloc[-1]
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({'name': name, 'theme': row[1] if len(row)>1 else "미분류"})
                    time.sleep(0.1)
                except: continue

        if matched:
            df = pd.DataFrame(matched)
            # 빈도순 정렬 로직
            counts = df['theme'].value_counts()
            df['cnt'] = df['theme'].map(counts)
            df = df.sort_values(by=['cnt', 'theme', 'name'], ascending=[False, True, True])
            
            msg = f"<b>🔔 [정기 분석] {now.strftime('%H:%M')}</b>\n총 <b>{len(df)}건</b> (빈도순 정렬)\n\n"
            for _, r in df.iterrows():
                msg += f"• <b>{r['name']}</b> | {r['theme']}\n"
            
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            requests.post(url, data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_analysis()
