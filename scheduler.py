import os, json, time, requests
import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

def run_analysis():
    try:
        # 인증 및 시트 로드
        creds_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
        creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
        gc = gspread.authorize(creds)
        rows = gc.open("관심종목").get_worksheet(0).get_all_values()[1:]
        
        now = datetime.utcnow() + timedelta(hours=9)
        ticker_map = {}
        valid_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        
        # 최신 영업일 티커 확보
        for i in range(7):
            d = (now - timedelta(days=i)).strftime("%Y%m%d")
            temp = stock.get_market_ticker_list(date=d, market="ALL")
            if temp:
                ticker_map = {stock.get_market_ticker_name(t): t for t in temp}
                valid_date = d
                break
        
        if not ticker_map:
            df_krx = fdr.StockListing('KRX')
            ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()

        matched = []
        start_date = (now - timedelta(days=450)).strftime("%Y%m%d")

        for row in rows:
            name = row[0].strip()
            ticker = ticker_map.get(name)
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                    if len(df) >= 224:
                        ma120 = df['종가'].rolling(120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(224).mean().iloc[-1]
                        close = df['종가'].iloc[-1]
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                'name': name, 
                                't1': row[1] if len(row) > 1 else "",
                                't2': row[2] if len(row) > 2 else "",
                                't3': row[3] if len(row) > 3 else ""
                            })
                    time.sleep(0.1)
                except: continue

        if matched:
            df = pd.DataFrame(matched)
            f1, f2, f3 = df['t1'].value_counts(), df['t2'].value_counts(), df['t3'].value_counts()
            df['b1'], df['b2'], df['b3'] = df['t1'].map(f1).fillna(0), df['t2'].map(f2).fillna(0), df['t3'].map(f3).fillna(0)
            df = df.sort_values(by=['b1', 'b2', 'b3', 't1', 'name'], ascending=[False, False, False, True, True])
            
            msg = f"<b>🔔 [샌드위치 리포트] {valid_date}</b>\n총 <b>{len(df)}건</b>\n\n"
            for _, r in df.iterrows():
                themes = f"{r['t1']}"
                if r['t2']: themes += f", {r['t2']}"
                if r['t3']: themes += f", {r['t3']}"
                msg += f"• <b>{r['name']}</b> | {themes}\n"
            
            requests.post(f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendMessage", 
                          data={"chat_id": os.environ['TELEGRAM_CHAT_ID'], "text": msg[:4000], "parse_mode": "HTML"})
    except Exception as e: print(f"Error: {e}")

if __name__ == "__main__":
    run_analysis()
