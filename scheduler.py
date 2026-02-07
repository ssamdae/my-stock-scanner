import os
import json
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

# 텔레그램 전송 함수
def send_telegram_msg(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    requests.post(url, data=payload)

def run_analysis():
    print("🚀 분석 프로세스를 시작합니다...")
    
    creds_raw = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_raw:
        print("❌ 에러: GCP_SERVICE_ACCOUNT 시크릿을 찾을 수 없습니다.")
        return
    else:
        print(f"✅ 시크릿 로드 성공 (글자 수: {len(creds_raw)})")
    
    # GitHub Secrets에서 정보 가져오기
    creds_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    
    # 구글 시트 연결
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    
    spreadsheet = gc.open("관심종목")
    worksheet = spreadsheet.get_worksheet(0)
    all_data = worksheet.get_all_values()
    rows = all_data[1:]
    
    today = datetime.now().strftime("%Y%m%d")
    try:
        all_tickers = stock.get_market_ticker_list(today, market="ALL")
    except:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        all_tickers = stock.get_market_ticker_list(yesterday, market="ALL")
        
    ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}
    matched_results = []

    for row in rows:
        name = row[0]
        ticker = ticker_map.get(name)
        if ticker:
            try:
                df = stock.get_market_ohlcv_by_date("20240101", today, ticker)
                if df is not None and not df.empty and len(df) >= 224:
                    ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                    ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                    current_close = df['종가'].iloc[-1]
                    if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                        matched_results.append([name, row[1] if len(row)>1 else ""])
                time.sleep(0.1)
            except: continue

    if matched_results:
        msg = f"<b>🔔 [정기 분석 완료] {today}</b>\n총 {len(matched_results)}건 포착\n\n"
        for res in matched_results:
            msg += f"• <b>{res[0]}</b> | {res[1]}\n"
        send_telegram_msg(bot_token, chat_id, msg)

if __name__ == "__main__":
    run_analysis()
