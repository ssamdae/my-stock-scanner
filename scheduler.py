import os
import json
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
import requests

def send_telegram_msg(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, data=payload)
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")

def run_analysis():
    print("🚀 분석 프로세스를 시작합니다...")
    
    # 1. 환경 변수 확인 단계
    try:
        creds_raw = os.environ.get("GCP_SERVICE_ACCOUNT")
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        
        if not creds_raw or not bot_token or not chat_id:
            raise ValueError("GitHub Secrets 설정이 누락되었습니다. (GCP_SERVICE_ACCOUNT, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID 확인 필요)")
            
        creds_json = json.loads(creds_raw)
        print("✅ 환경 변수 및 JSON 로드 성공")
    except Exception as e:
        print(f"❌ [에러] 환경 변수 확인 단계: {e}")
        return

    # 2. 구글 시트 연결 단계
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        gc = gspread.authorize(creds)
        
        # 현재 접속을 시도하는 서비스 계정 이메일을 로그에 출력
        print(f"📧 접속 계정: {creds_json.get('client_email')}")
        
        spreadsheet = gc.open("내관심종목") # 여기서 에러가 난다면 공유 설정 문제입니다.
        worksheet = spreadsheet.get_worksheet(0)
        all_data = worksheet.get_all_values()
        rows = all_data[1:]
        print(f"✅ 구글 시트 연결 성공: {len(rows)}개 종목 확인")
    except gspread.exceptions.SpreadsheetNotFound:
        print("❌ [에러] 시트를 찾을 수 없습니다. 시트 이름이 '내관심종목'이 맞는지, 서비스 계정 이메일이 공유되어 있는지 확인하세요.")
        return
    except Exception as e:
        print(f"❌ [에러] 구글 시트 연결 단계: {e}")
        return

    # 3. 주식 분석 단계 (기존 로직 동일)
    today = datetime.now().strftime("%Y%m%d")
    try:
        all_tickers = stock.get_market_ticker_list(today, market="ALL")
    except:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        all_tickers = stock.get_market_ticker_list(yesterday, market="ALL")
        
    ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}
    matched_results = []

    print("📊 종목 분석 중...")
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
                        theme = row[1] if len(row) > 1 else ""
                        matched_results.append([name, theme])
                time.sleep(0.05)
            except: continue

    # 4. 결과 전송 단계
    if matched_results:
        msg = f"<b>🔔 [정기 분석 완료] {today}</b>\n총 {len(matched_results)}건 포착\n\n"
        for res in matched_results:
            msg += f"• <b>{res[0]}</b> | {res[1]}\n"
        send_telegram_msg(bot_token, chat_id, msg)
        print(f"✅ 분석 완료 및 {len(matched_results)}건 텔레그램 전송 성공")
    else:
        print("ℹ️ 분석 완료: 조건 만족 종목 없음")

if __name__ == "__main__":
    run_analysis()
