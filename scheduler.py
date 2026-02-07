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
        requests.post(url, data=payload, timeout=10)
    except:
        pass

def run_analysis():
    print("🚀 [로직 보완 모드] 분석을 시작합니다...")
    
    try:
        # 1. 환경 변수 및 시트 연결
        creds_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
        bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]
        
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open("관심종목")
        worksheet = spreadsheet.get_worksheet(0)
        all_data = worksheet.get_all_values()
        rows = all_data[1:]
        print(f"✅ 시트 연결 성공: 총 {len(rows)}개 종목 로드")

        # 2. 최근 영업일 찾기 (반복문 방식)
        now = datetime.now() + timedelta(hours=9) # KST 한국 시간 보정
        all_tickers = []
        latest_trading_day = ""
        
        print("🔍 최근 영업일 데이터를 찾는 중...")
        for i in range(10): # 최근 10일간 데이터를 뒤로 가며 확인
            check_date = (now - timedelta(days=i)).strftime("%Y%m%d")
            tickers = stock.get_market_ticker_list(check_date, market="ALL")
            if tickers: # 데이터가 있는 날을 찾으면 중단
                all_tickers = tickers
                latest_trading_day = check_date
                break
        
        if not latest_trading_day:
            print("❌ [에러] 최근 영업일 데이터를 불러올 수 없습니다.")
            return

        print(f"📅 분석 기준 영업일: {latest_trading_day} (확인된 종목수: {len(all_tickers)})")

        # 종목명 -> 티커 맵 생성
        ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}
        matched_results = []

        # 3. 분석 루프
        print(f"📊 {latest_trading_day} 종목 분석 시작...")
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            if ticker:
                try:
                    # 분석에 필요한 충분한 데이터 확보
                    df = stock.get_market_ohlcv_by_date("20240101", latest_trading_day, ticker)
                    if df is not None and len(df) >= 224:
                        # 이동평균 계산
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        current_close = df['종가'].iloc[-1]
                        
                        # 샌드위치 조건
                        if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                            theme = row[1] if len(row) > 1 else "미지정"
                            matched_results.append([name, theme])
                            print(f"✨ [포착] {name}")
                except:
                    continue
            
            # API 과부하 방지 (10개 종목마다 약간의 휴식)
            if i % 10 == 0:
                time.sleep(0.05)

        # 4. 결과 전송
        if matched_results:
            msg = f"<b>🔔 [분석 완료] {latest_trading_day}</b>\n총 {len(matched_results)}건 포착\n\n"
            for res in matched_results:
                msg += f"• <b>{res[0]}</b> | {res[1]}\n"
            send_telegram_msg(bot_token, chat_id, msg)
            print(f"✅ 전송 성공: {len(matched_results)}건")
        else:
            print("ℹ️ 조건 만족 종목 없음")
            # 작동 여부 확인을 위한 알림 (종목이 0건이어도 메시지 발송)
            send_telegram_msg(bot_token, chat_id, f"✅ {latest_trading_day} 분석 완료: 조건 만족 종목 없음")

    except Exception as e:
        print(f"❌ [에러] 전체 프로세스 오류: {e}")

if __name__ == "__main__":
    run_analysis()
