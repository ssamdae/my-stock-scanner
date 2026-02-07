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
    print("🚀 [최종 해결 모드] 분석을 시작합니다...")
    
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

        # 2. 가장 최근 영업일 자동 확보 (중요!)
        # 주말이나 공휴일에도 마지막으로 장이 열린 날짜를 정확히 가져옵니다.
        today_str = (datetime.now() + timedelta(hours=9)).strftime("%Y%m%d") # KST 보정
        latest_trading_day = stock.get_nearest_business_day_in_range(
            (datetime.now() - timedelta(days=10)).strftime("%Y%m%d"), 
            today_str
        )
        print(f"📅 분석 기준 영업일: {latest_trading_day}")

        # 해당 날짜의 전체 티커 리스트 확보
        all_tickers = stock.get_market_ticker_list(latest_trading_day, market="ALL")
        
        # 만약 ALL에서 실패하면 KOSPI, KOSDAQ 각각 시도
        if not all_tickers:
            print("⚠️ ALL 리스트 실패, KOSPI/KOSDAQ 개별 시도 중...")
            kospi = stock.get_market_ticker_list(latest_trading_day, market="KOSPI")
            kosdaq = stock.get_market_ticker_list(latest_trading_day, market="KOSDAQ")
            all_tickers = kospi + kosdaq

        if not all_tickers:
            print("❌ [최종 에러] 시장 리스트를 불러올 수 없습니다. 거래소 서버 응답 없음.")
            return

        # 종목명 -> 티커 맵 생성
        ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}
        matched_results = []

        print(f"📊 {latest_trading_day} 데이터 분석 시작...")
        
        # 3. 분석 루프
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            if ticker:
                try:
                    # 샌드위치 분석에 필요한 충분한 데이터(약 1년치) 확보
                    df = stock.get_market_ohlcv_by_date("20240101", latest_trading_day, ticker)
                    if df is not None and len(df) >= 224:
                        # 단순 이동평균 계산
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
            
            # API 과부하 방지 (잠시 대기)
            if i % 10 == 0:
                time.sleep(0.1)

        # 4. 결과 전송
        if matched_results:
            msg = f"<b>🔔 [분석 완료] {latest_trading_day}</b>\n총 {len(matched_results)}건 포착\n\n"
            for res in matched_results:
                msg += f"• <b>{res[0]}</b> | {res[1]}\n"
            send_telegram_msg(bot_token, chat_id, msg)
            print(f"✅ 전송 성공: {len(matched_results)}건")
        else:
            print("ℹ️ 조건 만족 종목 없음")
            send_telegram_msg(bot_token, chat_id, f"✅ {latest_trading_day} 분석 완료: 조건 만족 종목 0건")

    except Exception as e:
        print(f"❌ [에러] 전체 프로세스 오류: {e}")

if __name__ == "__main__":
    run_analysis()
