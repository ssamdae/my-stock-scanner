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
    requests.post(url, data=payload)

def run_analysis():
    print("🚀 진단 모드로 분석을 시작합니다...")
    
    try:
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
        print(f"✅ 시트 연결 성공 (종목수: {len(rows)})")
        
        # 분석 기준일 설정 (안전하게 최근 10일치 데이터를 가져와서 마지막 영업일 확인)
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=500)).strftime("%Y%m%d")
        
        # 티커 맵핑 (오늘 날짜 실패 대비)
        try:
            all_tickers = stock.get_market_ticker_list(end_date, market="ALL")
        except:
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            all_tickers = stock.get_market_ticker_list(yesterday, market="ALL")
        
        ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}
        matched_results = []

        print("📊 [데이터 검증 시작]")
        for i, row in enumerate(rows):
            name = row[0]
            ticker = ticker_map.get(name)
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
                    if df is not None and not df.empty and len(df) >= 224:
                        # 이동평균선 계산 (단순 이동평균)
                        ma120 = round(df['종가'].rolling(window=120).mean().iloc[-1], 2)
                        ma224 = round(df['종가'].rolling(window=224).mean().iloc[-1], 2)
                        current_close = df['종가'].iloc[-1]
                        
                        # 샌드위치 조건
                        is_matched = (ma224 < current_close < ma120) or (ma120 < current_close < ma224)
                        
                        # 상위 5개 종목은 무조건 로그에 수치 출력 (진단용)
                        if i < 5:
                            print(f"🔍 {name}: 현재가({current_close}) | MA120({ma120}) | MA224({ma224}) -> 조건일치: {is_matched}")
                        
                        if is_matched:
                            matched_results.append([name, row[1] if len(row)>1 else ""])
                    elif i < 5:
                        print(f"⚠️ {name}: 데이터 부족 (전체 {len(df) if df is not None else 0}일치만 있음)")
                    
                    time.sleep(0.1) # 서버 과부하 방지
                except Exception as e:
                    continue

        if matched_results:
            msg = f"<b>🔔 [정기 분석] {end_date}</b>\n총 {len(matched_results)}건 포착\n\n"
            for res in matched_results:
                msg += f"• <b>{res[0]}</b> | {res[1]}\n"
            send_telegram_msg(bot_token, chat_id, msg)
            print(f"✅ {len(matched_results)}건 전송 성공!")
        else:
            print("ℹ️ 최종 분석 결과: 조건 만족 종목 없음")
            # 테스트용: 알림이 오는지 확인하기 위해 봇에게 생존 신고
            send_telegram_msg(bot_token, chat_id, f"✅ {end_date} 분석 완료 (결과 0건)")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    run_analysis()
