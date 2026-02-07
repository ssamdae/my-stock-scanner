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
    except:
        pass

def run_analysis():
    print("🚀 [초정밀 진단 모드] 분석을 시작합니다...")
    
    try:
        # 1. 환경 변수 로드
        creds_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
        bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]
        
        # 2. 구글 시트 연결
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open("관심종목")
        worksheet = spreadsheet.get_worksheet(0)
        all_data = worksheet.get_all_values()
        rows = all_data[1:]
        print(f"✅ 시트 연결 성공: 총 {len(rows)}개 종목 로드됨")
        
        # 3. 날짜 설정 (최근 영업일 기준 데이터 확보)
        # 오늘이 주말이면 금요일 데이터를 가져오기 위해 최근 7일 내의 가장 가까운 영업일 확인
        target_date = datetime.now().strftime("%Y%m%d")
        all_tickers = []
        
        # 최근 5일 중 데이터를 가져올 수 있는 가장 가까운 날짜 찾기
        for d in range(5):
            check_date = (datetime.now() - timedelta(days=d)).strftime("%Y%m%d")
            all_tickers = stock.get_market_ticker_list(check_date, market="ALL")
            if len(all_tickers) > 0:
                print(f"✅ {check_date} 기준 종목 리스트 확보 성공 (총 {len(all_tickers)}개 종목)")
                target_date = check_date
                break
        
        if not all_tickers:
            print("❌ [에러] 시장 종목 리스트를 불러올 수 없습니다.")
            return

        ticker_map = {stock.get_market_ticker_name(t): t for t in all_tickers}
        matched_results = []

        print(f"📊 [데이터 대조 시작] 분석 기준일: {target_date}")
        
        for i, row in enumerate(rows):
            # 종목명 앞뒤 공백 제거 (매우 중요!)
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            # 상위 5개 종목 대조 상태 로그 출력
            if i < 5:
                if ticker:
                    print(f"🔍 {name}: 티커[{ticker}] 매칭 성공")
                else:
                    print(f"❓ {name}: 시장 리스트에서 찾을 수 없음 (시트 오타 확인 필요)")

            if ticker:
                try:
                    # 500일치 데이터를 가져와서 이평선 계산
                    df = stock.get_market_ohlcv_by_date("20240101", target_date, ticker)
                    if df is not None and len(df) >= 224:
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        current_close = df['종가'].iloc[-1]
                        
                        # 샌드위치 조건 (수치 소수점 처리)
                        if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                            theme = row[1] if len(row) > 1 else "미지정"
                            matched_results.append([name, theme])
                            print(f"✨ [포착] {name}: 조건 일치!")
                except Exception as e:
                    continue
            
            # 325개 종목을 매번 호출하면 차단될 수 있어 0.05초 간격 유지
            time.sleep(0.05)

        # 4. 결과 보고
        if matched_results:
            msg = f"<b>🔔 [분석 완료] {target_date}</b>\n총 {len(matched_results)}건 포착\n\n"
            for res in matched_results:
                msg += f"• <b>{res[0]}</b> | {res[1]}\n"
            send_telegram_msg(bot_token, chat_id, msg)
            print(f"✅ 전송 성공: {len(matched_results)}건")
        else:
            print("ℹ️ 최종 분석 결과: 조건 만족 종목 없음")
            # 텔레그램 생존 신고 (결과가 0건이라도 왔는지 확인용)
            send_telegram_msg(bot_token, chat_id, f"✅ {target_date} 분석 완료: 조건 만족 종목 0건")

    except Exception as e:
        print(f"❌ [에러] 전체 프로세스 오류: {e}")

if __name__ == "__main__":
    run_analysis()
