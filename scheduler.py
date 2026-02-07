import os
import json
import FinanceDataReader as fdr  # 안정적인 리스트 확보용
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
    print("🚀 [클라우드 최적화 모드] 분석을 시작합니다...")
    
    try:
        # 1. 환경 변수 및 시트 연결
        creds_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
        bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]
        
        gc = gspread.authorize(Credentials.from_service_account_info(
            creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        ))
        
        spreadsheet = gc.open("관심종목")
        rows = spreadsheet.get_worksheet(0).get_all_values()[1:]
        print(f"✅ 시트 연결 성공: {len(rows)}개 종목 로드")

        # 2. 종목 리스트 확보 (FinanceDataReader 사용 - GitHub에서 훨씬 안정적)
        print("🔍 시장 종목 리스트를 불러오는 중...")
        try:
            df_krx = fdr.StockListing('KRX')
            # 종목명(Name)을 키로, 종목코드(Code)를 값으로 하는 딕셔너리 생성
            ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
            print(f"✅ 시장 리스트 확보 성공 (총 {len(ticker_map)}개 종목)")
        except Exception as e:
            print(f"❌ 시장 리스트 확보 실패: {e}")
            return

        # 3. 날짜 설정 (최근 영업일 확인)
        # 오늘이 주말인 경우를 대비해 최근 데이터를 가져올 수 있는 날짜 확인
        now = datetime.now() + timedelta(hours=9)
        target_date = now.strftime("%Y%m%d")
        
        matched_results = []
        print(f"📊 분석 시작 (기준일: {target_date} 전후)")

        # 4. 분석 루프
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            if ticker:
                try:
                    # pykrx를 사용하여 OHLCV 데이터 수집
                    # 주말이면 pykrx가 알아서 최근 영업일 데이터를 가져옵니다.
                    df = stock.get_market_ohlcv_by_date("20240101", target_date, ticker)
                    
                    if df is not None and len(df) >= 224:
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        current_close = df['종가'].iloc[-1]
                        
                        # 샌드위치 조건 판별
                        if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                            theme = row[1] if len(row) > 1 else "미지정"
                            matched_results.append([name, theme])
                            print(f"✨ [포착] {name}")
                except:
                    continue
            
            # API 과부하 방지
            if i % 20 == 0:
                time.sleep(0.1)

        # 5. 결과 전송
        final_date_str = now.strftime("%Y-%m-%d %H:%M")
        if matched_results:
            msg = f"<b>🔔 [분석 완료] {final_date_str}</b>\n총 {len(matched_results)}건 포착\n\n"
            for res in matched_results:
                msg += f"• <b>{res[0]}</b> | {res[1]}\n"
            send_telegram_msg(bot_token, chat_id, msg)
            print(f"✅ {len(matched_results)}건 전송 성공")
        else:
            print("ℹ️ 조건 만족 종목 없음")
            send_telegram_msg(bot_token, chat_id, f"✅ {final_date_str} 분석 완료: 포착된 종목 없음")

    except Exception as e:
        print(f"❌ [에러] 전체 프로세스 오류: {e}")

if __name__ == "__main__":
    run_analysis()
