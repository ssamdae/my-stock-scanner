import os
import json
import FinanceDataReader as fdr
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
    print("🚀 [테마 정렬 모드] 분석을 시작합니다...")
    
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

        # 2. 시장 리스트 확보
        df_krx = fdr.StockListing('KRX')
        ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()

        # 3. 분석 루프
        now = datetime.now() + timedelta(hours=9)
        target_date = now.strftime("%Y%m%d")
        matched_results = []

        print(f"📊 분석 진행 중...")
        for i, row in enumerate(rows):
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date("20240101", target_date, ticker)
                    if df is not None and len(df) >= 224:
                        ma120 = df['종가'].rolling(window=120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(window=224).mean().iloc[-1]
                        current_close = df['종가'].iloc[-1]
                        
                        if (ma224 < current_close < ma120) or (ma120 < current_close < ma224):
                            # 테마 정보가 있으면 가져오고 없으면 '미분류' 처리
                            theme1 = row[1] if len(row) > 1 and row[1] else "미분류"
                            matched_results.append({'종목명': name, '테마1': theme1})
                except:
                    continue
            
            if i % 20 == 0: time.sleep(0.05)

        # 4. 결과 정렬 및 메시지 생성
        if matched_results:
            # 리스트를 데이터프레임으로 변환
            res_df = pd.DataFrame(matched_results)
            
            # 테마1의 빈도수 계산 및 정렬
            theme_counts = res_df['테마1'].value_counts()
            res_df['빈도수'] = res_df['테마1'].map(theme_counts)
            
            # 정렬 순서: 1. 빈도수(내림차순) -> 2. 테마명(오름차순) -> 3. 종목명(오름차순)
            res_df = res_df.sort_values(by=['빈도수', '테마1', '종목명'], ascending=[False, True, True])
            
            # 텔레그램 메시지 구성
            final_date_str = now.strftime("%Y-%m-%d %H:%M")
            msg = f"<b>🔔 [분석 완료] {final_date_str}</b>\n"
            msg += f"포착된 종목: <b>{len(res_df)}건</b>\n"
            msg += f"<i>(많이 포착된 테마 순 정렬)</i>\n\n"
            
            current_theme = ""
            for _, r in res_df.iterrows():
                # 테마가 바뀔 때마다 구분선이나 강조 추가 가능 (선택 사항)
                msg += f"• <b>{r['종목명']}</b> | {r['테마1']}\n"
            
            send_telegram_msg(bot_token, chat_id, msg)
            print(f"✅ {len(res_df)}건 정렬 전송 완료")
        else:
            print("ℹ️ 포착 종목 없음")
            # 필요 시 결과 없음 알림 주석 해제
            # send_telegram_msg(bot_token, chat_id, f"✅ {target_date} 분석 완료: 포착된 종목 없음")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    run_analysis()
