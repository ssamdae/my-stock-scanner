import os
import json
import time
import requests
import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

def run_analysis():
    try:
        # 1. 환경 설정 및 인증 (GitHub Secrets 활용)
        #
        creds_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
        creds = Credentials.from_service_account_info(
            creds_json, 
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(creds)
        
        # 구글 시트 로드 (파일명: 관심종목)
        #
        worksheet = gc.open("관심종목").get_worksheet(0)
        rows = worksheet.get_all_values()[1:]
        
        # KST 시간 설정 (GitHub Actions 서버 기준 +9시간)
        now = datetime.utcnow() + timedelta(hours=9)
        
        # 2. 이중 백업 로직: 데이터가 존재하는 최신 영업일 및 종목 리스트 확보
        ticker_map = {}
        valid_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        
        # 1차 시도: pykrx로 최근 7일 탐색
        for i in range(7):
            check_d = (now - timedelta(days=i)).strftime("%Y%m%d")
            temp_tickers = stock.get_market_ticker_list(date=check_d, market="ALL")
            if temp_tickers:
                ticker_map = {stock.get_market_ticker_name(t): t for t in temp_tickers}
                valid_date = check_d
                break
        
        # 2차 시도: pykrx 실패 시 FinanceDataReader로 보완
        if not ticker_map:
            print("⚠️ pykrx 데이터 로드 실패, FinanceDataReader로 전환합니다.")
            df_krx = fdr.StockListing('KRX')
            ticker_map = pd.Series(df_krx.Code.values, index=df_krx.Name).to_dict()
            valid_date = now.strftime("%Y%m%d")

        if not ticker_map:
            raise Exception("모든 데이터 소스에서 종목 정보를 가져오는 데 실패했습니다.")

        # 3. 분석 루프 시작
        matched = []
        start_date = (now - timedelta(days=450)).strftime("%Y%m%d")

        for row in rows:
            name = row[0].strip()
            ticker = ticker_map.get(name)
            
            if ticker:
                try:
                    df = stock.get_market_ohlcv_by_date(start_date, valid_date, ticker)
                    if len(df) >= 224:
                        # 샌드위치 분석 로직: 120일선과 224일선 사이 포착
                        #
                        ma120 = df['종가'].rolling(120).mean().iloc[-1]
                        ma224 = df['종가'].rolling(224).mean().iloc[-1]
                        close = df['종가'].iloc[-1]

                        # 샌드위치 조건식: (MA_224 < 현재가 < MA_120) OR (MA_120 < 현재가 < MA_224)
                        if (ma224 < close < ma120) or (ma120 < close < ma224):
                            matched.append({
                                'name': name, 
                                'theme': row[1] if len(row) > 1 else "미분류",
                                'price': int(close)
                            })
                    time.sleep(0.1) # 서버 부하 방지
                except:
                    continue

        # 4. 결과 정리 및 텔레그램 전송
        if matched:
            res_df = pd.DataFrame(matched)
            # 테마 빈도수 기준 내림차순 정렬
            #
            counts = res_df['theme'].value_counts()
            res_df['cnt'] = res_df['theme'].map(counts)
            res_df = res_df.sort_values(by=['cnt', 'theme', 'name'], ascending=[False, True, True])
            
            msg = f"<b>🔔 [샌드위치 정기 리포트] {valid_date}</b>\n"
            msg += f"포착된 종목: <b>{len(res_df)}건</b>\n\n"
            
            for _, r in res_df.iterrows():
                msg += f"• <b>{r['name']}</b> | {r['theme']} ({r['price']:,}원)\n"
            
            # 텔레그램 전송 (메시지 길이 제한 방어: 최대 4000자)
            token = os.environ['TELEGRAM_BOT_TOKEN']
            chat_id = os.environ['TELEGRAM_CHAT_ID']
            send_url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(send_url, data={"chat_id": chat_id, "text": msg[:4000], "parse_mode": "HTML"}, timeout=10)
            
            print(f"✅ 리포트 발송 완료: {len(res_df)}건")
        else:
            print(f"ℹ️ {valid_date} 기준 조건에 맞는 종목이 없습니다.")

    except Exception as e:
        error_msg = f"❌ 스케줄러 실행 중 오류 발생: {str(e)}"
        print(error_msg)
        # 중요 오류 발생 시 텔레그램 알림 (선택 사항)
        try:
            requests.post(f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendMessage", 
                          data={"chat_id": os.environ['TELEGRAM_CHAT_ID'], "text": error_msg})
        except: pass

if __name__ == "__main__":
    run_analysis()
