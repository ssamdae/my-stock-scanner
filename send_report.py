import os
import requests
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. 텔레그램 비밀키 호출
def get_secrets():
    secrets = {}
    path = os.path.expanduser("~/my-stock-scanner/.streamlit/secrets.toml")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    k, v = line.split("=", 1)
                    secrets[k.strip()] = v.strip().strip('"').strip("'")
    return secrets

def send_telegram_msg(message, token, chat_id):
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        res = requests.post(url, data=payload, timeout=5)
        return res.status_code == 200
    except Exception:
        return False

# 💡 [핵심 추가] 네이버 금융에서 KOSPI/KOSDAQ 전체 종목 실시간 스크래핑
def get_all_tickers_naver():
    tickers = []
    # sosok 0: KOSPI, 1: KOSDAQ
    for sosok in [0, 1]:
        for page in range(1, 45): # 시가총액 페이지 순회 (보통 40페이지 내외)
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, timeout=5)
                soup = BeautifulSoup(res.text, 'html.parser')
                links = soup.select('a.tltle')
                
                # 해당 페이지에 종목 링크가 없으면 마지막 페이지로 간주하고 다음 시장으로 넘어감
                if not links:
                    break
                    
                for link in links:
                    href = link.get('href')
                    if href and 'code=' in href:
                        ticker = href.split('code=')[-1]
                        name = link.text.strip()
                        # 스팩주, 우선주 등 기본 필터링 (필요시 정교화 가능)
                        if "스팩" not in name and not name.endswith("우") and not name.endswith("우B"):
                            tickers.append((ticker, name))
            except Exception:
                continue
    return tickers

# 2. 네이버 fchart 분석 워커 (테마 제외)
def analyze_single_stock_naver(item):
    ticker, name = item
    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={ticker}&timeframe=day&count=360&requestType=0"
    try:
        res = requests.get(url, timeout=2)
        if res.status_code != 200: return None
        
        root = ET.fromstring(res.text)
        items = root.findall('.//item')
        if len(items) < 224: return None
        
        data_list = []
        for item in items:
            data_str = item.get('data')
            parts = data_str.split('|')
            if len(parts) == 6:
                data_list.append({
                    'Close': float(parts[4]),
                    'Volume': float(parts[5])
                })
        
        df_stock = pd.DataFrame(data_list)
        if len(df_stock) < 224: return None
        
        close_series = df_stock['Close']
        vol_series = df_stock['Volume']

        ma120 = close_series.rolling(120).mean().iloc[-1]
        ma224 = close_series.rolling(224).mean().iloc[-1]
        upper_ma = max(ma120, ma224)

        prev_close = close_series.iloc[-2]
        last_close = close_series.iloc[-1]
        
        prev_vol = vol_series.iloc[-2]
        last_vol = vol_series.iloc[-1]
        vol_ratio = (last_vol / prev_vol * 100) if prev_vol > 0 else 0

        is_breakout = prev_close < upper_ma < last_close
        is_vol_surge = vol_ratio >= 200

        if is_breakout and is_vol_surge:
            return {
                '종목명': name, 
                '현재가': f"{int(last_close):,}",
                '거래량비율': float(vol_ratio)  # 정렬을 위해 float 타입으로 저장
            }
    except Exception:
        pass
    return None

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] KOSPI/KOSDAQ 전 종목 스캔 가동...")
    secrets = get_secrets()
    token = secrets.get("bot_token")
    chat_id = secrets.get("chat_id")
    
    # 동적 전체 종목 수집
    market_tickers = get_all_tickers_naver()
    if not market_tickers:
        print("❌ 네이버 금융에서 종목 리스트를 가져오지 못했습니다.")
        return
        
    print(f"✅ 총 {len(market_tickers)}개 종목 수집 완료. 정밀 이평선 연산 시작...")

    matched = []
    # 2,600개 종목이므로 스레드를 20개로 소폭 늘려 속도 최적화
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(analyze_single_stock_naver, item): item for item in market_tickers}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    matched.append(result)
            except Exception:
                pass

    if matched:
        res_df = pd.DataFrame(matched)
        
        # 거래량이 가장 폭발적으로 터진 종목부터 내림차순 정렬
        res_df = res_df.sort_values(by='거래량비율', ascending=False)
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        msg = f"<b>⏰ [전 종목 자동 스캔 리포트: {today_str}]</b>\n"
        msg += f"상단 이평선 돌파 + 거래량 200%↑ (스팩/우선주 제외)\n"
        msg += f"총 <b>{len(res_df)}건</b>\n\n"
        
        for _, r in res_df.iterrows():
            msg += f"• <b>{r['종목명']}</b> (🔥{r['거래량비율']:.1f}%)\n"
        
        if send_telegram_msg(msg, token, chat_id):
            print(f"✅ 텔레그램 알림 발송 완료 ({len(res_df)}건)")
        else:
            print("❌ 텔레그램 알림 발송 실패")
    else:
        print("💡 조건에 일치하는 돌파 종목이 존재하지 않습니다.")

if __name__ == "__main__":
    main()
