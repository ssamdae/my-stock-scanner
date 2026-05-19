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

# 💡 [초강력 필터 보정] 국내 모든 ETF 브랜드 및 파생상품 키워드 완벽 차단
def get_all_tickers_naver():
    tickers = []
    
    # 🚫 차단할 키워드 블랙리스트 (운용사 브랜드 + 파생/리츠 용어 총망라)
    blacklist = (
        "KODEX", "TIGER", "RISE", "ACE", "ARIRANG", "KBSTAR", 
        "HANARO", "KOSEF", "SOL", "KOACT", "TIMEFOLIO", "PLUS", 
        "WON", "마이티", "히어로즈", "TREX", "UNICORN", 
        "ETF", "ETN", "리츠", "REIT", "스팩", "인프라", "펀드", "투자회사",
        "(H)", "(합성)", "레버리지", "인버스", "선물", "콜", "풋"
    )

    # sosok 0: KOSPI, 1: KOSDAQ
    for sosok in [0, 1]:
        for page in range(1, 45): 
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, timeout=5)
                soup = BeautifulSoup(res.text, 'html.parser')
                links = soup.select('a.tltle')
                
                if not links:
                    break
                    
                for link in links:
                    href = link.get('href')
                    if href and 'code=' in href:
                        ticker = href.split('code=')[-1]
                        name = link.text.strip()
                        
                        # ❌ [1차 방어벽] 우선주 제거
                        if name.endswith("우") or name.endswith("우B") or name.endswith("우C"): 
                            continue
                            
                        # ❌ [2차 방어벽] 블랙리스트 단어가 이름에 포함되면 즉시 제외
                        if any(bad_word in name for bad_word in blacklist):
                            continue
                        
                        tickers.append((ticker, name))
            except Exception:
                continue
    return tickers

# 네이버 fchart 분석 워커
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
                '거래량비율': float(vol_ratio)
            }
    except Exception:
        pass
    return None

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 코스피/코스닥 순수 주식 스캔 가동...")
    secrets = get_secrets()
    token = secrets.get("bot_token")
    chat_id = secrets.get("chat_id")
    
    market_tickers = get_all_tickers_naver()
    if not market_tickers:
        print("❌ 네이버 금융에서 종목 리스트를 가져오지 못했습니다.")
        return
        
    print(f"✅ 총 {len(market_tickers)}개 순수 주식 종목 수집 완료. 정밀 이평선 연산 시작...")

    matched = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(analyze_single_stock_naver, item): item for item in market_tickers}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    matched.append(result)
            except Exception:
                pass

    today_str = datetime.now().strftime('%Y-%m-%d')

    if matched:
        res_df = pd.DataFrame(matched)
        res_df = res_df.sort_values(by='거래량비율', ascending=False)
        
        msg = f"<b>⏰ [순수 주식 자동 스캔 리포트: {today_str}]</b>\n"
        msg += f"상단 이평선 돌파 + 거래량 200%↑ (ETF/ETN/리츠/우선주 제외)\n"
        msg += f"총 <b>{len(res_df)}건</b>\n\n"
        
        for _, r in res_df.iterrows():
            msg += f"• <b>{r['종목명']}</b> (🔥{r['거래량비율']:.1f}%)\n"
        
        if send_telegram_msg(msg, token, chat_id):
            print(f"✅ 텔레그램 알림 발송 완료 ({len(res_df)}건)")
        else:
            print("❌ 텔레그램 알림 발송 실패")
            
    else:
        msg = f"<b>⏰ [순수 주식 자동 스캔 리포트: {today_str}]</b>\n"
        msg += f"상단 이평선 돌파 + 거래량 200%↑ (ETF/ETN/리츠/우선주 제외)\n\n"
        msg += "💡 현재 조건에 일치하는 돌파 종목이 존재하지 않습니다."
        
        if send_telegram_msg(msg, token, chat_id):
            print("✅ 텔레그램 알림 발송 완료 (0건 - 안심 보고)")
        else:
            print("❌ 텔레그램 알림 발송 실패")

if __name__ == "__main__":
    main()
