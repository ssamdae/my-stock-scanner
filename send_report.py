import os
import requests
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# 스트림릿에 등록해둔 텔레그램 비밀키를 그대로 공유해서 읽어오는 함수
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

# 텔레그램 전송 함수
def send_telegram_msg(message, token, chat_id):
    if not token or not chat_id:
        print("❌ 텔레그램 토큰 또는 챗 ID가 등록되어 있지 않습니다.")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        res = requests.post(url, data=payload, timeout=5)
        return res.status_code == 200
    except Exception as e:
        print(f"❌ 텔레그램 전송 중 오류 발생: {str(e)}")
        return False

# 네이버 fchart 분석 워커
def analyze_single_stock_naver(row):
    if not row or not row[0]: return None
    ticker = str(row[0]).strip()
    if len(ticker) != 6 or not ticker.isdigit(): return None
    name = row[1].strip()
    
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
                '거래량비율': f"{vol_ratio:.1f}%",
                '테마1': row[2].strip() if len(row) > 2 else "",
                '테마2': row[3].strip() if len(row) > 3 else "",
                '테마3': row[4].strip() if len(row) > 4 else ""
            }
    except Exception:
        pass
    return None

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 스케줄러 자동 스캔 가동...")
    secrets = get_secrets()
    token = secrets.get("bot_token")
    chat_id = secrets.get("chat_id")
    
    csv_path = os.path.expanduser("~/my-stock-scanner/watchlist.csv")
    if not os.path.exists(csv_path):
        print("❌ 스캐너 실행 실패: watchlist.csv 파일이 없습니다.")
        return
        
    df_stocks = pd.read_csv(csv_path, dtype={'티커': str}, encoding='utf-8-sig').fillna('')
    rows = df_stocks.values.tolist()
    
    unique_rows = []
    seen_tickers = set()
    for row in rows:
        if not row or not row[0]: continue
        ticker = str(row[0]).strip()
        if len(ticker) == 6 and ticker.isdigit() and ticker not in seen_tickers:
            seen_tickers.add(ticker)
            unique_rows.append(row)
            
    matched = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(analyze_single_stock_naver, r): r for r in unique_rows}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    matched.append(result)
            except Exception:
                pass

    if matched:
        res_df = pd.DataFrame(matched)
        for t in ['테마1', '테마2', '테마3']:
            counts = res_df[res_df[t] != ''][t].value_counts()
            res_df[f'{t}_빈도'] = res_df[t].map(counts).fillna(0)
        
        # 💡 [오타 완벽 수정] 한자 '度'를 한글 '도'로 완전히 박멸하고 정렬 기준을 웹(app.py)과 백인해 정렬을 맞췄습니다.
        res_df = res_df.sort_values(by=['테마1_빈도', '테마1', '테마2_빈도', '테마2', '테마3_빈도', '종목명'], ascending=[False, True, False, True, False, True])
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        msg = f"<b>⏰ [정시 자동 스캔 리포트: {today_str}]</b>\n"
        msg += f"상단 이평선 돌파 + 거래량 200%↑ 포착 결과\n"
        msg += f"총 <b>{len(res_df)}건</b>\n\n"
        
        for _, r in res_df.iterrows():
            theme_list = [t for t in [r['테마1'], r['테마2'], r['테마3']] if t.strip()]
            theme_str = ", ".join(theme_list)
            msg += f"• <b>{r['종목명']}</b> (🔥{r['거래량비율']}) | {theme_str}\n"
        
        if send_telegram_msg(msg, token, chat_id):
            print(f"✅ 텔레그램 알림 발송 완료 ({len(res_df)}건)")
        else:
            print("❌ 텔레그램 알림 발송 실패")
    else:
        print("💡 조건에 일치하는 돌파 종목이 존재하지 않습니다.")

if __name__ == "__main__":
    main()
