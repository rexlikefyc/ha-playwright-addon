# 導入必要的函式庫
# Import necessary libraries
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import calendar
import numpy as np
import time
import logging
import random
import json
import io
from bs4 import BeautifulSoup
import subprocess
import sys
import os # 新增 os 函式庫

# 確保 xlsxwriter 函式庫已安裝
# Ensure the xlsxwriter library is installed
try:
    import xlsxwriter
except ImportError:
    print("❌ 偵測到 xlsxwriter 函式庫未安裝，正在為您安裝...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xlsxwriter"])
    print("✅ xlsxwriter 函式庫安裝成功！")

# 檢查並建立目錄
# Check and create the directory
log_dir = "/config"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 設定日誌
# Set up logging to a file.
logging.basicConfig(filename="/config/scrape_log.txt", level=logging.INFO, encoding="utf-8", format="%(asctime)s - %(levelname)s - %(message)s")

def roc_to_ad(roc_date_str):
    """
    將民國日期字串轉換為西元日期物件。
    Converts a Republic of China (ROC) date string to a datetime object.
    Example: '113/05/20' -> 2024-05-20 00:00:00
    """
    try:
        y, m, d = map(int, roc_date_str.strip().split("/"))
        return datetime(y + 1911, m, d)
    except:
        return None

def get_latest_trade_date(year, month):
    """
    獲取指定年月的最後交易日。
    Gets the last trading day of a given year and month.
    """
    _, last_day = calendar.monthrange(year, month)
    end_date = datetime(year, month, last_day)
    while end_date.weekday() >= 5: # 5=週六, 6=週日
        end_date -= timedelta(days=1)
    return end_date

def fetch_tpex_stock_list():
    """
    取得上櫃股票清單。
    Fetches the list of TWEX (Taipei Exchange) listed stocks.
    """
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyMarktVal"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        logging.info(f"市值排行 API 回應: {data.get('date', '無日期')}，總股票數: {data.get('totalCount', '未知')}")
        if not data.get("tables"):
            logging.error("沒有找到表格數據")
            return None
        table = data["tables"][0]
        df = pd.DataFrame(table["data"], columns=table["fields"])
        df = df[df["股票代號"].notnull() & (df["股票代號"].astype(str).str.len() > 0)]
        df = df[["股票代號", "股票名稱"]].rename(columns={"股票代號": "stock_id", "股票名稱": "name"})
        logging.info(f"抓取到 {len(df)} 檔上櫃股票")
        return df
    except Exception as e:
        logging.error(f"股票清單抓取失敗: {e}")
        return None

def fetch_tpex_history(stock_id, year, month):
    """
    抓取指定年月的整月歷史資料。
    Fetches a full month's historical data for a specific stock.
    """
    date_str = f"{year}/{month:02d}/01"
    url = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock?response=&date={date_str}&code={stock_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("stat") != "ok":
            return None, f"API 回傳失敗: {data.get('stat')} ({year}/{month:02d})"
        table = data["tables"][0]
        df = pd.DataFrame(table["data"], columns=table["fields"])
        if df.empty:
            return None, f"資料為空 ({year}/{month:02d})"
        if "日 期" not in df.columns or "收盤" not in df.columns or "成交張數" not in df.columns or "開盤" not in df.columns:
            return None, f"缺少必要欄位: {df.columns} ({year}/{month:02d})"
        df["日期"] = df["日 期"].apply(roc_to_ad)
        df["收盤價"] = pd.to_numeric(df["收盤"].str.replace(",", ""), errors="coerce")
        df["開盤價"] = pd.to_numeric(df["開盤"].str.replace(",", ""), errors="coerce")
        df["成交張數"] = pd.to_numeric(df["成交張數"].str.replace(",", ""), errors="coerce")
        df = df[["日期", "收盤價", "開盤價", "成交張數"]].dropna().sort_values("日期")
        if df.empty:
            return None, f"資料處理後為空 ({year}/{month:02d})"
        return df, None
    except requests.exceptions.RequestException as e:
        return None, f"請求失敗: {e} ({year}/{month:02d})"

def fetch_industry_chain_info(stock_code):
    """
    從產業價值鏈平台網頁抓取公司所屬產業與產業鏈資訊，並處理編碼問題。
    Fetches the company's industry and industry chain information from the
    Industry Value Chain Platform website, handling encoding issues.
    """
    url = f"https://ic.tpex.org.tw/company_chain.php?stk_code={stock_code}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    industry_info = {
        "industry": "N/A",
        "industry_chain": "N/A"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        # 嘗試用 UTF-8 解碼，如果失敗則改用 cp950 (Big5 的 Windows 擴充)
        # Try to decode with UTF-8, if it fails, switch to cp950 (Windows extension of Big5)
        try:
            response.encoding = 'utf-8'
            response_text = response.text
        except UnicodeDecodeError:
            response.encoding = 'cp950'
            response_text = response.text
            
        soup = BeautifulSoup(response_text, 'html.parser')

        # 尋找所有包含產業鏈資訊的 <h4> 標籤
        # Find all <h4> tags that contain the industry chain information
        industry_elements = soup.select('div.content h4 a')

        if industry_elements:
            industries = []
            industry_chains = []
            for element in industry_elements:
                # 擷取所屬產業
                # Extract the industry
                industries.append(element.text.strip())
                
                # 擷取產業鏈，它位於 <a> 標籤的下一個兄弟節點
                # Extract the industry chain, which is the next sibling of the <a> tag
                industry_chain_text = element.next_sibling
                if industry_chain_text:
                    # 移除所有空白字元和 ">" 符號
                    # Remove all whitespace and ">" symbols
                    cleaned_text = industry_chain_text.strip().replace('>', '').replace('&gt;', '').strip()
                    industry_chains.append(cleaned_text)
            
            industry_info["industry"] = ", ".join(industries)
            industry_info["industry_chain"] = ", ".join(industry_chains)
        
        return industry_info

    except requests.exceptions.RequestException as e:
        logging.error(f"股票 {stock_code} 產業鏈網頁抓取失敗: {e}")
        return industry_info
    except Exception as e:
        logging.error(f"股票 {stock_code} 產業鏈資料解析失敗: {e}")
        return industry_info

def calculate_rsi(df, period=14):
    """
    使用 Wilder's 方法計算 RSI。
    Calculates the Relative Srength Index (RSI) using Wilder's method.
    """
    delta = df["收盤價"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    
    # 使用指數移動平均線 (EMA)
    # Use Exponential Moving Average (EMA)
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    # 處理分母為 0 的情況
    # Handle the case where the denominator is 0
    rsi = rsi.fillna(100)
    
    return rsi.round(2)

def analyze_tpex_stocks(year_month=None):
    """
    分析上櫃股票並篩選符合特定條件的股票。
    Analyzes TWEX stocks and filters them based on specific conditions.
    """
    if year_month is None:
        year_month = datetime.today().strftime("%Y/%m")
    try:
        year, month = map(int, year_month.split("/"))
    except:
        logging.error("年月格式錯誤，應為 YYYY/MM")
        print("❌ 年月格式錯誤，應為 YYYY/MM")
        return

    stock_df = fetch_tpex_stock_list()
    if stock_df is None or stock_df.empty:
        logging.error("無法獲取股票清單，程式終止")
        print("❌ 無法獲取股票清單，程式終止")
        return

    target_date = get_latest_trade_date(year, month)
    pre_filtered_results = []
    total = len(stock_df)
    logging.info(f"共需分析 {total} 檔股票")
    print(f"🔍 共需分析 {total} 檔股票")

    def process_stock(stock_info):
        stock_id, stock_name = stock_info
        df_all = []
        for m in range(2):
            target_year = year
            target_month = month - m
            if target_month <= 0:
                target_year -= 1
                target_month += 12
            df, reason = fetch_tpex_history(stock_id, target_year, target_month)
            if df is not None and not df.empty:
                df_all.append(df)
            else:
                logging.warning(f"股票 {stock_id} 歷史資料抓取失敗: {reason}")
        
        df_all = [df for df in df_all if not df.empty]
        if not df_all:
            logging.warning(f"股票 {stock_id} 無有效歷史資料")
            return None
        
        df = pd.concat(df_all, ignore_index=True).drop_duplicates(subset=["日期"]).sort_values("日期")
        
        if len(df) < 20:
            logging.warning(f"股票 {stock_id} 資料不足20天: {len(df)}")
            return None

        df = df[df["日期"] <= target_date].reset_index(drop=True)
        
        if len(df) < 20:
            logging.warning(f"股票 {stock_id} 篩選後資料不足20天: {len(df)}")
            return None

        df["MA5"] = df["收盤價"].rolling(window=5).mean().round(2)
        df["MA10"] = df["收盤價"].rolling(window=10).mean().round(2)
        df["MA20"] = df["收盤價"].rolling(window=20).mean().round(2)
        df["Volume_MA5"] = df["成交張數"].rolling(window=5).mean().round(0)
        df["Volume_MA10"] = df["成交張數"].rolling(window=10).mean().round(0)
        df["RSI"] = calculate_rsi(df, period=14)

        if len(df) < 3:
            logging.warning(f"股票 {stock_id} 數據不足以進行分析，至少需要3天")
            return None

        latest = df.iloc[-1]
        one_day_ago = df.iloc[-2]
        two_days_ago = df.iloc[-3]

        if pd.isna(latest["成交張數"]) or latest["成交張數"] < 200:
            logging.warning(f"股票 {stock_id} 成交量不足200張或無效: {latest['成交張數']}")
            return None

        ma10_minus_ma5_pct = ((latest["MA10"] - latest["MA5"]) / latest["MA5"] * 100) if not pd.isna(latest["MA5"]) and latest["MA5"] != 0 else float("inf")

        # --- 恢復交易量邏輯 (條件1-4) ---
        # --- Restored volume logic (for conditions 1-4) ---
        volume_condition_1_4_met = False
        volume_multiplier_1_4_output = 0.0
        for i in range(3):
            day_df = df.iloc[-1-i]
            if pd.isna(day_df["成交張數"]) or pd.isna(day_df["Volume_MA5"]) or pd.isna(day_df["Volume_MA10"]) or pd.isna(day_df["收盤價"]) or pd.isna(day_df["開盤價"]):
                continue
            
            # 檢查紅K線與成交量爆發條件
            # Check for red candle and volume breakout condition
            if (day_df["收盤價"] > day_df["開盤價"] and 
                day_df["成交張數"] >= day_df["Volume_MA5"] * 1.5 and 
                day_df["成交張數"] >= day_df["Volume_MA10"] * 1.5):
                
                # 計算倍數 (成交量 / max(5日均量, 10日均量))
                # Calculate multiplier (volume / max(5-day MA, 10-day MA))
                volume_multiplier_1_4_output = day_df["成交張數"] / max(day_df["Volume_MA5"], day_df["Volume_MA10"])
                volume_condition_1_4_met = True
                break # 找到符合條件的紅K，跳出迴圈

        # --- 交易量邏輯 (條件5) ---
        # --- Volume logic (for condition 5) ---
        volume_condition_5_met = False
        volume_multiplier_5_output = 0.0
        if not pd.isna(latest["成交張數"]) and not pd.isna(latest["Volume_MA5"]) and not pd.isna(latest["Volume_MA10"]):
            if (latest["成交張數"] >= latest["Volume_MA5"] * 2 and
                latest["成交張數"] >= latest["Volume_MA10"] * 2):
                volume_condition_5_met = True
                volume_multiplier_5_output = latest["成交張數"] / max(latest["Volume_MA5"], latest["Volume_MA10"])

        conditions_met = []
        
        # 條件1: MA10-MA5 差距 0-1%，且 MA5 < MA10 < MA20，且 MA5 上升，且 RSI >= 49
        if (volume_condition_1_4_met and
            0 <= ma10_minus_ma5_pct < 1 and
            not pd.isna(latest["MA5"]) and not pd.isna(latest["MA10"]) and not pd.isna(latest["MA20"]) and
            latest["MA5"] < latest["MA10"] and latest["MA10"] < latest["MA20"] and
            not pd.isna(one_day_ago["MA5"]) and
            latest["MA5"] > one_day_ago["MA5"] and
            not pd.isna(latest["RSI"]) and latest["RSI"] >= 49):
            conditions_met.append("條件1")

        # 條件2: MA10-MA5 差距 0-1%，且 MA5 < MA10 < MA20，且 MA5 上升，且 RSI < 49 但上升
        if (volume_condition_1_4_met and
            0 <= ma10_minus_ma5_pct < 1 and
            not pd.isna(latest["MA5"]) and not pd.isna(latest["MA10"]) and not pd.isna(latest["MA20"]) and
            latest["MA5"] < latest["MA10"] and latest["MA10"] < latest["MA20"] and
            not pd.isna(one_day_ago["MA5"]) and
            latest["MA5"] > one_day_ago["MA5"] and
            not pd.isna(latest["RSI"]) and not pd.isna(one_day_ago["RSI"]) and
            latest["RSI"] < 49 and latest["RSI"] > one_day_ago["RSI"]):
            conditions_met.append("條件2")

        # 條件3: MA5 > MA10，且 MA10 < MA5 < MA20，且前一日 MA5 <= MA10，且 RSI >= 50
        if (volume_condition_1_4_met and
            not pd.isna(latest["MA5"]) and not pd.isna(latest["MA10"]) and not pd.isna(latest["MA20"]) and
            latest["MA5"] > latest["MA10"] and
            latest["MA10"] < latest["MA5"] and latest["MA5"] < latest["MA20"] and
            not pd.isna(one_day_ago["MA5"]) and not pd.isna(one_day_ago["MA10"]) and
            one_day_ago["MA5"] <= one_day_ago["MA10"] and
            not pd.isna(latest["RSI"]) and latest["RSI"] >= 50):
            conditions_met.append("條件3")

        # 條件4: MA10 < MA5 < MA20，且 RSI >= 50
        if (volume_condition_1_4_met and
            not pd.isna(latest["MA5"]) and not pd.isna(latest["MA10"]) and not pd.isna(latest["MA20"]) and
            latest["MA10"] < latest["MA5"] and latest["MA5"] < latest["MA20"] and
            not pd.isna(latest["RSI"]) and latest["RSI"] >= 50):
            conditions_met.append("條件4")

        # 條件5: 當日 MA5 > MA20，且當日交易量超過5或10最多均量倍數 >= 2
        if (volume_condition_5_met and
            not pd.isna(latest["MA5"]) and not pd.isna(latest["MA20"]) and
            latest["MA5"] > latest["MA20"]):
            conditions_met.append("條件5")

        if not conditions_met:
            return None
            
        industry_info = fetch_industry_chain_info(stock_id)

        output_data = {
            "stock_id": stock_id,
            "stock_name": stock_name,
            "close_price": f"{latest['收盤價']:.2f}",
            "conditions_met": ", ".join(conditions_met),
            "industry": industry_info["industry"],
            "industry_chain": industry_info["industry_chain"],
            "ma10_minus_ma5": ((latest['MA10'] - latest['MA5']) / latest['MA5'] * 100) if not pd.isna(latest["MA5"]) and latest["MA5"] != 0 else float("inf")
        }

        if "條件5" in conditions_met:
            output_data["volume_multiplier"] = f"{volume_multiplier_5_output:.2f}"
        else:
            output_data["volume_multiplier"] = f"{volume_multiplier_1_4_output:.2f}"
            
        logging.info(f"股票 {stock_id} 符合條件: {output_data['conditions_met']}")
        return output_data

    stock_list = list(stock_df[["stock_id", "name"]].itertuples(index=False, name=None))
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        pre_filtered_results = list(executor.map(process_stock, stock_list))

    condition5_stocks = []
    other_stocks = []

    for r in pre_filtered_results:
        if r is not None:
            if "條件5" in r.get("conditions_met", ""):
                condition5_stocks.append(r)
            else:
                other_stocks.append(r)

    final_columns = [
        "股票代號",
        "股票名稱",
        "當日收盤價",
        "交易量超過5或10最多均量的倍數",
        "符合條件",
        "所屬產業",
        "產業鏈"
    ]
    
    # 產出「其他」符合條件的股票檔案
    if other_stocks:
        other_df = pd.DataFrame(other_stocks)
        other_df = other_df.sort_values(by=["ma10_minus_ma5", "volume_multiplier"], ascending=[True, False])
        other_df = other_df[[
            "stock_id", "stock_name", "close_price", "volume_multiplier", "conditions_met",
            "industry", "industry_chain"
        ]]
        other_df.columns = final_columns
        output_file_other = f"/config/filtered_tpex_stocks_others_{year}_{month:02d}.csv"
        other_df.to_csv(output_file_other, index=False, encoding="utf-8-sig")
        logging.info(f"其他符合條件股票已輸出至 {output_file_other}，共 {len(other_stocks)} 檔")
        print(f"✅ 其他符合條件股票共 {len(other_stocks)} 檔")
    else:
        output_file_other = f"/config/filtered_tpex_stocks_others_{year}_{month:02d}.csv"
        with open(output_file_other, "w", encoding="utf-8-sig") as f:
            f.write("股票代號,股票名稱,當日收盤價,交易量超過5或10最多均量的倍數,符合條件,所屬產業,產業鏈\n")
            f.write("無其他符合條件的股票")
        logging.info("無其他符合條件的股票")
        print("✅ 無其他符合條件的股票")

    # 產出「條件五」符合條件的股票檔案
    if condition5_stocks:
        condition5_df = pd.DataFrame(condition5_stocks)
        condition5_df = condition5_df.sort_values(by="volume_multiplier", ascending=False)
        condition5_df = condition5_df[[
            "stock_id", "stock_name", "close_price", "volume_multiplier", "conditions_met",
            "industry", "industry_chain"
        ]]
        condition5_df.columns = final_columns
        output_file_condition5 = f"/config/filtered_tpex_stocks_condition5_{year}_{month:02d}.csv"
        condition5_df.to_csv(output_file_condition5, index=False, encoding="utf-8-sig")
        logging.info(f"符合條件五的股票已輸出至 {output_file_condition5}，共 {len(condition5_stocks)} 檔")
        print(f"✅ 符合條件五的股票共 {len(condition5_stocks)} 檔")
    else:
        output_file_condition5 = f"/config/filtered_tpex_stocks_condition5_{year}_{month:02d}.csv"
        with open(output_file_condition5, "w", encoding="utf-8-sig") as f:
            f.write("股票代號,股票名稱,當日收盤價,交易量超過5或10最多均量的倍數,符合條件,所屬產業,產業鏈\n")
            f.write("無符合條件五的股票")
        logging.info("無符合條件五的股票")
        print("✅ 無符合條件五的股票")

    logging.info(f"分析完成，結果輸出至 {output_file_other} 與 {output_file_condition5}")
    print(f"✅ 分析完成，結果輸出至 {output_file_other} 與 {output_file_condition5}")
    

if __name__ == "__main__":
    start_time = time.time()
    analyze_tpex_stocks()
    end_time = time.time()
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")
