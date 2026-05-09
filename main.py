from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import uvicorn
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import os
import webbrowser
import threading

app = FastAPI(title="FinDash API")

# Base directory for ticker files
TICKER_DIR = Path(__file__).parent

# Market file mapping
MARKET_FILES = {
    "sp500": "sp500.txt",
    "nasdaq": "nasdaq.txt",
    "dow": "dow.txt",
    "ftse100": "ftse100.txt",
    "bist": "xu100.txt",
}

def load_tickers_from_file(market: str) -> list[str]:
    """Read ticker symbols from a market .txt file"""
    filename = MARKET_FILES.get(market.lower())
    if not filename:
        return []
    filepath = TICKER_DIR / filename
    if not filepath.exists():
        return []
    with open(filepath, "r") as f:
        return [line.strip() for line in f if line.strip()]

def save_tickers_to_file(market: str, tickers: list[str]):
    """Write ticker symbols to a market .txt file"""
    filename = MARKET_FILES.get(market.lower())
    if not filename:
        return
    filepath = TICKER_DIR / filename
    with open(filepath, "w") as f:
        f.write("\n".join(tickers))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/market-summary")
def get_market_summary():
    """Fetch quick summary of major indices/assets"""
    tickers = ["^GSPC", "^IXIC", "^DJI", "GC=F", "BTC-USD"]
    summary = []
    names = {
        "^GSPC": "S&P 500",
        "^IXIC": "Nasdaq",
        "^DJI": "Dow",
        "GC=F": "Gold",
        "BTC-USD": "Bitcoin"
    }
    for t in tickers:
        try:
            stock = yf.Ticker(t)
            hist = stock.history(period="5d")
            if len(hist) >= 2:
                current = float(hist['Close'].iloc[-1])
                prev = float(hist['Close'].iloc[-2])
                change = current - prev
                change_pct = (change / prev) * 100
                summary.append({
                    "symbol": t,
                    "name": names.get(t, t),
                    "price": current,
                    "change": change,
                    "change_pct": change_pct
                })
        except:
            pass
    return summary

@app.get("/api/markets")
def get_markets(category: str = "cryptocurrencies"):
    categories = {
        "us_markets": {"^GSPC": "S&P 500", "^IXIC": "Nasdaq", "^DJI": "Dow Jones", "RUT": "Russell 2000"},
        "europe_markets": {"^FTSE": "FTSE 100", "^GDAXI": "DAX", "^FCHI": "CAC 40", "^STOXX50E": "EURO STOXX 50"},
        "cryptocurrencies": {"BTC-USD": "Bitcoin", "ETH-USD": "Ethereum", "XRP-USD": "XRP", "USDT-USD": "Tether", "SOL-USD": "Solana", "DOGE-USD": "Dogecoin"},
        "currencies": {"EURUSD=X": "EUR/USD", "JPY=X": "USD/JPY", "GBPUSD=X": "GBP/USD", "CHF=X": "USD/CHF"}
    }
    
    target_cat = categories.get(category.lower(), categories["cryptocurrencies"])
    summary = []
    
    for symbol, name in target_cat.items():
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1mo")
            if len(hist) >= 2:
                current = float(hist['Close'].iloc[-1])
                prev = float(hist['Close'].iloc[-2])
                change = current - prev
                change_pct = (change / prev) * 100
                
                # Get last 20 points for sparkline
                sparkline = hist['Close'].tail(20).tolist()
                
                summary.append({
                    "symbol": symbol,
                    "name": name,
                    "price": current,
                    "change": change,
                    "change_pct": change_pct,
                    "sparkline": sparkline
                })
        except:
            pass
    return summary

@app.get("/api/screener/markets")
def get_available_markets():
    """Return list of available markets and whether their ticker file exists"""
    market_labels = {
        "sp500": "S&P 500",
        "nasdaq": "Nasdaq",
        "dow": "Dow Jones",
        "ftse100": "FTSE 100",
        "bist": "BIST (Borsa Istanbul)",
    }
    result = []
    for key, label in market_labels.items():
        filepath = TICKER_DIR / MARKET_FILES[key]
        count = 0
        if filepath.exists():
            with open(filepath, "r") as f:
                count = len([l for l in f if l.strip()])
        result.append({"key": key, "label": label, "ticker_count": count, "file_exists": filepath.exists()})
    return result

@app.get("/api/screener/refresh")
def refresh_market_tickers(market: str = "sp500"):
    """Fetch latest tickers from Wikipedia and save to file"""
    import requests as req
    from io import StringIO
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    # (url, column_name, table_index, match_keyword_or_None)
    wiki_sources = {
        "sp500": ("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "Symbol", 0, None),
        "dow": ("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average", "Symbol", None, "Symbol"),
        "nasdaq": ("https://en.wikipedia.org/wiki/Nasdaq-100", "Ticker", 5, None),
        "ftse100": ("https://en.wikipedia.org/wiki/FTSE_100_Index", "Ticker", 6, None),
    }
    
    if market == "bist":
        tickers = load_tickers_from_file("bist")
        return {"market": market, "count": len(tickers), "message": "BIST file already maintained manually"}
    
    source = wiki_sources.get(market.lower())
    if not source:
        raise HTTPException(status_code=400, detail=f"Unknown market: {market}. Available: {list(wiki_sources.keys())}")
    
    try:
        url, col, table_idx, match = source
        r = req.get(url, headers=headers)
        if match:
            tables = pd.read_html(StringIO(r.text), match=match)
            tickers = tables[0][col].tolist()
        else:
            tables = pd.read_html(StringIO(r.text))
            tickers = tables[table_idx][col].tolist()
        # Clean ticker symbols
        tickers = [str(t).strip() for t in tickers if str(t).strip()]
        save_tickers_to_file(market, tickers)
        return {"market": market, "count": len(tickers), "message": f"Successfully saved {len(tickers)} tickers"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/screener")
def get_screener_data(category: str = "active", market: str = "sp500"):
    """Load tickers from file and return screener data"""
    symbols = load_tickers_from_file(market)
    
    if not symbols:
        raise HTTPException(status_code=404, detail=f"No tickers found for market '{market}'. Click refresh to download them.")
    
    # Limit to 20 tickers for performance
    symbols = symbols[:20]
    
    try:
        # Fast bulk download
        data = yf.download(symbols, period="1y", group_by="ticker", auto_adjust=True, threads=True)
        results = []
        
        is_single = len(symbols) == 1
        
        for symbol in symbols:
            try:
                df = data[symbol].dropna() if not is_single else data.dropna()
                if len(df) < 2: continue
                
                current = float(df['Close'].iloc[-1])
                prev = float(df['Close'].iloc[-2])
                change = current - prev
                change_pct = (change / prev) * 100
                
                vol = float(df['Volume'].iloc[-1])
                avg_vol = float(df['Volume'].tail(60).mean())
                
                price_1y_ago = float(df['Close'].iloc[0])
                change_52wk = ((current - price_1y_ago) / price_1y_ago) * 100
                
                sparkline = df['Close'].tail(20).tolist()
                
                # Try to get company name from yfinance (cached by yfinance internally)
                try:
                    ticker_info = yf.Ticker(symbol).info
                    name = ticker_info.get("shortName", symbol)
                except Exception:
                    name = symbol
                
                results.append({
                    "symbol": symbol,
                    "name": name,
                    "price": current,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": vol,
                    "avg_vol": avg_vol,
                    "change_52wk": change_52wk,
                    "sparkline": sparkline
                })
            except Exception:
                pass
                
        # Sort based on category
        if category == "gainers":
            results.sort(key=lambda x: x["change_pct"], reverse=True)
        elif category == "losers":
            results.sort(key=lambda x: x["change_pct"])
        elif category == "active" or category == "trending":
            results.sort(key=lambda x: x["volume"], reverse=True)
        elif category == "52wk_gainers":
            results.sort(key=lambda x: x["change_52wk"], reverse=True)
        elif category == "52wk_losers":
            results.sort(key=lambda x: x["change_52wk"])
            
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/{ticker}")
def get_stock_data(ticker: str, period: str = "1y", interval: str = "1d"):
    try:
        stock = yf.Ticker(ticker)
        
        intervals_to_try = [interval]
        if interval in ["1h", "4h"]:
            intervals_to_try.append("1d")
            
        hist = None
        actual_interval = interval
        for inv in intervals_to_try:
            if inv == "4h":
                h = stock.history(period=period, interval="1h")
                if not h.empty:
                    h = h.resample("4h").agg({
                        "Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"
                    }).dropna()
                    hist = h
                    actual_interval = "4h"
                    break
            else:
                h = stock.history(period=period, interval=inv)
                if not h.empty:
                    hist = h
                    actual_interval = inv
                    break
        
        if hist is None or hist.empty:
            raise HTTPException(status_code=404, detail="Ticker not found or no data available")

        # Calculate SMAs
        hist['SMA_20'] = hist['Close'].rolling(window=20).mean()
        hist['SMA_50'] = hist['Close'].rolling(window=50).mean()
        
        # Calculate Bollinger Bands
        std_dev = hist['Close'].rolling(window=20).std()
        hist['BB_upper'] = hist['SMA_20'] + 2 * std_dev
        hist['BB_lower'] = hist['SMA_20'] - 2 * std_dev

        # Calculate RSI
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        hist['RSI'] = 100 - (100 / (1 + rs))

        # Calculate MACD
        exp1 = hist['Close'].ewm(span=12, adjust=False).mean()
        exp2 = hist['Close'].ewm(span=26, adjust=False).mean()
        hist['MACD'] = exp1 - exp2
        hist['Signal_Line'] = hist['MACD'].ewm(span=9, adjust=False).mean()
        
        sma20 = hist['SMA_20'].replace({np.nan: None}).tolist()
        sma50 = hist['SMA_50'].replace({np.nan: None}).tolist()
        bb_upper = hist['BB_upper'].replace({np.nan: None}).tolist()
        bb_lower = hist['BB_lower'].replace({np.nan: None}).tolist()
        rsi = hist['RSI'].replace({np.nan: None}).tolist()
        macd = hist['MACD'].replace({np.nan: None}).tolist()
        signal_line = hist['Signal_Line'].replace({np.nan: None}).tolist()

        info = stock.info
        
        # Fetch News safely
        try:
            news_raw = stock.news[:4] # Top 4 news
            news = []
            for n in news_raw:
                content = n.get("content", n)
                
                title = content.get("title", "")
                
                provider = content.get("provider", {})
                publisher = provider.get("displayName", content.get("publisher", "News"))
                
                urls = content.get("clickThroughUrl", {})
                link = urls.get("url", content.get("link", ""))
                
                pub_date = content.get("pubDate", "")
                if pub_date:
                    try:
                        dt = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                        timestamp = int(dt.timestamp())
                    except:
                        timestamp = content.get("providerPublishTime", 0)
                else:
                    timestamp = content.get("providerPublishTime", 0)

                news.append({
                    "title": title,
                    "publisher": publisher,
                    "link": link,
                    "timestamp": timestamp
                })
        except:
            news = []
        
        current_price = info.get("currentPrice")
        if current_price is None:
            current_price = hist['Close'].iloc[-1]

        # Calculate day change for the stock
        if len(hist) >= 2:
            prev_close = hist['Close'].iloc[-2]
            day_change = current_price - prev_close
            day_change_pct = (day_change / prev_close) * 100
        else:
            day_change = 0
            day_change_pct = 0

        data = {
            "symbol": ticker.upper(),
            "name": info.get("shortName", ticker.upper()),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "description": info.get("longBusinessSummary", ""),
            "current_price": float(current_price),
            "day_change": float(day_change),
            "day_change_pct": float(day_change_pct),
            "market_cap": info.get("marketCap", "N/A"),
            "pe_ratio": info.get("trailingPE", "N/A"),
            "forward_pe": info.get("forwardPE", "N/A"),
            "peg_ratio": info.get("pegRatio", "N/A"),
            "eps": info.get("trailingEps", "N/A"),
            "dividend_yield": info.get("dividendYield", "N/A"),
            "beta": info.get("beta", "N/A"),
            "profit_margin": info.get("profitMargins", "N/A"),
            "revenue": info.get("totalRevenue", "N/A"),
            "debt_to_equity": info.get("debtToEquity", "N/A"),
            "roe": info.get("returnOnEquity", "N/A"),
            "free_cash_flow": info.get("freeCashflow", "N/A"),
            "book_value": info.get("bookValue", "N/A"),
            "fifty_two_week_high": info.get("fiftyTwoWeekHigh", "N/A"),
            "fifty_two_week_low": info.get("fiftyTwoWeekLow", "N/A"),
            "volume": hist['Volume'].tolist() if 'Volume' in hist.columns else [],
            "dates": [d.strftime('%Y-%m-%d %H:%M') if 'h' in actual_interval or 'm' in actual_interval else d.strftime('%Y-%m-%d') for d in hist.index],
            "open": hist['Open'].tolist(),
            "high": hist['High'].tolist(),
            "low": hist['Low'].tolist(),
            "close": hist['Close'].tolist(),
            "sma20": sma20,
            "sma50": sma50,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "rsi": rsi,
            "macd": macd,
            "signal_line": signal_line,
            "news": news
        }
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/currency")
def convert_currency(base: str, target: str, amount: float = 1.0):
    try:
        ticker = f"{base.upper()}{target.upper()}=X"
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
        
        if hist.empty:
            raise HTTPException(status_code=404, detail="Currency pair not found")
            
        rate = float(hist['Close'].iloc[-1])
        converted_amount = rate * amount
        
        return {
            "base": base.upper(),
            "target": target.upper(),
            "rate": rate,
            "amount": amount,
            "converted_amount": converted_amount
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/alert/telegram")
def send_telegram_alert(ticker: str, price: float, target: float, condition: str, chat_id: str, bot_token: str = ""):
    """Send a price alert via Telegram bot"""
    import requests as req
    
    if not bot_token:
        # User can set their own bot token, or use a placeholder
        return {"status": "skipped", "message": "No bot token provided. Set your Telegram bot token to enable notifications."}
    
    direction = "↑ above" if condition == "above" else "↓ below"
    message = f"🔔 *FinDash Price Alert*\n\n*{ticker}* has moved {direction} your target!\n\n💰 Current: ${price:.2f}\n🎯 Target: ${target:.2f}\n\n_Sent from FinDash Pro_"
    
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
        resp = req.post(url, json=payload, timeout=10)
        return {"status": "sent" if resp.ok else "failed", "response": resp.json()}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/social-feed")
def get_social_feed():
    """Get trending tickers and top movers as a social-style feed"""
    try:
        # Get trending tickers from Yahoo Finance
        trending_symbols = ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "META", "GOOGL", "AMD", "NFLX", "COIN"]
        
        feed = []
        data = yf.download(trending_symbols, period="5d", group_by="ticker", auto_adjust=True, threads=True)
        
        for symbol in trending_symbols:
            try:
                df = data[symbol].dropna()
                if len(df) < 2: continue
                
                current = float(df['Close'].iloc[-1])
                prev = float(df['Close'].iloc[-2])
                change_pct = ((current - prev) / prev) * 100
                vol = float(df['Volume'].iloc[-1])
                avg_vol = float(df['Volume'].tail(20).mean())
                vol_spike = vol / avg_vol if avg_vol > 0 else 1
                
                # Generate a human-readable insight
                if abs(change_pct) > 3:
                    action = "surging" if change_pct > 0 else "plunging"
                    insight = f"${symbol} is {action} {abs(change_pct):.1f}% today"
                elif vol_spike > 2:
                    insight = f"${symbol} seeing {vol_spike:.1f}x above average volume"
                else:
                    trend = "up" if change_pct > 0 else "down"
                    insight = f"${symbol} trading {trend} {abs(change_pct):.1f}% with normal activity"
                
                feed.append({
                    "symbol": symbol,
                    "price": current,
                    "change_pct": change_pct,
                    "volume": vol,
                    "vol_spike": vol_spike,
                    "insight": insight,
                    "timestamp": datetime.now().isoformat()
                })
            except Exception:
                pass
        
        # Sort by absolute change %
        feed.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
        return feed
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def serve_frontend():
    """Serve the dashboard frontend"""
    return FileResponse(TICKER_DIR / "index.html")

def open_browser():
    """Open browser after a short delay to let the server start"""
    import time
    time.sleep(1.5)
    webbrowser.open("http://localhost:8000")

if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)