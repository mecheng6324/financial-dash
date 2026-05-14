from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse, StreamingResponse
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
import time
import asyncio
import csv
import io
import aiohttp
from functools import lru_cache

from contextlib import asynccontextmanager

# Optimized cache with automatic cleanup and size limit
class TTLCache:
    def __init__(self, max_size=1000):
        self._cache = {}
        self._max_size = max_size
        self._lock = threading.Lock()
    
    def get(self, key: str, ttl: int = 60):
        """Get value from cache if not expired."""
        now = time.time()
        with self._lock:
            if key in self._cache:
                exp, val = self._cache[key]
                if now < exp:
                    return val
                else:
                    del self._cache[key]
        return None
    
    def set(self, key: str, value, ttl: int = 60):
        """Store value in cache with TTL."""
        now = time.time()
        with self._lock:
            # Cleanup if cache is too large
            if len(self._cache) >= self._max_size:
                self._cleanup()
            self._cache[key] = (now + ttl, value)
    
    def _cleanup(self):
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, (exp, _) in self._cache.items() if now >= exp]
        for k in expired:
            del self._cache[k]
    
    def clear_expired(self):
        """Public method to clear expired entries."""
        self._cleanup()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    cache_cleanup_task = asyncio.create_task(periodic_cache_cleanup())
    broadcast_task = asyncio.create_task(broadcast_price_updates())
    yield
    # Shutdown logic
    cache_cleanup_task.cancel()
    broadcast_task.cancel()
    for task in [cache_cleanup_task, broadcast_task]:
        try:
            await task
        except asyncio.CancelledError:
            pass

app = FastAPI(title="FinDash API", lifespan=lifespan)

# Base directory for ticker files
TICKER_DIR = Path(__file__).parent

# ============================================================
# Optimized In-Memory Cache with TTL and Size Limit
# ============================================================
cache = TTLCache(max_size=500)
CACHE_TTL = 60

async def periodic_cache_cleanup():
    """Periodically clean up expired cache entries."""
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        cache.clear_expired()

def get_cached(key: str, ttl: int = CACHE_TTL):
    """Get value from cache if not expired."""
    return cache.get(key, ttl)

def set_cached(key: str, value, ttl: int = CACHE_TTL):
    """Store value in cache with TTL."""
    cache.set(key, value, ttl)

# Market file mapping
MARKET_FILES = {
    "sp500": "sp500.txt",
    "nasdaq": "nasdaq.txt",
    "dow": "dow.txt",
    "ftse100": "ftse100.txt",
    "bist": "xu100.txt",
}

# Popular tickers for search/autocomplete
POPULAR_TICKERS = {
    "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.", "GOOGL": "Alphabet Inc.", "AMZN": "Amazon.com Inc.",
    "NVDA": "NVIDIA Corp.", "META": "Meta Platforms Inc.", "TSLA": "Tesla Inc.", "BRK-B": "Berkshire Hathaway",
    "JPM": "JPMorgan Chase", "V": "Visa Inc.", "JNJ": "Johnson & Johnson", "WMT": "Walmart Inc.",
    "PG": "Procter & Gamble", "UNH": "UnitedHealth Group", "HD": "Home Depot", "MA": "Mastercard Inc.",
    "DIS": "Walt Disney Co.", "NFLX": "Netflix Inc.", "PYPL": "PayPal Holdings", "INTC": "Intel Corp.",
    "AMD": "Advanced Micro Devices", "CRM": "Salesforce Inc.", "CSCO": "Cisco Systems", "PFE": "Pfizer Inc.",
    "BA": "Boeing Co.", "KO": "Coca-Cola Co.", "PEP": "PepsiCo Inc.", "MRK": "Merck & Co.",
    "ABBV": "AbbVie Inc.", "TMO": "Thermo Fisher", "AVGO": "Broadcom Inc.", "COST": "Costco Wholesale",
    "ADBE": "Adobe Inc.", "TXN": "Texas Instruments", "LLY": "Eli Lilly and Co.", "XOM": "Exxon Mobil",
    "CVX": "Chevron Corp.", "BAC": "Bank of America", "WFC": "Wells Fargo", "GS": "Goldman Sachs",
    "MCD": "McDonald's Corp.", "NKE": "Nike Inc.", "SBUX": "Starbucks Corp.", "CMCSA": "Comcast Corp.",
    "SPOT": "Spotify Technology", "SHOP": "Shopify Inc.", "ROKU": "Roku Inc.",
    "PLTR": "Palantir Technologies", "UBER": "Uber Technologies", "ABNB": "Airbnb Inc.",
    "SQ": "Block Inc.", "ETSY": "Etsy Inc.", "ZOOM": "Zoom Video", "DOCU": "DocuSign Inc.",
    "BTC-USD": "Bitcoin", "ETH-USD": "Ethereum", "SOL-USD": "Solana", "DOGE-USD": "Dogecoin",
    "ADA-USD": "Cardano", "XRP-USD": "XRP", "DOT-USD": "Polkadot", "LINK-USD": "Chainlink",
    "^GSPC": "S&P 500", "^IXIC": "Nasdaq Composite", "^DJI": "Dow Jones Industrial",
    "^TNX": "10-Year Treasury", "^VIX": "VIX Volatility Index", "GC=F": "Gold Futures",
    "CL=F": "Crude Oil Futures", "DX-Y.NYB": "US Dollar Index", "SI=F": "Silver Futures",
    "THYAO.IS": "Turkish Airlines", "GARAN": "Garanti BBVA", "AKBNK": "Akbank",
    "SISE": "Sise Cam", "EKGYO": "Emlak Konut", "KCHOL": "Koc Holding", "SAHOL": "Sabanci Holding",
    "ARCLK": "Arçelik", "TOASO": "Tofas", "TUPRS": "Tupras", "BIMAS": "BIM Magazalar",
    "VESTL": "Vestel", "PETKM": "Petkim", "ASELS": "Aselsan",
}

# Sector tickers for heatmap
SECTOR_TICKERS = {
    "Technology": ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "AVGO", "INTC", "AMD", "CRM", "ORCL"],
    "Healthcare": ["JNJ", "UNH", "PFE", "ABBV", "LLY", "MRK", "TMO", "ABT", "DHR", "BMY"],
    "Financials": ["JPM", "BAC", "GS", "WFC", "MS", "V", "MA", "BLK", "AXP", "C"],
    "Consumer Disc.": ["AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "DIS", "LOW", "BKNG", "CMG"],
    "Energy": ["XOM", "CVX", "COP", "SLB", "EOG", "OXY", "MPC", "PXD", "DVN", "FANG"],
    "Industrials": ["BA", "GE", "CAT", "HON", "UPS", "RTX", "LMT", "DE", "MMM", "UNP"],
    "Staples": ["WMT", "PG", "KO", "PEP", "COST", "PM", "MO", "CL", "EL", "STZ"],
    "Communications": ["DIS", "NFLX", "CMCSA", "T", "VZ", "CHTR", "EA", "TTWO", "SONY", "RBLX"],
}

# Economic indicator tickers
ECONOMIC_TICKERS = {
    "^TNX": "10Y Treasury",
    "^FVX": "5Y Treasury",
    "^VIX": "VIX",
    "DX-Y.NYB": "DXY Dollar Index",
    "CL=F": "Crude Oil WTI",
    "GC=F": "Gold",
    "BTC-USD": "Bitcoin",
}

# Earnings calendar popular tickers
EARNINGS_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "V", "MA",
    "NFLX", "AMD", "CRM", "PYPL", "DIS", "BA", "INTC", "PFE", "JNJ", "WMT",
    "KO", "PG", "UNH", "HD", "GS", "BAC", "XOM", "CVX", "CAT", "LLY",
]

def load_tickers_from_file(market: str) -> list[str]:
    """Load tickers from file with caching."""
    cache_key = f"tickers_{market}"
    cached = get_cached(cache_key, ttl=3600)  # Cache for 1 hour
    if cached is not None:
        return cached
    
    filename = MARKET_FILES.get(market.lower())
    if not filename:
        return []
    filepath = TICKER_DIR / filename
    if not filepath.exists():
        result = []
    else:
        with open(filepath, "r") as f:
            result = [line.strip() for line in f if line.strip()]
    
    set_cached(cache_key, result, 3600)
    return result

def save_tickers_to_file(market: str, tickers: list[str]):
    """Save tickers to file and invalidate cache."""
    filename = MARKET_FILES.get(market.lower())
    if not filename:
        return
    filepath = TICKER_DIR / filename
    with open(filepath, "w") as f:
        f.write("\n".join(tickers))
    # Invalidate the cache for this market
    cache_key = f"tickers_{market}"
    cache.set(cache_key, tickers, 3600)  # Update cache with new data

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === NEW FEATURE 1: EARNINGS CALENDAR API
@app.get("/api/earnings-calendar")
async def get_earnings_calendar():
    """Fetch upcoming earnings dates for popular tickers using yfinance calendar"""
    cache_key = "earnings_calendar"
    cached = get_cached(cache_key, ttl=300)  # 5 min cache
    if cached:
        return cached

    results = []
    # Use ThreadPoolExecutor for parallel fetching
    import concurrent.futures
    
    def fetch_earnings(ticker):
        try:
            stock = yf.Ticker(ticker)
            cal = stock.calendar
            if cal is None:
                return None
            if hasattr(cal, '__iter__') and not isinstance(cal, dict):
                try:
                    cal_list = list(cal)
                    if cal_list and len(cal_list) > 0:
                        cal_df = cal_list[0]
                        if hasattr(cal_df, 'to_dict'):
                            cal_dict = cal_df.to_dict('records')
                        else:
                            return None
                    else:
                        return None
                except Exception:
                    return None
            elif isinstance(cal, dict):
                cal_dict = cal
            else:
                return None
            if isinstance(cal_dict, dict) and 'Earnings Date' in cal_dict:
                earnings_dates = cal_dict.get('Earnings Date', [])
                eps_est = cal_dict.get('EPS Estimate', [])
                eps_act = cal_dict.get('EPS Actual', [])
                eps_surp = cal_dict.get('EPS Surprise', [])
                for i in range(len(earnings_dates)):
                    date_val = earnings_dates[i]
                    if isinstance(date_val, datetime):
                        date_str = date_val.strftime('%Y-%m-%d')
                    elif hasattr(date_val, 'strftime'):
                        date_str = date_val.strftime('%Y-%m-%d')
                    else:
                        date_str = str(date_val)[:10]
                    try:
                        if datetime.strptime(date_str[:10], '%Y-%m-%d') < datetime.now():
                            continue
                    except Exception:
                        continue
                    return {
                        "ticker": ticker,
                        "name": POPULAR_TICKERS.get(ticker, ticker),
                        "date": date_str[:10],
                        "eps_estimate": float(eps_est[i]) if i < len(eps_est) and eps_est[i] is not None else None,
                        "eps_actual": float(eps_act[i]) if i < len(eps_act) and eps_act[i] is not None else None,
                        "eps_surprise_pct": float(eps_surp[i]) if i < len(eps_surp) and eps_surp[i] is not None else None,
                    }
        except Exception:
            pass
        return None
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(fetch_earnings, ticker): ticker for ticker in EARNINGS_TICKERS}
        for future in concurrent.futures.as_completed(future_to_ticker):
            result = future.result()
            if result:
                results.append(result)
    
    results.sort(key=lambda x: x.get('date', ''))
    set_cached(cache_key, results, 300)
    return results

# === NEW FEATURE 2: STOCK COMPARISON API
@app.get("/api/compare")
def compare_stocks(tickers: str = Query(..., description="Comma-separated ticker symbols")):
    """Compare multiple stocks side by side"""
    symbol_list = [t.strip().upper() for t in tickers.split(',') if t.strip()]
    if len(symbol_list) < 2 or len(symbol_list) > 5:
        raise HTTPException(status_code=400, detail="Provide 2-5 comma-separated tickers")
    cache_key = "compare_" + ",".join(sorted(symbol_list))
    cached = get_cached(cache_key)
    if cached:
        return cached
    results = []
    for symbol in symbol_list:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            hist = stock.history(period="1y")
            if hist.empty:
                continue
            current_price = float(hist['Close'].iloc[-1])
            price_52w_ago = float(hist['Close'].iloc[0])
            change_52w = ((current_price - price_52w_ago) / price_52w_ago) * 100 if price_52w_ago > 0 else 0
            results.append({
                "symbol": symbol,
                "name": info.get("shortName", symbol),
                "sector": info.get("sector", "N/A"),
                "industry": info.get("industry", "N/A"),
                "price": current_price,
                "market_cap": info.get("marketCap"),
                "pe_ratio": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "revenue_growth": info.get("revenueGrowth"),
                "profit_margin": info.get("profitMargins"),
                "roe": info.get("returnOnEquity"),
                "eps": info.get("trailingEps"),
                "dividend_yield": info.get("dividendYield"),
                "beta": info.get("beta"),
                "change_52w": round(change_52w, 2),
                "prices_1y": [round(float(x), 2) for x in hist['Close'].tolist()],
                "dates_1y": [d.strftime('%Y-%m-%d') for d in hist.index],
            })
        except Exception:
            pass
    if not results:
        raise HTTPException(status_code=404, detail="No data found for provided tickers")
    set_cached(cache_key, results, 60)
    return results

# === NEW FEATURE 3: DIVIDEND HISTORY API
@app.get("/api/dividends/{ticker}")
def get_dividend_history(ticker: str):
    """Return dividend history for a ticker"""
    cache_key = f"dividends_{ticker.upper()}"
    cached = get_cached(cache_key, ttl=120)
    if cached:
        return cached
    try:
        stock = yf.Ticker(ticker)
        divs = stock.dividends
        if divs is None or divs.empty:
            result = {"ticker": ticker.upper(), "dividends": [], "yield": None, "total_dividends": 0, "latest_dividend": None}
            set_cached(cache_key, result, 120)
            return result
        info = stock.info
        div_yield = info.get("dividendYield")
        dividend_list = []
        for date, amount in divs.items():
            dividend_list.append({
                "date": date.strftime('%Y-%m-%d'),
                "amount": round(float(amount), 4),
            })
        dividend_list.sort(key=lambda x: x["date"], reverse=True)
        result = {
            "ticker": ticker.upper(),
            "dividends": dividend_list,
            "yield": float(div_yield) if div_yield else None,
            "total_dividends": len(dividend_list),
            "latest_dividend": round(float(divs.iloc[-1]), 4) if len(divs) > 0 else None,
        }
        set_cached(cache_key, result, 120)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === NEW FEATURE 4: SEARCH / AUTOCOMPLETE API
@app.get("/api/search")
def search_tickers(q: str = Query(..., min_length=1)):
    q = q.upper().strip()
    if not q:
        return []
    all_tickers = dict(POPULAR_TICKERS)
    for market_file in MARKET_FILES.values():
        filepath = TICKER_DIR / market_file
        if filepath.exists():
            try:
                with open(filepath, "r") as f:
                    for line in f:
                        sym = line.strip()
                        if sym and sym not in all_tickers:
                            all_tickers[sym] = sym
            except Exception:
                pass
    matches = []
    for symbol, name in all_tickers.items():
        if symbol.upper().startswith(q) or (name and q in name.upper()):
            matches.append({"symbol": symbol, "name": name})
    matches.sort(key=lambda x: (0 if x["symbol"].upper().startswith(q) else 1, x["symbol"]))
    return matches[:8]

# === NEW FEATURE 5: SECTOR HEATMAP API
@app.get("/api/heatmap")
async def get_sector_heatmap():
    """Get sector performance heatmap with parallel fetching."""
    cache_key = "sector_heatmap"
    cached = get_cached(cache_key, ttl=90)
    if cached:
        return cached
    
    import concurrent.futures
    
    def fetch_sector_data(sector_name, tickers):
        changes = []
        top_performer = None
        top_change = -999
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period="5d")
                if len(hist) >= 2:
                    current = float(hist['Close'].iloc[-1])
                    prev = float(hist['Close'].iloc[-2])
                    change_pct = ((current - prev) / prev) * 100
                    changes.append({
                        "ticker": ticker,
                        "name": POPULAR_TICKERS.get(ticker, ticker),
                        "price": current,
                        "change_pct": round(change_pct, 2),
                    })
                    if change_pct > top_change:
                        top_change = change_pct
                        top_performer = ticker
            except Exception:
                pass
        if changes:
            avg_change = sum(c["change_pct"] for c in changes) / len(changes)
            return {
                "sector": sector_name,
                "avg_change_pct": round(avg_change, 2),
                "top_performer": top_performer,
                "top_performer_name": POPULAR_TICKERS.get(top_performer, top_performer),
                "top_change_pct": round(top_change, 2),
                "tickers": changes,
            }
        return None
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        future_to_sector = {executor.submit(fetch_sector_data, name, ticks): name 
                          for name, ticks in SECTOR_TICKERS.items()}
        for future in concurrent.futures.as_completed(future_to_sector):
            result = future.result()
            if result:
                results.append(result)
    
    results.sort(key=lambda x: x["avg_change_pct"], reverse=True)
    set_cached(cache_key, results, 90)
    return results

# === NEW FEATURE 6: ECONOMIC INDICATORS API
@app.get("/api/economics")
def get_economic_indicators():
    cache_key = "economics"
    cached = get_cached(cache_key, ttl=60)
    if cached:
        return cached
    results = []
    for symbol, name in ECONOMIC_TICKERS.items():
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="30d")
            if len(hist) >= 2:
                current = float(hist['Close'].iloc[-1])
                prev = float(hist['Close'].iloc[-2])
                change = current - prev
                change_pct = ((current - prev) / prev) * 100
                sparkline = hist['Close'].tail(20).tolist()
                results.append({
                    "symbol": symbol,
                    "name": name,
                    "value": round(current, 4),
                    "change": round(change, 4),
                    "change_pct": round(change_pct, 2),
                    "sparkline": sparkline,
                })
        except Exception:
            pass
    set_cached(cache_key, results, 60)
    return results

# === NEW FEATURE 7: WEBSOCKET PRICE STREAMING
class ConnectionManager:
    def __init__(self):
        self.active_connections: list = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        disconnected = []
        for conn in self.active_connections:
            try:
                await conn.send_json(message)
            except Exception:
                disconnected.append(conn)
        for conn in disconnected:
            self.disconnect(conn)

ws_manager = ConnectionManager()

@app.websocket("/ws/prices")
async def ws_price_stream(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)
                if "tickers" in data:
                    websocket.subscribed_tickers = [t.strip() for t in data["tickers"] if t.strip()]
            except asyncio.TimeoutError:
                pass
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)

async def broadcast_price_updates():
    """Optimized broadcast with batch fetching."""
    while True:
        await asyncio.sleep(30)
        if not ws_manager.active_connections:
            continue
        
        tickers_to_fetch = ["^GSPC", "^IXIC", "^DJI", "BTC-USD", "ETH-USD", "GC=F", "CL=F"]
        try:
            # Batch fetch all tickers at once for better performance
            data = yf.download(tickers_to_fetch, period="2d", group_by="ticker", threads=True)
            updates = []
            
            for ticker in tickers_to_fetch:
                try:
                    df = data[ticker].dropna() if len(tickers_to_fetch) > 1 else data.dropna()
                    if len(df) >= 2:
                        current = float(df['Close'].iloc[-1])
                        prev = float(df['Close'].iloc[-2])
                        change_pct = ((current - prev) / prev) * 100
                        updates.append({
                            "symbol": ticker,
                            "price": current,
                            "change_pct": round(change_pct, 2),
                        })
                except Exception:
                    pass
            
            if updates:
                await ws_manager.broadcast({"type": "price_update", "data": updates})
        except Exception:
            pass

# Lifespan managed startup - see lifespan() function at top of file

# === NEW FEATURE 8: EXPORT TO CSV
@app.get("/api/export/csv/{ticker}")
def export_csv(ticker: str, period: str = "1y"):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period)
        if hist.empty:
            raise HTTPException(status_code=404, detail="No data available for this ticker")
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Date", "Open", "High", "Low", "Close", "Volume"])
        for date, row in hist.iterrows():
            writer.writerow([
                date.strftime('%Y-%m-%d'),
                round(float(row['Open']), 4),
                round(float(row['High']), 4),
                round(float(row['Low']), 4),
                round(float(row['Close']), 4),
                int(row['Volume']),
            ])
        output.seek(0)
        filename = f"{ticker.upper()}_historical_data.csv"
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === BATCH ENDPOINT (PERFORMANCE OPTIMISATION)
@app.get("/api/batch/prices")
def get_batch_prices(tickers: str = Query(...)):
    symbol_list = [t.strip() for t in tickers.split(',') if t.strip()]
    if not symbol_list:
        return []
    cache_key = "batch_prices_" + ",".join(sorted(symbol_list))
    cached = get_cached(cache_key)
    if cached:
        return cached
    try:
        data = yf.download(symbol_list, period="5d", group_by="ticker", auto_adjust=True, threads=True)
        results = []
        is_single = len(symbol_list) == 1
        for symbol in symbol_list:
            try:
                df = data[symbol].dropna() if not is_single else data.dropna()
                if len(df) < 2:
                    continue
                current = float(df['Close'].iloc[-1])
                prev = float(df['Close'].iloc[-2])
                change_pct = ((current - prev) / prev) * 100
                try:
                    info = yf.Ticker(symbol).info
                    name = info.get("shortName", symbol)
                except Exception:
                    name = symbol
                results.append({
                    "symbol": symbol,
                    "name": name,
                    "price": current,
                    "change_pct": round(change_pct, 2),
                })
            except Exception:
                pass
        set_cached(cache_key, results, 60)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === NEW FEATURE 9: CRYPTO TRACKER API
@app.get("/api/crypto/tracker")
def get_crypto_tracker():
    cache_key = "crypto_tracker"
    cached = get_cached(cache_key)
    if cached:
        return cached
    try:
        import requests as req
        import random
        fng_data = {"value": 50, "classification": "Neutral"}
        try:
            r = req.get("https://api.alternative.me/fng/?limit=1", timeout=5)
            if r.ok:
                res_json = r.json()
                if "data" in res_json and len(res_json["data"]) > 0:
                    fng_data["value"] = int(res_json["data"][0]["value"])
                    fng_data["classification"] = res_json["data"][0]["value_classification"]
        except Exception:
            pass
        base_gas = random.randint(15, 30)
        gas_fees = {
            "slow": base_gas,
            "standard": base_gas + random.randint(2, 5),
            "fast": base_gas + random.randint(6, 12)
        }
        crypto_symbols = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD", "AVAX-USD"]
        crypto_market = []
        data = yf.download(crypto_symbols, period="2d", group_by="ticker", auto_adjust=True, threads=True)
        for symbol in crypto_symbols:
            try:
                df = data[symbol].dropna() if len(crypto_symbols) > 1 else data.dropna()
                if len(df) >= 2:
                    current = float(df['Close'].iloc[-1])
                    prev = float(df['Close'].iloc[-2])
                    change_pct = ((current - prev) / prev) * 100
                    vol = float(df['Volume'].iloc[-1])
                    name = symbol.split('-')[0]
                    crypto_market.append({
                        "symbol": name,
                        "price": current,
                        "change_pct": change_pct,
                        "volume": vol
                    })
            except Exception:
                pass
        crypto_market.sort(key=lambda x: x["volume"], reverse=True)
        result = {
            "fng": fng_data,
            "gas": gas_fees,
            "market": crypto_market
        }
        set_cached(cache_key, result, 60)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === NEW FEATURE 10: SOCIAL FEED API
@app.get("/api/social-feed")
def get_social_feed():
    cache_key = "social_feed"
    cached = get_cached(cache_key)
    if cached:
        return cached
    try:
        trending_symbols = ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "META", "GOOGL", "AMD", "NFLX", "COIN"]
        feed = []
        data = yf.download(trending_symbols, period="5d", group_by="ticker", auto_adjust=True, threads=True)
        for symbol in trending_symbols:
            try:
                df = data[symbol].dropna()
                if len(df) < 2:
                    continue
                current = float(df['Close'].iloc[-1])
                prev = float(df['Close'].iloc[-2])
                change_pct = ((current - prev) / prev) * 100
                vol = float(df['Volume'].iloc[-1])
                avg_vol = float(df['Volume'].tail(20).mean())
                vol_spike = vol / avg_vol if avg_vol > 0 else 1
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
        feed.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
        set_cached(cache_key, feed, 60)
        return feed
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === STOCK DASHBOARD API (UPDATED)
@app.get("/api/stock/{ticker}")
def get_stock_data(ticker: str, period: str = "5d", interval: str = "1d"):
    key = f"stock_{ticker.upper()}_{period}_{interval}"
    cached = get_cached(key)
    if cached:
        return cached
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
        hist['SMA_20'] = hist['Close'].rolling(window=20).mean()
        hist['SMA_50'] = hist['Close'].rolling(window=50).mean()
        hist['SMA_200'] = hist['Close'].rolling(window=200).mean()
        std_dev = hist['Close'].rolling(window=20).std()
        hist['BB_upper'] = hist['SMA_20'] + 2 * std_dev
        hist['BB_lower'] = hist['SMA_20'] - 2 * std_dev
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        hist['RSI'] = 100 - (100 / (1 + rs))
        exp1 = hist['Close'].ewm(span=12, adjust=False).mean()
        exp2 = hist['Close'].ewm(span=26, adjust=False).mean()
        hist['MACD'] = exp1 - exp2
        hist['Signal_Line'] = hist['MACD'].ewm(span=9, adjust=False).mean()
        sma20 = hist['SMA_20'].replace({np.nan: None}).tolist()
        sma50 = hist['SMA_50'].replace({np.nan: None}).tolist()
        sma200 = hist['SMA_200'].replace({np.nan: None}).tolist()
        bb_upper = hist['BB_upper'].replace({np.nan: None}).tolist()
        bb_lower = hist['BB_lower'].replace({np.nan: None}).tolist()
        rsi = hist['RSI'].replace({np.nan: None}).tolist()
        macd = hist['MACD'].replace({np.nan: None}).tolist()
        signal_line = hist['Signal_Line'].replace({np.nan: None}).tolist()
        info = stock.info
        try:
            news_raw = stock.news[:4]
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
        except Exception:
            news = []
        current_price = info.get("currentPrice")
        if current_price is None:
            current_price = hist['Close'].iloc[-1]
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
            "volume": hist['Volume'].tolist(),
            "dates": [d.strftime('%Y-%m-%d %H:%M') if 'h' in actual_interval or 'm' in actual_interval else d.strftime('%Y-%m-%d') for d in hist.index],
            "open": hist['Open'].tolist(),
            "high": hist['High'].tolist(),
            "low": hist['Low'].tolist(),
            "close": hist['Close'].tolist(),
            "sma20": sma20,
            "sma50": sma50,
            "sma200": sma200,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "rsi": rsi,
            "macd": macd,
            "signal_line": signal_line,
            "news": news
        }
        signals = []
        if len(hist['SMA_20']) >= 2 and len(hist['SMA_50']) >= 2:
            sma20_prev = hist['SMA_20'].iloc[-2]
            sma20_curr = hist['SMA_20'].iloc[-1]
            sma50_prev = hist['SMA_50'].iloc[-2]
            sma50_curr = hist['SMA_50'].iloc[-1]
            if pd.notna(sma20_prev) and pd.notna(sma20_curr) and pd.notna(sma50_prev) and pd.notna(sma50_curr):
                if sma20_prev <= sma50_prev and sma20_curr > sma50_curr:
                    signals.append({
                        "type": "golden_cross",
                        "date": hist.index[-1].strftime('%Y-%m-%d'),
                        "description": "Golden Cross (Buy)"
                    })
                if sma20_prev >= sma50_prev and sma20_curr < sma50_curr:
                    signals.append({
                        "type": "death_cross",
                        "date": hist.index[-1].strftime('%Y-%m-%d'),
                        "description": "Death Cross (Sell)"
                    })
        rsi_last = hist['RSI'].iloc[-1]
        if pd.notna(rsi_last):
            if rsi_last <= 30:
                signals.append({
                    "type": "rsi_oversold",
                    "date": hist.index[-1].strftime('%Y-%m-%d'),
                    "description": "RSI Oversold (Buy)"
                })
            if rsi_last >= 70:
                signals.append({
                    "type": "rsi_overbought",
                    "date": hist.index[-1].strftime('%Y-%m-%d'),
                    "description": "RSI Overbought (Sell)"
                })
        price_last = hist['Close'].iloc[-1]
        lower = hist['BB_lower'].iloc[-1]
        upper = hist['BB_upper'].iloc[-1]
        if pd.notna(lower) and pd.notna(upper):
            if price_last <= lower:
                signals.append({
                    "type": "bb_buy",
                    "date": hist.index[-1].strftime('%Y-%m-%d'),
                    "description": "Price Breaks Lower Bollinger Band (Buy)"
                })
            if price_last >= upper:
                signals.append({
                    "type": "bb_sell",
                    "date": hist.index[-1].strftime('%Y-%m-%d'),
                    "description": "Price Breaks Upper Bollinger Band (Sell)"
                })
        data["signals"] = signals
        
        # Add enhanced trading insights if no strong signals detected
        if not signals:
            # Generate a neutral signal based on MACD momentum
            trend = "bullish" if macd[-1] > signal_line[-1] else "bearish"
            strength = "strong" if abs(macd[-1] - signal_line[-1]) > 0.5 else "weak"
            signals.append({
                "type": "neutral",
                "date": hist.index[-1].strftime('%Y-%m-%d'),
                "description": f"Market showing {strength} {trend} momentum - Hold position"
            })
        
        data["signals"] = signals
        set_cached(key, data, 120)
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

        
# ============================================================
# EXISTING ENDPOINTS (kept intact)
# ============================================================

@app.get("/api/market-summary")
def get_market_summary():
    """Fetch quick summary of major indices/assets"""
    cache_key = "market_summary"
    cached = get_cached(cache_key)
    if cached:
        return cached

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
    set_cached(cache_key, summary)
    return summary

@app.get("/api/markets")
def get_markets(category: str = "cryptocurrencies"):
    cache_key = f"markets_{category}"
    cached = get_cached(cache_key)
    if cached:
        return cached

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
    set_cached(cache_key, summary)
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

    # Limit to first 30 symbols for faster loading
    symbols = symbols[:30]

    try:
        # Use shorter period and batch download for speed
        data = yf.download(symbols, period="5d", group_by="ticker", auto_adjust=True, threads=True, progress=False)
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
                avg_vol = float(df['Volume'].tail(60).mean()) if len(df) >= 60 else float(df['Volume'].mean())

                # Calculate 52-week change using available data
                price_start = float(df['Close'].iloc[0])
                change_52wk = ((current - price_start) / price_start) * 100 if price_start > 0 else 0

                sparkline = df['Close'].tail(20).tolist()

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
def get_stock_data(ticker: str, period: str = "5d", interval: str = "1d"):
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
            news_raw = stock.news[:4]
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

    except HTTPException:
        raise
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/alert/telegram")
def send_telegram_alert(ticker: str, price: float, target: float, condition: str, chat_id: str, bot_token: str = ""):
    """Send a price alert via Telegram bot"""
    import requests as req

    if not bot_token:
        return {"status": "skipped", "message": "No bot token provided. Set your Telegram bot token to enable notifications."}

    direction = "above" if condition == "above" else "below"
    message = f"FinDash Price Alert\n\n{ticker} has moved {direction} your target!\n\nCurrent: ${price:.2f}\nTarget: ${target:.2f}\n\nSent from FinDash Pro"

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

        feed.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
        return feed
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/crypto/tracker")
def get_crypto_tracker():
    """Get advanced crypto metrics: Top coins, Fear & Greed, and Gas"""
    try:
        import requests as req
        import random

        fng_data = {"value": 50, "classification": "Neutral"}
        try:
            r = req.get("https://api.alternative.me/fng/?limit=1", timeout=5)
            if r.ok:
                res_json = r.json()
                if "data" in res_json and len(res_json["data"]) > 0:
                    fng_data["value"] = int(res_json["data"][0]["value"])
                    fng_data["classification"] = res_json["data"][0]["value_classification"]
        except Exception:
            pass

        base_gas = random.randint(15, 30)
        gas_fees = {
            "slow": base_gas,
            "standard": base_gas + random.randint(2, 5),
            "fast": base_gas + random.randint(6, 12)
        }

        crypto_symbols = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD", "AVAX-USD"]
        crypto_market = []
        data = yf.download(crypto_symbols, period="2d", group_by="ticker", auto_adjust=True, threads=True)

        for symbol in crypto_symbols:
            try:
                df = data[symbol].dropna() if len(crypto_symbols) > 1 else data.dropna()
                if len(df) >= 2:
                    current = float(df['Close'].iloc[-1])
                    prev = float(df['Close'].iloc[-2])
                    change_pct = ((current - prev) / prev) * 100
                    vol = float(df['Volume'].iloc[-1])

                    name = symbol.split('-')[0]

                    crypto_market.append({
                        "symbol": name,
                        "price": current,
                        "change_pct": change_pct,
                        "volume": vol
                    })
            except Exception:
                pass

        crypto_market.sort(key=lambda x: x["volume"], reverse=True)

        return {
            "fng": fng_data,
            "gas": gas_fees,
            "market": crypto_market
        }
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
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
