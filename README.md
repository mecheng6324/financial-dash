# FinDash Pro - Financial Dashboard

FinDash Pro is a high-performance, feature-rich financial dashboard designed for real-time market analysis, portfolio tracking, and economic data visualization. Built with a robust **FastAPI** backend and a sleek, responsive **Vanilla JavaScript** frontend, it leverages the **Yahoo Finance API** to provide deep insights into global markets.

![Dashboard Preview](https://img.shields.io/badge/Status-Active-success?style=for-the-badge)
![Tech Stack](https://img.shields.io/badge/Stack-FastAPI%20%7C%20TailwindCSS%20%7C%20Plotly.js-blue?style=for-the-badge)

---

## 🚀 Key Features

### 📊 Market Intelligence & Analysis
- **Advanced Interactive Charts**: Visualize stock performance with Plotly.js across multiple timeframes (1H, 4H, 1D, 1W) and historical ranges (up to 5 years).
- **Technical Indicators**: Overlay SMA20, SMA50, and Bollinger Bands. Dedicated views for RSI and MACD.
- **Trade Signals**: Automated technical analysis to generate buy/sell signals based on indicator crossovers.
- **Dividend Analytics**: Comprehensive dividend history charts and yield tracking.
- **Market Screener**: Real-time lists for Most Active, Top Gainers/Losers, and 52-week extremes across major global indices (S&P 500, Nasdaq, Dow Jones, FTSE 100, BIST).

### 💼 Portfolio & Watchlist
- **Custom Watchlist**: Track your favorite assets with real-time price updates.
- **Portfolio Management**: Manage a simulated portfolio with profit/loss tracking and asset allocation visualization.

### 🌐 Global Economic & Crypto Tracking
- **Economic Indicators**: Live tracking of the 10Y Treasury yield, VIX Volatility Index, US Dollar Index (DXY), and key commodities like Gold and Crude Oil.
- **Crypto Tracker**: Deep dive into the crypto market with real-time prices, Ethereum gas fees, and the Fear & Greed Index.
- **Sector Heatmap**: Performance comparison across Technology, Healthcare, Financials, Energy, and more.
- **Earnings Calendar**: Stay ahead of the market with upcoming earnings dates for major corporations.

### ⚡ Technical Excellence
- **Real-time Price Streaming**: WebSockets integration for instantaneous price updates without page refreshes.
- **Smart Search**: High-speed autocomplete ticker search across thousands of global symbols.
- **Data Export**: Download any stock's historical data directly as a CSV file.
- **Performance Optimized**: Multi-threaded data fetching and intelligent TTL-based caching for lightning-fast responses.
- **Responsive Design**: Fully optimized for Desktop, Tablet, and Mobile devices with a premium Dark Mode.

---

## 🛠️ Technology Stack

### Backend
- **FastAPI**: Modern, high-performance web framework for Python.
- **yfinance**: Reliable market data extraction from Yahoo Finance.
- **Pandas & NumPy**: Advanced data processing and technical analysis calculation.
- **WebSockets**: Real-time bi-directional communication for live price streams.
- **aiohttp**: Asynchronous HTTP client for external API integrations.

### Frontend
- **TailwindCSS**: Utility-first CSS framework for a premium UI/UX.
- **Plotly.js**: High-quality financial charting library.
- **Phosphor Icons**: Beautiful, consistent iconography.
- **Vanilla JavaScript**: Lightweight and fast frontend logic without the overhead of heavy frameworks.

---

## 🏁 Getting Started

### Prerequisites
- Python 3.8+
- pip (Python package manager)

### Installation

1. **Set up a virtual environment**:
   ```bash
   # Windows
   python -m venv .venv
   .venv\Scripts\activate

   # macOS/Linux
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Application

Start the development server:
```bash
python main.py
```
Or use uvicorn directly:
```bash
uvicorn main:app --reload
```

The application will be accessible at `http://localhost:8000`.

---

## 📁 Project Structure

```text
financial-dash/
├── main.py              # FastAPI Backend & WebSocket Server
├── index.html           # Single Page Application (Frontend)
├── requirements.txt     # Python Dependencies
├── seed_tickers.py      # Utility script for ticker management
├── sp500.txt            # S&P 500 Ticker List
├── nasdaq.txt           # Nasdaq Ticker List
├── dow.txt              # Dow Jones Ticker List
├── ftse100.txt          # FTSE 100 Ticker List
└── xu100.txt            # BIST 100 Ticker List
```
