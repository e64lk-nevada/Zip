#!/usr/bin/env python3
"""
Polymarket 5-Min Signal Web App
З запасними джерелами цін (Binance часто блокує Render).
"""

import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional
from collections import deque

import httpx
from starlette.applications import Starlette
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route

SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT").split(",")]
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "0.68"))
WINDOW_SECONDS = 300
CONFIRM_AT = 180
PRICE_HISTORY_LEN = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Мапінг для різних API
COINGECKO_IDS = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "XRPUSDT": "ripple",
}

class WindowState:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price_to_beat: Optional[float] = None
        self.window_start: Optional[datetime] = None
        self.prices: deque = deque(maxlen=PRICE_HISTORY_LEN)
        self.last_price: Optional[float] = None
        self.signal_sent: bool = False
        self.last_signal: Optional[dict] = None
        self.last_update: Optional[datetime] = None
        self.last_error: Optional[str] = None

    def reset(self, price: float, start: datetime):
        self.price_to_beat = price
        self.window_start = start
        self.prices.clear()
        self.prices.append(price)
        self.last_price = price
        self.signal_sent = False
        self.last_signal = None
        self.last_update = datetime.now(timezone.utc)
        self.last_error = None

state: Dict[str, WindowState] = {s: WindowState(s) for s in SYMBOLS}
global_error: Optional[str] = None


def generate_signal(price_to_beat: float, current_price: float, seconds_passed: int) -> dict:
    if seconds_passed < CONFIRM_AT:
        return {"side": None, "confidence": 0.0, "reason": "too_early"}

    lead_bps = ((current_price - price_to_beat) / price_to_beat) * 10000
    time_left = WINDOW_SECONDS - seconds_passed

    if abs(lead_bps) < 3.0:
        return {"side": None, "confidence": 0.0, "reason": "too_small_lead"}

    side = "UP" if lead_bps > 0 else "DOWN"
    abs_lead = abs(lead_bps)

    if time_left > 90:
        confidence = min(0.62 + abs_lead * 0.018, 0.78)
    elif time_left > 45:
        confidence = min(0.72 + abs_lead * 0.022, 0.88)
    else:
        confidence = min(0.82 + abs_lead * 0.025, 0.95)

    return {
        "side": side,
        "confidence": round(confidence, 3),
        "lead_bps": round(lead_bps, 2),
        "time_left": time_left
    }


def momentum_bps(prices: deque, lookback: int = 12) -> float:
    if len(prices) < lookback + 1:
        return 0.0
    return (prices[-1] - prices[-lookback]) / prices[-lookback] * 10000


def is_strong_signal(signal: dict, mom: float) -> bool:
    if signal["side"] is None:
        return False
    same_dir = (
        (signal["side"] == "UP" and mom > 1.5) or
        (signal["side"] == "DOWN" and mom < -1.5)
    )
    return same_dir and signal["confidence"] >= MIN_CONFIDENCE


async def fetch_price_binance(client: httpx.AsyncClient, symbol: str) -> Optional[float]:
    urls = [
        f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}",
        f"https://data-api.binance.vision/api/v3/ticker/price?symbol={symbol}",
    ]
    for url in urls:
        try:
            r = await client.get(url, timeout=4.0)
            if r.status_code == 200:
                return float(r.json()["price"])
        except Exception:
            continue
    return None


async def fetch_price_coingecko(client: httpx.AsyncClient, symbol: str) -> Optional[float]:
    cg_id = COINGECKO_IDS.get(symbol)
    if not cg_id:
        return None
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd"
        r = await client.get(url, timeout=5.0)
        if r.status_code == 200:
            data = r.json()
            return float(data[cg_id]["usd"])
    except Exception:
        pass
    return None


async def fetch_price(client: httpx.AsyncClient, symbol: str) -> Optional[float]:
    # 1) Binance
    price = await fetch_price_binance(client, symbol)
    if price:
        return price
    # 2) CoinGecko fallback
    price = await fetch_price_coingecko(client, symbol)
    if price:
        return price
    return None


def get_current_window_start() -> datetime:
    now = datetime.now(timezone.utc)
    minute = (now.minute // 5) * 5
    return now.replace(minute=minute, second=0, microsecond=0)


async def price_loop():
    global global_error
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        while True:
            try:
                window_start = get_current_window_start()
                seconds_passed = int((datetime.now(timezone.utc) - window_start).total_seconds())
                any_ok = False

                for symbol in SYMBOLS:
                    price = await fetch_price(client, symbol)
                    if price is None:
                        state[symbol].last_error = "no price"
                        continue

                    any_ok = True
                    ws = state[symbol]
                    ws.last_error = None

                    if ws.window_start is None or ws.window_start != window_start:
                        ws.reset(price, window_start)
                        logger.info(f"[{symbol}] New window | open={price:.4f}")
                        continue

                    ws.prices.append(price)
                    ws.last_price = price
                    ws.last_update = datetime.now(timezone.utc)

                    if seconds_passed < CONFIRM_AT or ws.signal_sent:
                        continue

                    signal = generate_signal(ws.price_to_beat, price, seconds_passed)
                    mom = momentum_bps(ws.prices)

                    if is_strong_signal(signal, mom):
                        ws.signal_sent = True
                        ws.last_signal = {
                            **signal,
                            "momentum_bps": round(mom, 2),
                            "price": price,
                            "open": ws.price_to_beat,
                            "time": datetime.now(timezone.utc).isoformat()
                        }
                        logger.info(f"SIGNAL {symbol} {signal['side']} conf={signal['confidence']}")

                global_error = None if any_ok else "Всі джерела цін недоступні"
                await asyncio.sleep(2)

            except Exception as e:
                global_error = str(e)
                logger.error(f"Loop error: {e}")
                await asyncio.sleep(5)


async def api_status(request):
    window_start = get_current_window_start()
    seconds_passed = int((datetime.now(timezone.utc) - window_start).total_seconds())
    time_left = max(0, WINDOW_SECONDS - seconds_passed)

    result = {
        "server_time": datetime.now(timezone.utc).isoformat(),
        "window_start": window_start.isoformat(),
        "seconds_passed": seconds_passed,
        "time_left": time_left,
        "confirm_at": CONFIRM_AT,
        "error": global_error,
        "markets": {}
    }

    for symbol, ws in state.items():
        name = symbol.replace("USDT", "")
        if ws.price_to_beat is None:
            result["markets"][name] = {
                "status": "waiting",
                "error": ws.last_error or global_error
            }
            continue

        current = ws.last_price or ws.price_to_beat
        lead = ((current - ws.price_to_beat) / ws.price_to_beat) * 10000

        result["markets"][name] = {
            "open": round(ws.price_to_beat, 4),
            "current": round(current, 4),
            "lead_bps": round(lead, 2),
            "signal_sent": ws.signal_sent,
            "signal": ws.last_signal,
            "updated": ws.last_update.isoformat() if ws.last_update else None
        }

    return JSONResponse(result)


async def health(request):
    return JSONResponse({"ok": True, "error": global_error})


HTML_PAGE = """<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no" />
  <meta name="apple-mobile-web-app-capable" content="yes" />
  <title>Polymarket 5m Signals</title>
  <style>
    :root {
      --bg: #0b0f14; --card: #141a22; --border: #1e2733;
      --text: #e8eef6; --muted: #8b9bb4; --up: #22c55e; --down: #ef4444; --accent: #3b82f6;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg); color: var(--text); min-height: 100vh;
      padding: 16px; padding-bottom: 40px;
    }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
    h1 { font-size: 1.25rem; font-weight: 700; }
    .badge {
      font-size: 0.75rem; background: #1a2332; color: var(--muted);
      padding: 4px 10px; border-radius: 999px; border: 1px solid var(--border);
    }
    .badge.err { color: #f87171; border-color: #7f1d1d; }
    .timer {
      background: var(--card); border: 1px solid var(--border); border-radius: 14px;
      padding: 14px 16px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center;
    }
    .timer-label { color: var(--muted); font-size: 0.85rem; }
    .timer-value { font-size: 1.4rem; font-weight: 700; font-variant-numeric: tabular-nums; }
    .progress { height: 4px; background: #1e2733; border-radius: 2px; margin-bottom: 12px; overflow: hidden; }
    .progress-bar { height: 100%; background: var(--accent); transition: width 0.5s linear; }
    .error-box {
      background: #1c1010; border: 1px solid #7f1d1d; color: #fca5a5;
      border-radius: 12px; padding: 10px 14px; margin-bottom: 12px; font-size: 0.85rem;
    }
    .grid { display: grid; gap: 12px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 16px; }
    .card.has-signal { border-color: #2a3a4f; }
    .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
    .symbol { font-size: 1.15rem; font-weight: 700; }
    .side { font-size: 0.8rem; font-weight: 700; padding: 4px 10px; border-radius: 8px; text-transform: uppercase; }
    .side.up { background: rgba(34,197,94,0.15); color: var(--up); }
    .side.down { background: rgba(239,68,68,0.15); color: var(--down); }
    .side.wait { background: #1a2332; color: var(--muted); }
    .row { display: flex; justify-content: space-between; font-size: 0.9rem; margin-bottom: 6px; }
    .row span:first-child { color: var(--muted); }
    .lead.positive { color: var(--up); }
    .lead.negative { color: var(--down); }
    .signal-box { margin-top: 12px; padding: 12px; background: #0f1520; border-radius: 12px; border: 1px solid #243044; }
    .signal-title { font-size: 0.75rem; color: var(--muted); margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.04em; }
    .confidence { font-size: 1.5rem; font-weight: 700; }
    .footer { margin-top: 24px; text-align: center; color: var(--muted); font-size: 0.75rem; line-height: 1.5; }
    .pulse { animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
  </style>
</head>
<body>
  <header>
    <h1>5m Signals</h1>
    <div class="badge" id="statusBadge">loading…</div>
  </header>
  <div class="timer">
    <div>
      <div class="timer-label">Час у вікні</div>
      <div class="timer-value" id="timerValue">--:--</div>
    </div>
    <div style="text-align:right">
      <div class="timer-label">До кінця</div>
      <div class="timer-value" id="timeLeft">--</div>
    </div>
  </div>
  <div class="progress"><div class="progress-bar" id="progressBar" style="width:0%"></div></div>
  <div id="errorBox" class="error-box" style="display:none"></div>
  <div class="grid" id="cards"></div>
  <div class="footer">Сигнали після T+180с · Лід + моментум<br>Не фінансова порада</div>
  <script>
    const symbols = ["BTC", "ETH", "SOL", "XRP"];
    function formatTime(sec) {
      const m = Math.floor(sec / 60), s = sec % 60;
      return m + ":" + s.toString().padStart(2, "0");
    }
    function render(data) {
      const passed = data.seconds_passed, left = data.time_left;
      document.getElementById("timerValue").textContent = formatTime(passed);
      document.getElementById("timeLeft").textContent = left + "с";
      document.getElementById("progressBar").style.width = (passed / 300 * 100) + "%";

      const badge = document.getElementById("statusBadge");
      const errBox = document.getElementById("errorBox");
      if (data.error) {
        badge.textContent = "error";
        badge.className = "badge err";
        errBox.style.display = "block";
        errBox.textContent = data.error;
      } else {
        badge.textContent = "live";
        badge.className = "badge";
        errBox.style.display = "none";
      }

      const container = document.getElementById("cards");
      container.innerHTML = "";
      for (const name of symbols) {
        const m = data.markets[name];
        if (!m || m.status === "waiting") {
          const err = m && m.error ? m.error : "завантаження…";
          container.innerHTML += '<div class="card"><div class="card-header"><div class="symbol">' + name + '</div><div class="side wait">чекаю</div></div><div class="row"><span>Статус</span><span class="pulse">' + err + '</span></div></div>';
          continue;
        }
        const leadClass = m.lead_bps >= 0 ? "positive" : "negative";
        let sideHtml = '<div class="side wait">очікування</div>', signalHtml = "";
        if (m.signal_sent && m.signal) {
          const s = m.signal;
          const sideClass = s.side === "UP" ? "up" : "down";
          sideHtml = '<div class="side ' + sideClass + '">' + s.side + '</div>';
          const confColor = s.side === "UP" ? "var(--up)" : "var(--down)";
          signalHtml = '<div class="signal-box"><div class="signal-title">Сигнал</div><div class="confidence" style="color:' + confColor + '">' + (s.confidence * 100).toFixed(1) + '%</div><div class="row" style="margin-top:8px"><span>Лід</span><span>' + (s.lead_bps > 0 ? "+" : "") + s.lead_bps + ' bps</span></div><div class="row"><span>Моментум</span><span>' + (s.momentum_bps > 0 ? "+" : "") + s.momentum_bps + ' bps</span></div></div>';
        }
        container.innerHTML += '<div class="card ' + (m.signal_sent ? "has-signal" : "") + '"><div class="card-header"><div class="symbol">' + name + '</div>' + sideHtml + '</div><div class="row"><span>Open</span><span>' + m.open + '</span></div><div class="row"><span>Now</span><span>' + m.current + '</span></div><div class="row"><span>Lead</span><span class="lead ' + leadClass + '">' + (m.lead_bps > 0 ? "+" : "") + m.lead_bps + ' bps</span></div>' + signalHtml + '</div>';
      }
    }
    async function refresh() {
      try {
        const res = await fetch("/api/status");
        render(await res.json());
      } catch (e) {
        document.getElementById("statusBadge").textContent = "error";
        document.getElementById("statusBadge").className = "badge err";
      }
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>"""


async def dashboard(request):
    return HTMLResponse(HTML_PAGE)


async def on_startup():
    asyncio.create_task(price_loop())
    logger.info("Price loop started")


routes = [
    Route("/", dashboard),
    Route("/api/status", api_status),
    Route("/health", health),
]

app = Starlette(routes=routes, on_startup=[on_startup])
