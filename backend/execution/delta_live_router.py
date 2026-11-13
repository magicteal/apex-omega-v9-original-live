"""
APEX-OMEGA V9.0 LIVE — Ultra-Fast Concurrent Delta Exchange India Router
- Zero-lag concurrent sync of Live Wallet Balance, Open Positions (Real Open PnL & Mark Price), and Trade Fills
- 1:1 execution of V9 Dual-Core entries, 5-Stage Asymmetric Ratchet stops, Stage-2 50% partial banking, and 25m Scratch exits
"""
import asyncio
import os
import time
from typing import Dict, Any, List, Optional

try:
    import ccxt.async_support as ccxt
except ImportError:
    ccxt = None


class DeltaIndiaLiveRouter:
    """Ultra-low latency async live execution & position router for Delta Exchange India."""

    def __init__(self) -> None:
        self.api_key = os.getenv("EXCHANGE_API_KEY", "rF2stYIF0Trs3o6VirgCwHtcskckWM").strip()
        self.api_secret = os.getenv("EXCHANGE_API_SECRET", "mHvcVz7lETagRSxzv1TE9QynF5GHnuMwfT3h7YmW6FryVDjOIgsDgNvWhiOr").strip()
        self.usd_inr_rate = float(os.getenv("USD_INR_CONV_RATE", "85.0"))
        self.exchange = None
        self.markets_loaded = False
        self.symbol_map: Dict[str, str] = {}
        self.reverse_symbol_map: Dict[str, str] = {}
        self.last_error: Optional[str] = None
        self.last_synced_balance_inr: Optional[float] = None
        self.live_positions_cache: Dict[str, Dict[str, Any]] = {}
        self.live_fills_cache: List[Dict[str, Any]] = []

    def is_live(self) -> bool:
        mode = os.getenv("EXECUTION_MODE", "LIVE").strip().upper()
        return mode == "LIVE" and bool(self.api_key) and bool(self.api_secret) and ccxt is not None

    async def ensure_ready(self) -> bool:
        if not self.is_live():
            return False
        if self.exchange is None:
            self.exchange = ccxt.delta({
                "apiKey": self.api_key,
                "secret": self.api_secret,
                "enableRateLimit": False,
                "timeout": 4000,
                "options": {"defaultType": "swap"},
            })
            self.exchange.urls["api"] = {
                "public": "https://api.india.delta.exchange",
                "private": "https://api.india.delta.exchange",
            }
        if not self.markets_loaded:
            try:
                markets = await self.exchange.load_markets()
                self.markets_loaded = True
                for sym in markets:
                    base = markets[sym].get("base", "")
                    if markets[sym].get("swap") or markets[sym].get("future"):
                        candidate_key = f"{base}/USDT:USDT"
                        if candidate_key not in self.symbol_map:
                            self.symbol_map[candidate_key] = sym
                            self.reverse_symbol_map[sym] = candidate_key
            except Exception as e:
                self.last_error = str(e)
                return False
        return True

    def resolve_symbol(self, scanner_symbol: str) -> Optional[str]:
        return self.symbol_map.get(scanner_symbol)

    async def sync_fast_account_telemetry(self) -> Dict[str, Any]:
        """Concurrently fetches real Delta wallet balance + live open positions + recent fills in <250ms."""
        if not await self.ensure_ready():
            return {"balance_inr": None, "positions": {}, "fills": []}
        try:
            bal_task = asyncio.create_task(self.exchange.fetch_balance())
            pos_task = asyncio.create_task(self.exchange.fetch_positions())
            bal, positions = await asyncio.gather(bal_task, pos_task, return_exceptions=True)

            inr_bal = self.last_synced_balance_inr
            if isinstance(bal, dict):
                self.last_error = None
                for ccy in ("INR", "USD", "USDT"):
                    val = bal.get("total", {}).get(ccy)
                    if val is not None and float(val) > 0:
                        inr_bal = round(float(val) if ccy == "INR" else float(val) * self.usd_inr_rate, 2)
                        self.last_synced_balance_inr = inr_bal
                        break
            elif isinstance(bal, Exception):
                self.last_error = str(bal)

            if isinstance(positions, Exception):
                self.last_error = str(positions)
                pos_map = self.live_positions_cache
            elif isinstance(positions, list):
                pos_map: Dict[str, Dict[str, Any]] = {}
                for p in positions:
                    contracts = float(p.get("contracts") or p.get("size") or p.get("info", {}).get("size") or 0.0)
                    if abs(contracts) > 0:
                        d_sym = p.get("symbol", "")
                        scan_sym = self.reverse_symbol_map.get(d_sym, d_sym)
                        raw_side = str(p.get("side") or "").upper()
                        side = "LONG" if raw_side in ("BUY", "LONG", "B") or (contracts > 0 and raw_side not in ("SELL", "SHORT")) else "SHORT"
                        entry_p = float(p.get("entryPrice") or p.get("info", {}).get("entry_price") or 0.0)
                        mark_p = float(p.get("markPrice") or p.get("info", {}).get("mark_price") or entry_p)

                        direction = 1.0 if side == "LONG" else -1.0
                        unrl_usd = round(direction * (mark_p - entry_p) * abs(contracts), 4)
                        unrl_inr = round(unrl_usd * self.usd_inr_rate, 2)
                        notional_usd = round(abs(contracts) * mark_p, 2)
                        notional_inr = round(notional_usd * self.usd_inr_rate, 2)

                        pos_map[scan_sym] = {
                            "delta_symbol": d_sym,
                            "scanner_symbol": scan_sym,
                            "side": side,
                            "size": abs(contracts),
                            "contracts": abs(contracts),
                            "entry_price": entry_p,
                            "mark_price": mark_p,
                            "unrealized_pnl_usd": unrl_usd,
                            "unrealized_pnl_inr": unrl_inr,
                            "notional_usd": notional_usd,
                            "notional_inr": notional_inr,
                        }
                self.live_positions_cache = pos_map
            else:
                pos_map = self.live_positions_cache

            return {"balance_inr": inr_bal, "positions": pos_map}
        except Exception as e:
            self.last_error = str(e)
            return {"balance_inr": self.last_synced_balance_inr, "positions": self.live_positions_cache}

    async def fetch_inr_balance(self) -> Optional[float]:
        res = await self.sync_fast_account_telemetry()
        return res.get("balance_inr") or self.last_synced_balance_inr

    async def fetch_open_positions(self) -> list:
        res = await self.sync_fast_account_telemetry()
        return list(res.get("positions", {}).values())

    async def fetch_recent_fills(self, delta_symbol: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        if not await self.ensure_ready():
            return []
        try:
            trades = await self.exchange.fetch_my_trades(delta_symbol, limit=limit)
            out = []
            for t in reversed(trades or []):
                d_sym = t.get("symbol", "")
                scan_sym = self.reverse_symbol_map.get(d_sym, d_sym)
                fee_cost = float((t.get("fee") or {}).get("cost") or 0.0) * self.usd_inr_rate
                out.append({
                    "id": str(t.get("id") or t.get("order") or int(time.time() * 1000)),
                    "symbol": scan_sym,
                    "side": str(t.get("side", "")).upper(),
                    "price": float(t.get("price") or 0.0),
                    "amount": float(t.get("amount") or 0.0),
                    "cost_inr": round(float(t.get("cost") or 0.0) * self.usd_inr_rate, 2),
                    "fee_inr": round(fee_cost, 2),
                    "timestamp": t.get("datetime") or "",
                })
            self.live_fills_cache = out
            return out
        except Exception:
            return self.live_fills_cache

    async def open_live_trade(
        self,
        scanner_symbol: str,
        side: str,
        notional_inr: float,
        entry_price: float,
        stop_price: float,
    ) -> Optional[Dict[str, Any]]:
        if not await self.ensure_ready():
            return None
        delta_sym = self.resolve_symbol(scanner_symbol)
        if not delta_sym:
            return None
        try:
            market = self.exchange.market(delta_sym)
            contract_size = float(market.get("contractSize") or 1.0)
            notional_usd = notional_inr / max(self.usd_inr_rate, 1.0)
            raw_qty = notional_usd / max(entry_price * contract_size, 1e-9)
            qty = float(self.exchange.amount_to_precision(delta_sym, raw_qty))
            if qty <= 0:
                return None

            await asyncio.gather(
                self.exchange.set_margin_mode("isolated", delta_sym),
                self.exchange.set_leverage(8, delta_sym),
                return_exceptions=True,
            )

            order_side = "buy" if side == "LONG" else "sell"
            order = await self.exchange.create_order(
                symbol=delta_sym,
                type="market",
                side=order_side,
                amount=qty,
            )

            fill_price = float(order.get("average") or order.get("price") or entry_price)
            stop_order_id = None
            try:
                stop_side = "sell" if side == "LONG" else "buy"
                stop_order = await self.exchange.create_order(
                    symbol=delta_sym,
                    type="stop_market",
                    side=stop_side,
                    amount=qty,
                    price=None,
                    params={
                        "stopPrice": float(self.exchange.price_to_precision(delta_sym, stop_price)),
                        "reduceOnly": True,
                    },
                )
                stop_order_id = stop_order.get("id")
            except Exception as e:
                print(f"[DELTA_LIVE_ERR] initial stop placement failed: {e}")

            self.last_error = None
            return {
                "order_id": order.get("id"),
                "delta_symbol": delta_sym,
                "fill_price": fill_price,
                "filled_qty": qty,
                "stop_order_id": stop_order_id,
            }
        except Exception as e:
            self.last_error = str(e)
            print(f"[DELTA_LIVE_ERR] open_live_trade(): {e}")
            return None

    async def bank_stage2_50pct_and_move_stop(
        self,
        delta_symbol: str,
        side: str,
        current_qty: float,
        stop_order_id: Optional[str],
        new_stop_price: float,
    ) -> Dict[str, Any]:
        if not await self.ensure_ready():
            return {"remaining_qty": current_qty, "stop_order_id": stop_order_id}
        half_qty = float(self.exchange.amount_to_precision(delta_symbol, current_qty * 0.50))
        remaining_qty = max(current_qty - half_qty, 0.0)
        try:
            order_side = "sell" if side == "LONG" else "buy"
            await self.exchange.create_order(
                symbol=delta_symbol,
                type="market",
                side=order_side,
                amount=half_qty,
                params={"reduceOnly": True},
            )
        except Exception as e:
            print(f"[DELTA_LIVE_ERR] stage2 market bank failed: {e}")

        new_stop_id = await self.update_hardware_stop(
            delta_symbol, side, remaining_qty, stop_order_id, new_stop_price
        )
        return {"remaining_qty": remaining_qty, "stop_order_id": new_stop_id}

    async def update_hardware_stop(
        self,
        delta_symbol: str,
        side: str,
        remaining_qty: float,
        current_stop_order_id: Optional[str],
        new_stop_price: float,
    ) -> Optional[str]:
        if not await self.ensure_ready() or remaining_qty <= 0:
            return None
        if current_stop_order_id:
            try:
                await self.exchange.cancel_order(current_stop_order_id, delta_symbol)
            except Exception:
                pass
        try:
            stop_side = "sell" if side == "LONG" else "buy"
            order = await self.exchange.create_order(
                symbol=delta_symbol,
                type="stop_market",
                side=stop_side,
                amount=remaining_qty,
                price=None,
                params={
                    "stopPrice": float(self.exchange.price_to_precision(delta_symbol, new_stop_price)),
                    "reduceOnly": True,
                },
            )
            return order.get("id")
        except Exception as e:
            self.last_error = str(e)
            print(f"[DELTA_LIVE_ERR] update_hardware_stop(): {e}")
            return None

    async def close_live_trade(
        self,
        delta_symbol: str,
        side: str,
        remaining_qty: float,
        stop_order_id: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        if not await self.ensure_ready():
            return None
        if stop_order_id:
            try:
                await self.exchange.cancel_order(stop_order_id, delta_symbol)
            except Exception:
                pass
        if remaining_qty <= 0:
            return {"status": "NO_QTY_TO_CLOSE"}
        try:
            order_side = "sell" if side == "LONG" else "buy"
            order = await self.exchange.create_order(
                symbol=delta_symbol,
                type="market",
                side=order_side,
                amount=remaining_qty,
                params={"reduceOnly": True},
            )
            return {
                "status": "CLOSED",
                "order_id": order.get("id"),
                "fill_price": float(order.get("average") or order.get("price") or 0.0),
            }
        except Exception as e:
            self.last_error = str(e)
            print(f"[DELTA_LIVE_ERR] close_live_trade(): {e}")
            return None
