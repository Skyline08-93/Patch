# === traiding_allowed.patched.py ===
# ⚠️ Версия без симуляции. Только РЕАЛЬНАЯ ТОРГОВЛЯ с полным логированием.

import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import signal
import time
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes
from datetime import datetime, timezone

# === Настройки ===
commission_rate = 0.001
min_profit = 0.1
max_profit = 3.0
min_trade_volume = 10
max_trade_volume = 100
scan_liquidity_range = (10, 1000)
real_trading_enabled = True  # 🔥 ВСЕГДА торгует по-настоящему!
debug_mode = True
triangle_cache = {}
triangle_hold_time = 5
log_file = "triangle_log.csv"
is_shutting_down = False

# Telegram setup
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

# Биржа Bybit Unified
exchange = ccxt.bybit({
    "options": {"defaultType": "unified"},
    "enableRateLimit": True,
    "apiKey": os.getenv("BYBIT_API_KEY"),
    "secret": os.getenv("BYBIT_API_SECRET")
})

# === Служебные ===
def handle_signal(signum, frame):
    global is_shutting_down
    is_shutting_down = True

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

async def send_telegram_message(text):
    try:
        await telegram_app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[Ошибка Telegram]: {e}")

async def get_available_balance(coin='USDT'):
    try:
        balance = await exchange.fetch_balance({'type': 'unified'})
        if 'list' in balance.get('info', {}).get('result', {}):
            for asset in balance['info']['result']['list'][0]['coin']:
                if asset['coin'] == coin:
                    return float(asset['availableToWithdraw'])
        return float(balance['total'].get(coin, {}).get('availableBalance', 0))
    except Exception as e:
        print(f"[Ошибка баланса] {e}")
        return 0.0

async def execute_real_trade(route_id, steps, base_coin, markets, dynamic_volume):
    print(f"\n🚀 [TORGOVLYA] Старт сделки {route_id}, объем: {dynamic_volume:.2f} {base_coin}")
    try:
        available = await get_available_balance(base_coin)
        if available < dynamic_volume:
            msg = f"❌ Недостаточно {base_coin}. Доступно: {available:.2f}, нужно: {dynamic_volume:.2f}"
            print(msg)
            await send_telegram_message(msg)
            return False

        executed_orders = []
        current_amount = dynamic_volume

        for i, (symbol, side, price, amount) in enumerate(steps, 1):
            try:
                market = markets[symbol]
                tick_size = market.get('precision', {}).get('price', 0.00000001)
                rounded_price = round(float(price) / tick_size) * tick_size
                print(f"\n🔹 Ордер {i}: {side.upper()} {amount:.6f} {symbol} по цене {rounded_price:.6f}")

                order = await exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=side,
                    amount=float(amount),
                    price=rounded_price,
                    params={'timeInForce': 'PostOnly'}
                )
                executed_orders.append(order)

                await asyncio.sleep(3)
                order_status = await exchange.fetch_order(order['id'], symbol)
                filled = float(order_status['filled'])
                print(f"✅ Статус ордера: filled={filled}, avg={order_status.get('average')}")

                if filled == 0:
                    raise ValueError(f"❌ Ордер не исполнен: {order_status}")

                if side == 'buy':
                    current_amount = filled * float(order_status['average'])
                else:
                    current_amount = filled

            except Exception as e:
                err = f"🔥 Ошибка шага {i}: {e}"
                print(err)
                await send_telegram_message(err)
                for o in executed_orders:
                    try:
                        await exchange.cancel_order(o['id'], o['symbol'])
                    except: pass
                return False

        profit = current_amount - dynamic_volume
        profit_percent = (profit / dynamic_volume) * 100

        msg = (
            f"✅ <b>Сделка завершена</b>\n"
            f"Маршрут: {route_id}\n"
            f"📈 Прибыль: {profit:.2f} USDT ({profit_percent:.2f}%)\n"
            f"📤 Исходный объем: {dynamic_volume:.2f} USDT\n"
            f"📥 Финальный объем: {current_amount:.2f} USDT"
        )
        print(msg)
        await send_telegram_message(msg)

        with open(log_file, "a") as f:
            f.write(f"{datetime.utcnow()},{route_id},{profit:.4f},{profit_percent:.4f}\n")

        return True

    except Exception as e:
        err = f"🔥 Критическая ошибка: {e}"
        print(err)
        await send_telegram_message(err)
        return False

# === Новый блок: поиск и проверка треугольников ===
async def load_symbols():
    markets = await exchange.load_markets()
    return [s for s in markets.keys() if ":" not in s], markets

async def get_execution_price(symbol, side, target_usdt):
    try:
        ob = await exchange.fetch_order_book(symbol)
        side_data = ob['asks'] if side == 'buy' else ob['bids']
        total_base = total_usd = 0
        for price, volume in side_data:
            price, volume = float(price), float(volume)
            usd = price * volume
            if total_usd + usd >= target_usdt:
                total_base += (target_usdt - total_usd) / price
                total_usd = target_usdt
                break
            total_base += volume
            total_usd += usd
        if total_usd < target_usdt:
            return None, 0, total_usd
        return total_usd / total_base, total_usd, total_usd
    except Exception as e:
        print(f"[Ошибка стакана {symbol}]: {e}")
        return None, 0, 0

async def check_triangle(base, mid1, mid2, symbols, markets):
    try:
        route_id = f"{base}->{mid1}->{mid2}->{base}"
        s1 = f"{mid1}/{base}" if f"{mid1}/{base}" in symbols else f"{base}/{mid1}"
        s2 = f"{mid2}/{mid1}" if f"{mid2}/{mid1}" in symbols else f"{mid1}/{mid2}"
        s3 = f"{mid2}/{base}" if f"{mid2}/{base}" in symbols else f"{base}/{mid2}"
        if not all(s in symbols for s in [s1, s2, s3]): return

        side1 = 'buy' if f"{mid1}/{base}" in symbols else 'sell'
        side2 = 'buy' if f"{mid2}/{mid1}" in symbols else 'sell'
        side3 = 'sell' if f"{mid2}/{base}" in symbols else 'buy'

        p1, _, l1 = await get_execution_price(s1, side1, 100)
        p2, _, l2 = await get_execution_price(s2, side2, 100)
        p3, _, l3 = await get_execution_price(s3, side3, 100)
        if None in [p1, p2, p3]: return

        result = (1 / p1 if side1 == 'buy' else p1)
        result *= (1 / p2 if side2 == 'buy' else p2)
        result *= (p3 if side3 == 'sell' else 1 / p3)
        result *= (1 - commission_rate) ** 3
        profit_percent = (result - 1) * 100
        if profit_percent < min_profit or profit_percent > max_profit:
            return

        min_liq = min(l1, l2, l3)
        if min_liq < min_trade_volume: return

        dynamic_volume = min(max(min_liq * 0.9, min_trade_volume), max_trade_volume)
        trade_steps = [
            (s1, side1, p1, dynamic_volume),
            (s2, side2, p2, dynamic_volume / p1 if side1 == 'buy' else dynamic_volume * p1),
            (s3, side3, p3, dynamic_volume)
        ]

        now = datetime.now(timezone.utc)
        route_hash = hashlib.md5(route_id.encode()).hexdigest()
        prev_time = triangle_cache.get(route_hash)
        if prev_time is None:
            triangle_cache[route_hash] = now
            return
        elif (now - prev_time).total_seconds() < triangle_hold_time:
            return
        else:
            triangle_cache[route_hash] = now

        print(f"\n🧠 Арбитраж найден: {route_id} | Профит: {profit_percent:.2f}%")
        await send_telegram_message(f"🧠 Арбитраж: {route_id}\nСпред: {profit_percent:.2f}%\nОбъем: {dynamic_volume:.2f} USDT")
        await execute_real_trade(route_id, trade_steps, base, markets, dynamic_volume)

    except Exception as e:
        print(f"[Ошибка {base}->{mid1}->{mid2}]: {e}")

# === Запуск бота ===
async def main():
    await telegram_app.initialize()
    await telegram_app.start()
    await send_telegram_message("♻️ Бот запущен и готов к реальной торговле!")

    symbols, markets = await load_symbols()
    start_coins = ['USDT']
    triangles = []
    for base in start_coins:
        for s1 in symbols:
            if not s1.endswith('/' + base): continue
            mid1 = s1.split('/')[0]
            for s2 in symbols:
                if not s2.startswith(mid1 + '/'): continue
                mid2 = s2.split('/')[1]
                third = f"{mid2}/{base}"
                if third in symbols or f"{base}/{mid2}" in symbols:
                    triangles.append((base, mid1, mid2))

    while not is_shutting_down:
        await asyncio.gather(*[check_triangle(b, m1, m2, symbols, markets) for b, m1, m2 in triangles])
        await asyncio.sleep(10)

    await telegram_app.stop()
    await telegram_app.shutdown()
    await exchange.close()

if __name__ == '__main__':
    asyncio.run(main())