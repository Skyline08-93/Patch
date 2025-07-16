import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
from datetime import datetime, timezone
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes

# === Конфигурация ===
commission_rate = 0.001
min_profit = 0.1
max_profit = 3.0
min_trade_volume = 10
max_trade_volume = 100
triangle_hold_time = 5
real_trading_enabled = True
log_file = "triangle_log.csv"
debug_mode = True
triangle_cache = {}
last_cycle_time = None

# === Telegram ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

# === Bybit (unified account) ===
exchange = ccxt.bybit({
    "options": {"defaultType": "unified"},
    "enableRateLimit": True,
    "apiKey": os.getenv("BYBIT_API_KEY"),
    "secret": os.getenv("BYBIT_API_SECRET")
})

# === Утилиты ===
async def send_telegram_message(text):
    try:
        await telegram_app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[Telegram Error]: {e}")

async def get_available_balance(coin='USDT'):
    try:
        balance = await exchange.fetch_balance({'type': 'unified'})
        if 'list' in balance.get('info', {}).get('result', {}):
            for asset in balance['info']['result']['list'][0]['coin']:
                if asset['coin'] == coin:
                    return float(asset['availableToWithdraw'])
        return float(balance['total'].get(coin, {}).get('availableBalance', 0))
    except Exception as e:
        print(f"[Balance Error]: {e}")
        return 0.0

# === Telegram команды ===
async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("🔥 Вызвана /balance")
    balances = await exchange.fetch_balance({'type': 'unified'})
    msg = "<b>💼 Балансы:</b>\n"
    for asset in balances['info']['result']['list'][0]['coin']:
        coin = asset['coin']
        free = asset['availableToWithdraw']
        total = asset['equity']
        msg += f"{coin}: {free} / {total}\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("🔥 Вызвана /status")
    msg = f"<b>🟢 Бот работает</b>\nПоследний цикл: {last_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} UTC" if last_cycle_time else "Цикл ещё не запускался."
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

# === Исполнение сделки ===
async def execute_real_trade(route_id, steps, base_coin, markets, volume):
    print(f"🚀 Торговля {route_id} на сумму {volume}")
    available = await get_available_balance(base_coin)
    if available < volume:
        await send_telegram_message(f"❌ Недостаточно средств: {available:.2f} {base_coin}")
        return

    for i, (symbol, side, price, amount) in enumerate(steps, 1):
        try:
            market = markets[symbol]
            tick = market.get('precision', {}).get('price', 8)
            rounded_price = round(price, tick)
            order = await exchange.create_order(
                symbol=symbol,
                type='limit',
                side=side,
                amount=amount,
                price=rounded_price,
                params={'timeInForce': 'PostOnly'}
            )
            print(f"✅ Ордер {i} создан: {symbol} {side} {amount} по {rounded_price}")
            await asyncio.sleep(3)
            status = await exchange.fetch_order(order['id'], symbol)
            if status['filled'] == 0:
                raise Exception("Ордер не исполнен")
        except Exception as e:
            await send_telegram_message(f"❌ Ошибка ордера {i}: {e}")
            return

    await send_telegram_message(f"✅ Сделка завершена: {route_id} на сумму {volume:.2f} {base_coin}")

# === Проверка связки ===
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

        ob1 = await exchange.fetch_order_book(s1)
        ob2 = await exchange.fetch_order_book(s2)
        ob3 = await exchange.fetch_order_book(s3)

        p1 = ob1['asks'][0][0] if side1 == 'buy' else ob1['bids'][0][0]
        p2 = ob2['asks'][0][0] if side2 == 'buy' else ob2['bids'][0][0]
        p3 = ob3['bids'][0][0] if side3 == 'sell' else ob3['asks'][0][0]

        result = (1 / p1 if side1 == 'buy' else p1)
        result *= (1 / p2 if side2 == 'buy' else p2)
        result *= (p3 if side3 == 'sell' else 1 / p3)
        result *= (1 - commission_rate) ** 3
        profit_percent = (result - 1) * 100

        if profit_percent < min_profit or profit_percent > max_profit:
            return

        volume = min_trade_volume
        steps = [
            (s1, side1, p1, volume),
            (s2, side2, p2, volume / p1),
            (s3, side3, p3, volume / p1 / p2)
        ]

        await send_telegram_message(f"🧠 Арбитраж найден: {route_id}\nПрофит: {profit_percent:.2f}%")
        await execute_real_trade(route_id, steps, base, markets, volume)
    except Exception as e:
        print(f"[Ошибка check_triangle]: {e}")

# === Сканер торговли ===
async def trading_scanner():
    global last_cycle_time
    markets = await exchange.load_markets()
    symbols = list(markets.keys())
    start_coins = ['USDT']
    triangles = []

    for base in start_coins:
        for s1 in symbols:
            if not s1.endswith('/' + base): continue
            mid1 = s1.split('/')[0]
            for s2 in symbols:
                if not s2.startswith(mid1 + '/'): continue
                mid2 = s2.split('/')[1]
                if f"{mid2}/{base}" in symbols or f"{base}/{mid2}" in symbols:
                    triangles.append((base, mid1, mid2))

    while True:
        last_cycle_time = datetime.now(timezone.utc)
        print(f"\n🔄 Цикл: {last_cycle_time.strftime('%H:%M:%S')} | связок: {len(triangles)}")
        for b, m1, m2 in triangles:
            print(f"Проверка: {b}->{m1}->{m2}->{b}")
            await check_triangle(b, m1, m2, symbols, markets)
        await asyncio.sleep(10)

# === Запуск ===
async def main():
    telegram_app.add_handler(CommandHandler("balance", balance_command))
    telegram_app.add_handler(CommandHandler("status", status_command))
    asyncio.create_task(trading_scanner())
    print("🚀 Бот запущен. Telegram polling + торговля")
    await telegram_app.run_polling(close_loop=False)

if __name__ == "__main__":
    asyncio.run(main())