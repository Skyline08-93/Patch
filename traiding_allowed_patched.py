import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import signal
from datetime import datetime, timezone
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes

# === Настройки ===
commission_rate = 0.001
min_profit = 0.1
max_profit = 3.0
min_trade_volume = 10
max_trade_volume = 100
triangle_hold_time = 5
real_trading_enabled = True
debug_mode = True
log_file = "triangle_log.csv"
is_shutting_down = False
last_cycle_time = None
triangle_cache = {}

# === Telegram ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

# === Bybit API ===
exchange = ccxt.bybit({
    "options": {"defaultType": "unified"},
    "enableRateLimit": True,
    "apiKey": os.getenv("BYBIT_API_KEY"),
    "secret": os.getenv("BYBIT_API_SECRET")
})

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
        if debug_mode: print(f"[BALANCE DEBUG] Raw: {balance}")
        if 'list' in balance.get('info', {}).get('result', {}):
            for asset in balance['info']['result']['list'][0]['coin']:
                if asset['coin'] == coin:
                    return float(asset['equity'])  # Используем equity (total баланс)
        return float(balance['total'].get(coin, {}).get('totalBalance', 0))  # Резервный вариант
    except Exception as e:
        print(f"[Ошибка баланса] {e}")
        return 0.0

async def fetch_balances():
    try:
        balances = await exchange.fetch_balance({'type': 'unified'})
        result = {}
        if debug_mode: print(f"[FETCH BALANCES RAW]: {balances}")
        if 'list' in balances.get('info', {}).get('result', {}):
            for asset in balances['info']['result']['list'][0]['coin']:
                result[asset['coin']] = {
                    'free': float(asset.get('equity', 0)),  # Используем equity
                    'total': float(asset.get('equity', 0))
                }
        return result
    except Exception as e:
        print(f"[Ошибка fetch_balances]: {e}")
        return {}

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
        balances = await fetch_balances()
        if not balances:
            await update.message.reply_text("⚠️ Не удалось получить балансы")
            return
        msg = "<b>💼 Балансы:</b>\n"
        for coin, b in balances.items():
            msg += f"{coin}: {b['free']:.4f} / {b['total']:.4f} (free / total)\n"
        available = await get_available_balance('USDT')
        msg += f"\n<b>🔄 Доступно для торговли:</b> {available:.2f} USDT"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[Ошибка команды /balance]: {e}")
        await update.message.reply_text("❌ Ошибка получения баланса.")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        global last_cycle_time
        now = datetime.now(timezone.utc)
        msg = "<b>🟢 Бот работает</b>\n"
        msg += f"Последний цикл: {last_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} UTC\n" if last_cycle_time else "Цикл ещё не запускался.\n"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[Ошибка команды /status]: {e}")
        await update.message.reply_text("❌ Ошибка при проверке статуса.")

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

# Добавьте в настройки
min_order_value = 1.0  # Минимальная сумма ордера в USDT (1 USDT для USDT-пар)

async def execute_real_trade(route_id, steps, base_coin, markets, dynamic_volume):
    print(f"\n🚀 Старт сделки {route_id}, объем: {dynamic_volume:.2f} {base_coin}")
    try:
        # Проверка минимального объема
        if dynamic_volume < min_order_value:
            msg = f"❌ Объем {dynamic_volume:.2f} USDT меньше минимума {min_order_value} USDT"
            print(msg)
            await send_telegram_message(msg)
            return False

        total_balance = await get_available_balance(base_coin)
        if total_balance < dynamic_volume:
            msg = f"❌ Недостаточно {base_coin}. Всего: {total_balance:.2f}, нужно: {dynamic_volume:.2f}"
            print(msg)
            await send_telegram_message(msg)
            return False

        executed_orders = []
        current_amount = dynamic_volume

        for i, (symbol, side, price, amount) in enumerate(steps, 1):
            try:
                market = markets[symbol]
                
                # Получаем точность объема и цены
                amount_precision = int(market['precision']['amount'])
                price_precision = int(market['precision']['price'])
                
                # Корректное округление объема
                adjusted_amount = round(float(amount), amount_precision)
                min_amount = float(market['limits']['amount']['min'])
                
                if adjusted_amount < min_amount:
                    msg = f"❌ Объем {adjusted_amount} {symbol.split('/')[0]} < минимума {min_amount}"
                    print(msg)
                    raise ValueError(msg)

                # Корректное округление цены с учетом минимального шага
                price_step = 10 ** -price_precision
                raw_price = float(price)
                rounded_price = round(raw_price / price_step) * price_step
                
                # Проверка минимальной цены
                min_price = float(market['limits']['price']['min'])
                if rounded_price < min_price:
                    rounded_price = min_price
                
                print(f"🔹 Ордер {i}: {side.upper()} {adjusted_amount} {symbol} @ {rounded_price:.{price_precision}f}")
                
                # Дополнительная проверка перед отправкой ордера
                if rounded_price <= 0:
                    raise ValueError(f"Некорректная цена: {rounded_price}")

                order = await exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=side,
                    amount=adjusted_amount,
                    price=rounded_price,
                    params={
                        'timeInForce': 'PostOnly',
                        'price': str(rounded_price)  # Явное преобразование в строку
                    }
                )
                executed_orders.append(order)
                await asyncio.sleep(3)
                
                # Проверка статуса ордера
                order_status = None
                try:
                    closed_orders = await exchange.fetchClosedOrders(symbol, limit=5)
                    order_status = next((o for o in closed_orders if o['id'] == order['id']), None)
                except Exception as e:
                    print(f"⚠️ Ошибка при проверке закрытых ордеров: {e}")

                if not order_status:
                    try:
                        open_orders = await exchange.fetchOpenOrders(symbol)
                        order_status = next((o for o in open_orders if o['id'] == order['id']), None)
                        if order_status:
                            print(f"🟡 Ордер еще открыт: {order_status['status']}")
                            await exchange.cancel_order(order['id'], symbol)
                            raise ValueError("Ордер не исполнился и был отменен")
                    except Exception as e:
                        print(f"⚠️ Ошибка при проверке открытых ордеров: {e}")

                if not order_status:
                    raise ValueError(f"❌ Ордер {order['id']} не найден")

                filled = float(order_status['filled'])
                print(f"✅ Статус ордера: filled={filled}")
                
                if filled == 0:
                    raise ValueError(f"❌ Ордер не исполнен: {order_status}")
                
                if side == 'buy':
                    current_amount = filled * float(order_status['average'])
                else:
                    current_amount = filled
                    
            except Exception as e:
                print(f"🔥 Ошибка шага {i}: {e}")
                await send_telegram_message(f"🔥 Ошибка шага {i} ({symbol}): {str(e)}")
                for o in executed_orders:
                    try: 
                        await exchange.cancel_order(o['id'], o['symbol'])
                    except: 
                        pass
                return False

        profit = current_amount - dynamic_volume
        percent = (profit / dynamic_volume) * 100
        msg = (f"✅ <b>Сделка завершена</b>\n"
               f"Маршрут: {route_id}\n"
               f"📈 Прибыль: {profit:.2f} USDT ({percent:.2f}%)\n"
               f"📤 Исходный объем: {dynamic_volume:.2f} USDT\n"
               f"📥 Финальный объем: {current_amount:.2f} USDT")
        print(msg)
        await send_telegram_message(msg)
        with open(log_file, "a") as f:
            f.write(f"{datetime.utcnow()},{route_id},{profit:.4f},{percent:.4f}\n")
        return True
        
    except Exception as e:
        print(f"🔥 Критическая ошибка: {e}")
        await send_telegram_message(f"🔥 Ошибка исполнения: {e}")
        return False

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
        if profit_percent < min_profit or profit_percent > max_profit: return

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
        if prev_time and (now - prev_time).total_seconds() < triangle_hold_time: return
        triangle_cache[route_hash] = now

        print(f"♻️ Арбитраж найден: {route_id} | Спред: {profit_percent:.2f}%")
        await send_telegram_message(f"♻️ Арбитраж: {route_id}\nСпред: {profit_percent:.2f}%\nОбъем: {dynamic_volume:.2f} USDT")
        await execute_real_trade(route_id, trade_steps, base, markets, dynamic_volume)  # Исправлено: base_coin -> base
    except Exception as e:
        print(f"[Ошибка {base}->{mid1}->{mid2}]: {e}")

async def load_symbols():
    markets = await exchange.load_markets()
    return [s for s in markets.keys() if ":" not in s], markets

async def main():
    global last_cycle_time
    telegram_app.add_handler(CommandHandler("balance", balance_command))
    telegram_app.add_handler(CommandHandler("status", status_command))
    await telegram_app.initialize()
    await telegram_app.start()
    await send_telegram_message("♻️ Бот запущен и готов к реальной торговле")

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
                if f"{mid2}/{base}" in symbols or f"{base}/{mid2}" in symbols:
                    triangles.append((base, mid1, mid2))

    while not is_shutting_down:
        last_cycle_time = datetime.now(timezone.utc)
        print(f"\n🔄 Новый цикл: {last_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} | Треугольников: {len(triangles)}")
        await asyncio.gather(*[check_triangle(b, m1, m2, symbols, markets) for b, m1, m2 in triangles])
        await asyncio.sleep(10)

    await telegram_app.stop()
    await telegram_app.shutdown()
    await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())