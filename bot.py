import asyncio, os, random, re, sqlite3, statistics, time
from urllib.parse import quote, urlparse, unquote

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from bs4 import BeautifulSoup

BOT_TOKEN = os.environ["BOT_TOKEN"]
PROXY = os.getenv("PROXY") or None          # http://user:pass@host:port (рекомендуется)
CITY = os.getenv("CITY", "rossiya")          # moskva, sankt-peterburg, rossiya ...
INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))   # сек между кругами проверки
DEFAULT_DISCOUNT = int(os.getenv("DISCOUNT", "25")) # % ниже медианы рынка
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "data.db")

def _proxy_args():
    if not PROXY:
        return {}
    u = urlparse(PROXY)
    args = {"proxy": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        args["proxy_auth"] = aiohttp.BasicAuth(unquote(u.username), unquote(u.password or ""))
    return args


PROXY_ARGS = _proxy_args()


def safe(e):
    """не светим логин/пароль прокси в сообщениях"""
    t = str(e)
    if PROXY:
        t = t.replace(PROXY, "<proxy>")
        u = urlparse(PROXY)
        if u.password:
            t = t.replace(u.password, "***")
    return t


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.executescript("""
CREATE TABLE IF NOT EXISTS subs(id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER,
  query TEXT, discount INTEGER, initialized INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS seen(sub_id INTEGER, item_id TEXT, PRIMARY KEY(sub_id, item_id));
""")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
market_cache = {}  # query -> (ts, median)
stats = {}         # query -> {"ok": ts, "found": n, "median": x, "err": str, "fails": n}


async def fetch_items(session, query, sort=104):
    """sort=104 — по дате (новые), 101 — по умолчанию (для оценки рынка)"""
    url = f"https://www.avito.ru/{CITY}?q={quote(query)}&s={sort}"
    async with session.get(url, headers=HEADERS, **PROXY_ARGS,
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status != 200:
            raise RuntimeError(f"Avito вернул {r.status} (нужен прокси / бан по IP)")
        html = await r.text()
    soup = BeautifulSoup(html, "html.parser")
    low = html.lower()
    if "captcha" in low or "доступ ограничен" in low or "firewall" in low:
        raise RuntimeError("Авито показал капчу/блокировку (нужен прокси)")
    items = []
    for el in soup.select('div[data-marker="item"]'):
        a = el.select_one('a[data-marker="item-title"]')
        p = el.select_one('meta[itemprop="price"]')
        if not a or not p or not p.get("content", "").isdigit():
            continue
        items.append({
            "id": el.get("data-item-id"),
            "title": a.get("title") or a.get_text(strip=True),
            "price": int(p["content"]),
            "url": "https://www.avito.ru" + a["href"],
        })
    if not items:
        raise RuntimeError("Объявления не найдены: блокировка или изменилась вёрстка Авито")
    return items


async def market_median(session, query):
    ts, med = market_cache.get(query, (0, None))
    if time.time() - ts < 3600 and med:
        return med
    items = await fetch_items(session, query, sort=101)
    prices = [i["price"] for i in items if i["price"] > 500]
    med = statistics.median(prices) if len(prices) >= 5 else None
    market_cache[query] = (time.time(), med)
    return med


async def worker():
    async with aiohttp.ClientSession() as session:
        while True:
            for sub_id, chat_id, query, discount, inited in db.execute(
                    "SELECT id, chat_id, query, discount, initialized FROM subs").fetchall():
                try:
                    items = await fetch_items(session, query)
                    med = await market_median(session, query)
                except Exception as e:
                    st = stats.setdefault(query, {"fails": 0})
                    st["err"], st["fails"] = safe(e), st.get("fails", 0) + 1
                    print("ERR", query, safe(e), flush=True)
                    if st["fails"] == 3:
                        await bot.send_message(chat_id, f"⚠️ «{query}»: 3 ошибки подряд.\n{safe(e)}")
                    await asyncio.sleep(random.uniform(5, 15))
                    continue
                stats[query] = {"ok": time.strftime("%H:%M:%S"), "found": len(items),
                                "median": med, "err": None, "fails": 0}
                print("OK", query, len(items), "median", med, flush=True)
                for it in items:
                    if db.execute("SELECT 1 FROM seen WHERE sub_id=? AND item_id=?",
                                  (sub_id, it["id"])).fetchone():
                        continue
                    db.execute("INSERT INTO seen VALUES(?,?)", (sub_id, it["id"]))
                    if not inited or not med:
                        continue  # первый проход — просто запоминаем
                    if it["price"] <= med * (1 - discount / 100):
                        off = round((1 - it["price"] / med) * 100)
                        await bot.send_message(
                            chat_id,
                            f"🔥 <b>{it['title']}</b>\n💰 {it['price']:,} ₽ "
                            f"(рынок ≈ {int(med):,} ₽, дешевле на {off}%)\n{it['url']}".replace(",", " "),
                            parse_mode="HTML")
                db.execute("UPDATE subs SET initialized=1 WHERE id=?", (sub_id,))
                db.commit()
                await asyncio.sleep(random.uniform(3, 8))
            await asyncio.sleep(INTERVAL)


@dp.message(Command("start"))
async def start(m: Message):
    await m.answer(
        "Привет! Я ищу выгодные объявления на Авито сразу после публикации.\n\n"
        "/add iphone 13 — следить за запросом\n"
        "/list — мои запросы\n"
        "/del 1 — удалить запрос\n/status — состояние бота\n/check rtx 2060 — проверить связь с Авито\n"
        "/discount 1 30 — порог «дешевле рынка» в % для запроса 1\n\n"
        f"По умолчанию уведомляю, если цена ниже медианы рынка на {DEFAULT_DISCOUNT}%.")


@dp.message(Command("add"))
async def add(m: Message, command: CommandObject):
    q = (command.args or "").strip()
    if not q:
        return await m.answer("Пример: /add macbook air m1")
    db.execute("INSERT INTO subs(chat_id, query, discount) VALUES(?,?,?)",
               (m.chat.id, q, DEFAULT_DISCOUNT))
    db.commit()
    await m.answer(f"✅ Слежу за «{q}». Новые выгодные объявления пришлю сюда.")


@dp.message(Command("list"))
async def lst(m: Message):
    rows = db.execute("SELECT id, query, discount FROM subs WHERE chat_id=?", (m.chat.id,)).fetchall()
    await m.answer("\n".join(f"{i}. {q} (−{d}%)" for i, q, d in rows) or "Пока пусто. /add запрос")


@dp.message(Command("del"))
async def delete(m: Message, command: CommandObject):
    if not (command.args or "").isdigit():
        return await m.answer("Пример: /del 1")
    db.execute("DELETE FROM subs WHERE id=? AND chat_id=?", (int(command.args), m.chat.id))
    db.commit()
    await m.answer("Удалено")


@dp.message(Command("discount"))
async def discount(m: Message, command: CommandObject):
    parts = (command.args or "").split()
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        return await m.answer("Пример: /discount 1 30")
    db.execute("UPDATE subs SET discount=? WHERE id=? AND chat_id=?",
               (int(parts[1]), int(parts[0]), m.chat.id))
    db.commit()
    await m.answer("Готово")


@dp.message(Command("status"))
async def status(m: Message):
    rows = db.execute("SELECT query FROM subs WHERE chat_id=?", (m.chat.id,)).fetchall()
    if not rows:
        return await m.answer("Нет запросов. /add запрос")
    out = []
    for (q,) in rows:
        st = stats.get(q)
        if not st:
            out.append(f"• {q}: ещё не проверялся")
        elif st.get("err"):
            out.append(f"• {q}: ❌ {st['err']} (ошибок подряд: {st['fails']})")
        else:
            med = f"{int(st['median']):,} ₽".replace(",", " ") if st["median"] else "нет (мало данных)"
            out.append(f"• {q}: ✅ {st['ok']}, найдено {st['found']}, медиана {med}")
    await m.answer("\n".join(out))


@dp.message(Command("check"))
async def check(m: Message, command: CommandObject):
    q = (command.args or "").strip()
    if not q:
        return await m.answer("Пример: /check rtx 2060")
    async with aiohttp.ClientSession() as session:
        try:
            items = await fetch_items(session, q)
            med = await market_median(session, q)
        except Exception as e:
            return await m.answer(f"❌ {safe(e)}")
    cheap = sorted(items, key=lambda i: i["price"])[:3]
    txt = f"✅ Авито отвечает. Найдено {len(items)}, медиана: {int(med) if med else 'нет'} ₽\n\n"
    txt += "\n".join(f"{i['price']} ₽ — {i['title']}\n{i['url']}" for i in cheap)
    await m.answer(txt)


async def main():
    asyncio.create_task(worker())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
