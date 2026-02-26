# TECH_DEBT

-------

STATUS (2026-02-26) — по последнему архиву

✅ Сделано

BITGET подключён к CORE

добавлен в exchange factory (клиент создаётся)

подключён WS price stream в CORE/market_streams.py (_make_price_stream понимает BITGET)

добавлен в cfg.json -> exchanges.enabled (там сейчас есть "bitget")

REST: shared aiohttp.ClientSession (connection pooling)

Bitget funding/symbol больше не создают ClientSession() на каждый запрос (есть shared session + aclose()).

То же решение прокинуто на часть других funding/symbol клиентов (как и было задумано).

BITGET WS: stop-механизм унифицирован

в API/BITGET/price.py и API/BITGET/stakan.py stop переведён на asyncio.Event() (как “канон”).

BITGET WS: global reconnect limiter

добавлен общий ограничитель переподключений, чтобы чанки не создавали шторм reconnect’ов.

Snapshot guard: единый snap_now_ms

MarketStreams.get_price_info/get_book_info считают age_sec от переданного now_ms

в цикле кандидатов используется единый snap_now_ms (меньше “микса поколений” данных)

⚠️ Осталось / не выполнено относительно твоего “НОВОЕ”

OKX “пока отключим (чисто в настройках)” — НЕ сделано по дефолту

В cfg.json последнего архива OKX всё ещё включён ("okx" присутствует в exchanges.enabled).

Универсальная фабрика “подключение биржи по имени если есть API-блок” — НЕ делал

Сейчас подключение Bitget сделано прямым импортом/веткой, как ты и разрешил (“пока ограничимся прямым подключением”).

Универсальную автоподгрузку можно сделать отдельной задачей.

BITGET stakan stream в CORE не включён

В CORE/market_streams.py блок stakan сейчас заглушен (_ensure_stakan = pass), и фабрика stakan стримов закомментирована.

То есть Bitget book-stream модуль есть, но движком не используется (как и стаканы других бирж в этом режиме).