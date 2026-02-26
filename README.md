# funding_arb_bot

Бот ищет кросс-биржевые funding opportunities по perpetual futures (Binance / OKX / KuCoin / Phemex),
проверяет **funding spread + price component + качество стакана**, держит условие по TTL и отправляет сигнал в Telegram.

## Что уже умеет
- Нормализация funding rates в единую систему координат (в `%`)
- Сравнение только синхронных `nextFundingTime` (миллисекунды UTC)
- Перебор **всех** валидных LONG/SHORT пар внутри funding-кластера
- Онлайн price + orderbook streams
- TTL-подтверждение условия (funding/price/book проверяются повторно)
- Anti-hole book filters (`BOOK_QUALITY_*`)
- Планировщик по UTC-маркам (`SCHED_MARKS_*`)
- Test force scan mode (`TEST_FORCE_SCAN_MODE`)
- Отдельный инфо-алерт `PHEMEX TOP funding`

## Быстрый старт
1. Открой `cfg.json` (пользовательские настройки)
2. Укажи `TG_TOKEN`, `TG_CHAT_ID`
3. При необходимости подстрой пороги:
   - `FUNDING_SPREAD`
   - `TOTAL_FUNDING_SPREAD_MIN`
   - `BOOK_QUALITY_*`
   - `STAKAN_SPREAD_TTL`
4. Запуск:

```bash
python main.py
```

## Важные настройки (`cfg.json` + `consts.py`)
- `TEST_FORCE_SCAN_MODE` — не ждать расписание, сканировать сразу (удобно для тестов)
- `REST_FUNDING_REFRESH_EVERY_SEC` — как часто обновлять funding-кеш REST-бирж во время runtime
- `SCHED_MARKS_1H/4H/8H` + `*_OFFSET_SEC` — точные моменты UTC для сканов
- `BOOK_QUALITY_MAX_SPREAD_PCT` / `BOOK_QUALITY_TARGET_POS_USD` / `BOOK_QUALITY_MIN_TOP_COVERAGE_X` / `BOOK_QUALITY_MIN_SIDE_BALANCE_RATIO` — защита от дырявого стакана (position-based)

## Выходные артефакты
Папка `OUT/`:
- дампы universe / funding scans
- состояние dedup
- логи

## Примечание
Проект конфигурируется **через `cfg.json`** (пользовательские настройки). `consts.py` — единая точка загрузки/алиасов. `.env` не используется.


Локации сервера: Турция, Бразилия, Швейцария