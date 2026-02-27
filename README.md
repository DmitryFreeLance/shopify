# Telegram → Shopify bot (Java)

## Что делает
- Получает новые посты и редактирования из канала (бот — админ).
- Обрабатывает только посты, где есть ключевое слово `Cena`.
- Если приходит `edited_channel_post` с ключевым словом `prodato` — удаляет товар в Shopify и удаляет пост в Telegram.
- Для новых постов отправляет фото + текст в Gemini (Kie.ai, модель `gemini-3-flash`) и создаёт товар в Shopify.
- Создаёт коллекции (разделы/подразделы) автоматически, если их нет.
- Цена в EUR идёт в цену товара, RSD дописывается в описание.

## Переменные окружения
Обязательные:
- `TELEGRAM_BOT_TOKEN`
- `KIE_API_KEY`
- `SHOPIFY_SHOP_DOMAIN` (например `my-shop.myshopify.com`)
- `SHOPIFY_ADMIN_TOKEN`

Опциональные:
- `TELEGRAM_CHANNEL_ID` (по умолчанию `-1003856584928`)
- `PRICE_KEYWORD` (по умолчанию `cena`)
- `SOLD_KEYWORDS` (по умолчанию `prodato`, можно несколько через запятую)
- `MEDIA_GROUP_FINALIZE_SECONDS` (по умолчанию `30`)
- `PRODUCT_SYNC_SECONDS` (по умолчанию `180`) — проверка: если товар удалён в Shopify вручную, бот удалит соответствующий пост в Telegram
- `SQLITE_PATH` (по умолчанию `./data/bot.db`)
- `KIE_MODEL` (по умолчанию `gemini-3-flash`)
- `KIE_FALLBACK_MODEL` (по умолчанию `gpt-5-2`)
- `KIE_BASE_URL` (по умолчанию `https://api.kie.ai`)
- `KIE_ENDPOINT` (полный URL, если нужно переопределить endpoint)
- `KIE_FALLBACK_ENDPOINT` (полный URL для fallback модели)
- `KIE_TIMEOUT_SECONDS` (по умолчанию `90`)
- `SHOPIFY_API_VERSION` (по умолчанию `2024-10`)
- `SHOPIFY_PUBLISH_ALL` (по умолчанию `true`)
- `MAX_IMAGE_BYTES` (по умолчанию `4194304`)

## Сборка локально
```bash
mvn -q -DskipTests package
java -jar target/telegram-shopify-bot.jar
```

## Docker
Сборка:
```bash
docker build -t telegram-shopify-bot .
```

Запуск:
```bash
docker run --rm \
  -e TELEGRAM_BOT_TOKEN=xxx \
  -e KIE_API_KEY=xxx \
  -e SHOPIFY_SHOP_DOMAIN=your-shop.myshopify.com \
  -e SHOPIFY_ADMIN_TOKEN=shpat_xxx \
  -e TELEGRAM_CHANNEL_ID=-1001234567890 \
  -v $(pwd)/data:/data \
  telegram-shopify-bot
```

## Коллекции
Бот создаёт ручные коллекции (Custom Collections):
- Muško
- Žensko
- Dečija kolekcija
- Sniženje
- Muško / [подразделы]
- Žensko / [подразделы]

## Примечания
- Обновления удаления постов Telegram Bot API не предоставляет. Удаление товара происходит только при `edited_channel_post` с ключевым словом `prodato`.
- По умолчанию бот публикует товар во всех доступных sales channels через GraphQL.
- Kie.ai вызывается в OpenAI-совместимом формате с `image_url` и `data:image/jpeg;base64,...`. Если у вас другой endpoint или формат, задайте `KIE_ENDPOINT`.
