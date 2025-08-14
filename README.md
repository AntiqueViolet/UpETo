<h1 align="center">ETArch — надёжная архивация MySQL с Telegram‑ботом</h1>

<p align="center">
  <a href="https://github.com/AntiqueViolet/UpETo">
    <img src="https://img.shields.io/github/actions/workflow/status/AntiqueViolet/UpETo/docker-publish.yml?label=Build&logo=github" alt="Build Status">
  </a>
  <a href="https://hub.docker.com/r/malolet/upeto">
    <img src="https://img.shields.io/docker/pulls/malolet/upeto?logo=docker" alt="Docker Pulls">
  </a>
</p>

---

## 📖 Описание

**ETArch** — сервис для пакетной архивации строк в MySQL по условию `validity < now - 1 day` с устойчивостью к обрывам, автоматическим переподключением, управлением через Telegram и развёртыванием в Docker.

---

## ✨ Возможности
- Батчевое обновление с безопасными коммитами.
- Плавные задержки между батчами (пауза + джиттер + extra).
- Автоматическое уменьшение размера батча при тайм‑аутах.
- Telegram‑бот: `/status`, `/run_now`, `/restart_container`.
- Планировщик CRON в любой таймзоне.
- Чекпоинт прогресса и возобновление после сбоев.
- Логи с ротацией в файл и stdout.
- Полная упаковка в Docker.

---

## 🚀 Быстрый старт

1. **Настройка** (`docker-compose.yml`):
```yaml
services:
  etarch:
    build: .
    restart: always
    environment:
      DB_USER: root
      DB_PASSWORD: dbpassword
      DB_HOST: 127.0.0.1
      DB_PORT: "3306"
      DB_NAME: dbname
      TG_BOT_TOKEN: "<ваш_токен>"
      TG_CHAT_ID: "<id_чата>"
      TG_ALLOWED_USER_IDS: "<ваш_user_id>"
      CRON_TIME: "10 2 * * *"
      TZ: "Europe/Moscow"
      LOG_FILE: /app/data/etarch.log
      CKPT_FILE: /app/data/etarch.ckpt.json
    volumes:
      - ./data:/app/data
```

2. **Сборка и запуск**:
```bash
docker compose build
docker compose up -d
```

3. **Проверка логов**:
```bash
docker compose logs -f
```

---

## 🤖 Примеры работы Telegram‑бота

**Запрос**:
```
/status
```

**Ответ**:
```
{
  "started_at": "2025-08-14T02:10:00Z",
  "finished_at": "2025-08-14T02:15:42Z",
  "running": false,
  "total_updated": 184320,
  "last_id": 9821131,
  "error": null,
  "batch_size": 20000
}
```

---

**Запрос**:
```
/run_now
```

**Ответ**:
```
Запускаю задачу архивации...
✅ Готово. Итог: обновлено 152000 строк.
```

---

## ⚙️ Переменные окружения

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `DB_USER` | — | Пользователь MySQL |
| `DB_PASSWORD` | — | Пароль MySQL |
| `DB_HOST` | — | Хост БД |
| `DB_PORT` | — | Порт БД |
| `DB_NAME` | — | Имя базы |
| `BATCH_SIZE` | 20000 | Размер батча |
| `MIN_BATCH_SIZE` | 2000 | Минимальный батч при сбоях |
| `BATCH_SLEEP_MS` | 150 | Пауза между батчами |
| `BATCH_SLEEP_JITTER_MS` | 100 | Джиттер задержки |
| `SLOWDOWN_AFTER_ROWS` | 15000 | Порог для extra‑паузы |
| `SLOWDOWN_EXTRA_MS` | 500 | Доп. пауза (мс) |
| `READ_TIMEOUT` | 120 | Тайм‑аут чтения |
| `WRITE_TIMEOUT` | 120 | Тайм‑аут записи |
| `CRON_TIME` | "10 2 * * *" | Расписание |
| `TZ` | UTC | Таймзона |
| `TG_BOT_TOKEN` | — | Токен бота |
| `TG_CHAT_ID` | — | ID чата |
| `TG_ALLOWED_USER_IDS` | — | Разрешённые пользователи |

---

## 🛠 Тюнинг и советы
- Уменьшайте `BATCH_SIZE`, если БД не справляется.
- Увеличивайте `READ_TIMEOUT`/`WRITE_TIMEOUT` при долгих батчах.
- Добавьте индекс `(validity, status, id)` для ускорения.

---

## 📜 Лицензия
MIT
