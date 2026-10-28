# pastebin

Pastebin на Go с собственным S3-подобным хранилищем. Тексты лежат не в базе, а в виде блобов на нескольких storage-нодах с репликацией. Метаданные хранятся в Postgres, горячий контент и счётчики просмотров живут в Redis.

## Архитектура

```
             HTTP :8080
                 │
          ┌──────▼──────┐      ┌──────────────┐
          │  pastebin   ├─────►│ Redis        │  кэш контента, счётчики просмотров
          │             │      └──────────────┘
          │             │      ┌──────────────┐
          │             ├─────►│ Postgres     │  pastes: addr, TTL, просмотры
          └──────┬──────┘      └──────────────┘
                 │ gRPC :8090 (StorageNode API)
          ┌──────▼──────┐      ┌──────────────┐
          │   gateway   ├─────►│ Postgres     │  blobs, replicas, nodes
          └──┬───┬───┬──┘      └──────────────┘
             │   │   │  gRPC :8050 (тот же StorageNode API)
       ┌─────▼┐ ┌▼────┐ ┌▼─────┐
       │ sn1  │ │ sn2 │ │ sn3  │  блобы на диске
       └──────┘ └─────┘ └──────┘
```

Все три сервиса собраны в одном Go-модуле (`pastebin`). Gateway и storage-ноды реализуют **один и тот же** gRPC-сервис `StorageNode` (`api/proto/storagenode/v1/node.proto`), поэтому для pastebin gateway выглядит как одна большая надёжная нода.

### pastebin

HTTP API для пользователей.

- **Create.** Генерирует id блоба (16 случайных байт в hex) и считает sha256 контента. Кладёт блоб через gateway и сверяет хэш, который тот вернул. После этого создаёт строку в `pastes` с коротким адресом (6 случайных байт в base64url, 8 символов). На коллизию адреса даётся 3 попытки. При любой ошибке блоб удаляется.
- **View.** Берёт метаданные из Postgres, контент сначала ищет в Redis, при промахе идёт в gateway и кладёт результат в Redis. TTL в кэше равен `min(paste-cache-ttl, время до delete_at)`. Просмотр засчитывается через `HINCRBY` в Redis.
- **Burn after read.** Блоб читается, затем `ConsumePasteByAddr` атомарно выставляет `delete_at = now()`. Если два читателя пришли одновременно, пасту получит только один, второй получит 404.
- **Flush worker** раз в `flush-interval` переносит счётчики из Redis в Postgres. Сначала `RENAME views:pending → views:flushing`, потом пачечный `UPDATE ... FROM unnest(...)`, и только после этого `DEL`. Если запись в Postgres упала, данные остаются в `views:flushing` до следующей попытки.
- **Delete worker** раз в `delete-interval` берёт пасты, у которых `delete_at` истёк больше минуты назад, удаляет их блобы через gateway и затем строки из базы.

### gateway

Метаданные блобов, выбор нод и репликация.

- **PutBlob.** Тело читается целиком в память, считается sha256. Балансировщик выбирает `alive/2 + 1` нод, в `blobs`/`replicas` создаются строки со статусом `pending`, затем блоб параллельно пишется на все выбранные ноды, у каждой записи свой `write-timeout`. Нода считается успешной, если вернула тот же sha256. Клиент получает OK, когда закоммичена **хотя бы одна** реплика. Остальные копии добивает репликатор.
- **GetBlob.** Читает с закоммиченных реплик: сначала с живых нод, при ошибке переходит на следующую.
- **DeleteBlob.** Помечает блоб как `deleting`, физически его удаляет воркер.
- **Балансировщик** (`-balancer`):
  - `least-conns` (по умолчанию) выбирает ноду с максимальным весом `free/total / (активные соединения + 1)`;
  - `weighted-random` выбирает случайную ноду с вероятностью, пропорциональной её весу.

  Ноды, у которых свободного места меньше `размер блоба + 10 MiB`, пропускаются.
- **Воркеры:**
  - `checker` раз в `check-interval` опрашивает `GetStatus` каждой ноды. По ответу обновляет свободное место и флаг alive.
  - `replicate` раз в `replicate-interval` ищет блобы, у которых закоммиченных реплик меньше, чем живых нод, и копирует их с живой реплики на новые ноды.
  - `delete` удаляет реплики блобов в статусе `deleting` с нод, а затем строки из базы.
  - `janitor` чистит `pending`-блобы и реплики, которые старше `pending-stale`. Такие остаются после падений посреди загрузки или репликации.

  Очереди `delete` и `replicate` захватывают работу через `UPDATE ... checked_at = now() ... FOR UPDATE SKIP LOCKED` с сортировкой `checked_at NULLS FIRST`. Поэтому проблемные блобы уходят в конец очереди и не блокируют остальные.

### storagenode

Простое хранилище блобов на диске.

- Каждый блоб лежит отдельным файлом: `blobs/ab/cd/abcd…` (шардирование по первым двум байтам id).
- Запись идёт атомарно в три шага: временный файл в `tmp/` + `fsync`, затем `rename`, затем `fsync` каталога.
- `GetBlob` поддерживает `offset`/`length`. `GetStatus` отдаёт общий и свободный объём диска.

## Запуск

Нужны Docker и Docker Compose.

```sh
cp .env.example .env   # поменяй пароли
docker compose up -d --build
```

Миграции (goose) применяются автоматически при старте pastebin и gateway.

| Сервис             | Порт                                    |
|--------------------|-----------------------------------------|
| pastebin (HTTP)    | `8080`                                  |
| gateway (gRPC)     | `8090`                                  |
| storagenode 1–3    | `8051`, `8052`, `8053`                  |
| Postgres gateway   | `127.0.0.1:5432`                        |
| Postgres pastebin  | `127.0.0.1:5433`                        |
| Redis              | `127.0.0.1:6379`                        |

Порты gateway и нод открыты наружу для отладки. В gRPC включён reflection, так что с ними можно работать через `grpcurl`:

```sh
grpcurl -plaintext localhost:8090 list
grpcurl -plaintext localhost:8051 storagenode.v1.StorageNode/GetStatus
```

## HTTP API

### Создать пасту

`POST /api/pastes`

```sh
curl -X POST localhost:8080/api/pastes \
  -H 'Content-Type: application/json' \
  -d '{"content": "hello", "delete_after_view": false, "delete_at": "2030-01-01T00:00:00Z"}'
```

| Поле                | Тип      | Описание                                        |
|---------------------|----------|-------------------------------------------------|
| `content`           | string   | текст пасты, обязательно, не больше `MAX_BLOB_SIZE` |
| `delete_after_view` | bool     | удалить после первого просмотра                 |
| `delete_at`         | RFC 3339 | когда удалить; необязательно, должно быть в будущем |

Ответ `201 Created`:

```json
{"addr": "aB3_x-9q", "delete_at": "2030-01-01T00:00:00Z"}
```

### Посмотреть пасту

`GET /api/pastes/{addr}`

```sh
curl localhost:8080/api/pastes/aB3_x-9q
```

```json
{
  "addr": "aB3_x-9q",
  "content": "hello",
  "views": 0,
  "delete_after_view": false,
  "delete_at": "2030-01-01T00:00:00Z",
  "created_at": "2026-10-28T12:00:00Z"
}
```

`views` обновляется с задержкой до `PASTEBIN_FLUSH_INTERVAL`, потому что счётчики сначала копятся в Redis.

### Коды ошибок

| Код | Когда                                      |
|-----|--------------------------------------------|
| 400 | невалидный JSON, пустая паста, `delete_at` в прошлом |
| 404 | пасты нет, она истекла или уже прочитана (burn after read) |
| 413 | паста больше `MAX_BLOB_SIZE`               |
| 500 | внутренняя ошибка                          |

## Конфигурация

Значения задаются через `.env` и пробрасываются в сервисы через флаги в `docker-compose.yml`.

| Переменная                  | По умолчанию | Описание                                       |
|-----------------------------|--------------|------------------------------------------------|
| `ENV`                       | `prod`       | `dev` или `prod` (формат логов)                |
| `MAX_BLOB_SIZE`             | `524288`     | максимальный размер пасты/блоба в байтах (общий для pastebin и gateway) |
| `PASTEBIN_FLUSH_INTERVAL`   | `5s`         | как часто просмотры переносятся из Redis в Postgres |
| `PASTEBIN_DELETE_INTERVAL`  | `10s`        | как часто удаляются истёкшие пасты             |
| `PASTEBIN_SHUTDOWN_TIMEOUT` | `10s`        | сколько ждать завершения запросов при остановке |
| `PASTEBIN_DB_MAX_CONNS`     | `50`         | размер пула Postgres у pastebin                |
| `PASTEBIN_PASTE_CACHE_TTL`  | `1h`         | TTL контента в Redis                           |
| `GATEWAY_DB_MAX_CONNS`      | `50`         | размер пула Postgres у gateway                 |
| `GATEWAY_WRITE_TIMEOUT`     | `4s`         | таймаут записи блоба на одну ноду              |
| `GATEWAY_CHECK_TIMEOUT`     | `5s`         | таймаут проверки одной ноды                    |
| `REDIS_MAX_MEMORY`          | `1024mb`     | лимит памяти Redis (политика `volatile-lfu`)   |
| `POSTGRES_MAX_CONNS`        | `200`        | `max_connections` у обоих Postgres             |
| `*_DB_USER`, `*_DB_NAME`, `*_DB_PASSWORD` | | доступы к базам                         |

У gateway есть флаги, которые не вынесены в `.env` (полный список выводит `-h`):

| Флаг                  | По умолчанию   | Описание                                        |
|-----------------------|----------------|-------------------------------------------------|
| `-nodes`              | —              | ноды в формате `id=addr,id=addr`                |
| `-balancer`           | `least-conns`  | `least-conns` или `weighted-random`             |
| `-check-interval`     | `5s`           | интервал checker                                |
| `-replicate-interval` | `30s`          | интервал replicate worker                       |
| `-delete-interval`    | `10s`          | интервал delete worker                          |
| `-janitor-interval`   | `1m`           | интервал janitor                                |
| `-pending-stale`      | `5m`           | через сколько `pending` считается брошенным; должно быть больше `write-timeout` и таймаута репликации (10s) |

## Разработка

Структура:

```
api/proto/          gRPC-контракт StorageNode
gen/go/             сгенерированный код protobuf
pastebin/           HTTP-сервис: httpapi, paste (бизнес-логика), cache, views, metadata, workers
gateway/            gRPC-шлюз: server, balancer, nodepool, checker, transport, nodeio, metadata, workers
storagenode/        хранилище блобов: blob (диск), server (gRPC)
```

Тесты:

```sh
go test -race ./...
```

Кодогенерация:

```sh
# SQL-запросы → Go (pgx/v5)
(cd gateway && sqlc generate)
(cd pastebin && sqlc generate)

# protobuf
protoc -I api/proto \
  --go_out=gen/go --go_opt=paths=source_relative \
  --go-grpc_out=gen/go --go-grpc_opt=paths=source_relative \
  storagenode/v1/node.proto
```

Миграции лежат в `*/internal/metadata/migrations` и встраиваются в бинарь.

## Производительность

Замер на ноутбуке: vegeta, 500 воркеров, пасты по 1 KB, весь стек в Docker на одной машине (NVMe под LUKS). Цифры только для ориентира: на ноутбуке HPET-таймер, из-за него `time.Now` дорогой, и реальный результат на другом железе будет выше.

| Сценарий                  | RPS     | p50    | p99    | Во что упирается                        |
|---------------------------|---------|--------|--------|------------------------------------------|
| Create                    | ~1 100  | 404 ms | 1.0 s  | fsync: блоб на ноде и WAL обоих Postgres |
| View, контент в Redis     | ~16 700 | 28 ms  | 53 ms  | CPU pastebin: PG + 2 запроса в Redis на каждый просмотр |
| View, холодный            | ~4 500  | 107 ms | 214 ms | CPU gateway, аллокации на чтении         |

## Известные ограничения

- **Кворума записи нет.** OK возвращается после первой закоммиченной реплики, остальные копии догоняет репликатор. Это осознанный выбор ради latency, но если единственная нода с блобом умрёт до репликации, блоб потерян. Под большой нагрузкой репликатор (50 блобов за тик) отстаёт.
- **Нет сверки ноды и метаданных.** Файлы, оставшиеся от упавших загрузок и гонок delete/upload, не находятся и не удаляются. При чтении хэш не перепроверяется.
- **Просмотры могут посчитаться дважды**, если `Ack` в Redis упал уже после коммита в Postgres. С несколькими инстансами pastebin общий ключ `views:flushing` тоже даёт такой эффект.
- **В gateway нет поддержки нескольких инстансов**: два gateway будут конкурировать за репликацию одного блоба.
- **Безопасность.** Нет аутентификации, TLS и rate limit. gRPC-порты gateway и нод опубликованы на `0.0.0.0`. Адрес пасты занимает 48 бит, так что burn-after-read пасты в теории можно найти перебором.
- После рестарта первые запросы gateway → нода могут висеть около 5 секунд. Причина: gRPC-резолвер делает DNS TXT-запрос `_grpc_config.<host>`. Лечится опцией `grpc.WithDisableServiceConfig()`.
