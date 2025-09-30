# Kafka Connect + PostgreSQL CDC

Захват изменений из PostgreSQL в реальном времени с помощью Debezium.

## Требования

- Docker
- Bash
- curl

## Быстрый запуск

```bash
# 1. Сделать скрипты исполняемыми
chmod +x setup.sh test.sh

# 2. Запустить и настроить всё
./setup.sh

# 3. Протестировать CDC
./test.sh
```

## Что происходит

1. **PostgreSQL** → создаются таблицы (users, products, orders)
2. **Debezium** → отслеживает все изменения в базе
3. **Kafka** → получает события об изменениях
4. **Kafka Connect** → управляет Debezium коннектором

## Структура топиков

- `postgres.inventory.users` - изменения в таблице users
- `postgres.inventory.products` - изменения в таблице products  
- `postgres.inventory.orders` - изменения в таблице orders

## Мониторинг

- http://localhost:8080 - Kafka UI (визуальный мониторинг)
- http://localhost:8083 - Kafka Connect API

## Ручное тестирование

```bash
# Добавить данные в PostgreSQL
docker exec -it postgres psql -U postgres -d testdb
> INSERT INTO inventory.users (username, email, full_name) 
  VALUES ('test', 'test@test.com', 'Test User');

# Прочитать изменения из Kafka
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic postgres.inventory.users \
  --from-beginning
```

## Остановка

```bash
docker-compose down -v
```

## Как это работает

```
PostgreSQL (изменение) 
    ↓
Debezium (захват)
    ↓
Kafka Connect (обработка)
    ↓  
Kafka Topic (событие)
```

Каждое изменение в базе (INSERT/UPDATE/DELETE) создает событие в Kafka.