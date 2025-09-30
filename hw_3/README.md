# Kafka Transactions

Демонстрация транзакционных сообщений в Kafka с commit и abort.

## Требования

- Python 3.8+
- Docker

## Установка

```bash
pip install kafka-python
```

## Запуск

### 1. Запустить Kafka
```bash
docker-compose up -d
```

### 2. Отправить сообщения с транзакциями
```bash
python producer.py
```
- Транзакция 1: отправит 10 сообщений и подтвердит (COMMIT)
- Транзакция 2: отправит 4 сообщения и отменит (ABORT)

### 3. Прочитать только подтвержденные сообщения
```bash
python consumer.py
```

## Проверка работы

```bash
# Читать только подтвержденные (по умолчанию)
python consumer.py

# Читать все включая отмененные
python consumer.py --read-uncommitted
```

## Что происходит

1. **Producer** создает 2 транзакции:
   - ✅ Первая: 5 сообщений в topic1 + 5 в topic2 → COMMIT
   - ❌ Вторая: 2 сообщения в topic1 + 2 в topic2 → ABORT

2. **Consumer** с `read_committed`:
   - Видит только 10 сообщений из первой транзакции
   - НЕ видит 4 сообщения из отмененной транзакции

## UI

- http://localhost:8090 - Kafka UI для мониторинга


Результат запуска:
<img width="1003" height="851" alt="Screenshot 2025-09-30 at 00 18 57" src="https://github.com/user-attachments/assets/3ff43956-a852-4b92-81e1-5a7d5843252a" />
<img width="1021" height="797" alt="Screenshot 2025-09-30 at 00 18 40" src="https://github.com/user-attachments/assets/fb85157c-2cd1-444a-be12-85c32a53d4ac" />
