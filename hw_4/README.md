# Kafka Streams Event Counter

Подсчет событий с одинаковыми ключами в 5-минутном окне.

## Требования

- Python 3.8+
- Docker

## Установка

```bash
pip install faust-streaming kafka-python aiohttp
```

## Запуск

### 1. Запустить Kafka
```bash
docker-compose up -d
```

### 2. Запустить приложение
```bash
python kafka_streams_app.py worker -l info
```

### 3. Отправить тестовые данные
```bash
# В новом терминале
python test_producer.py
```

## Проверка работы

```bash
# Статистика
curl http://localhost:6066/stats

# Сбросить окно вручную
curl http://localhost:6066/reset
```

## Endpoints

- http://localhost:6066/stats - текущая статистика
- http://localhost:6066/health - проверка здоровья
- http://localhost:6066/reset - сбросить окно
- http://localhost:9000 - Kafdrop UI

## Как это работает

1. Приложение читает события из топика `events`
2. Считает количество событий для каждого ключа
3. Автоматически сбрасывает счетчики каждые 5 минут
4. Показывает статистику через Web API

Результаты запуска:
<img width="1512" height="982" alt="Screenshot 2025-09-30 at 22 59 37" src="https://github.com/user-attachments/assets/feae1287-31b0-4a9b-ba76-934b98ac61cb" />
<img width="1512" height="176" alt="Screenshot 2025-09-30 at 22 59 59" src="https://github.com/user-attachments/assets/bae8683a-187b-44d2-9cb5-6055aef542da" />
<img width="1187" height="701" alt="Screenshot 2025-09-30 at 23 05 43" src="https://github.com/user-attachments/assets/5ce882cf-7b8c-44e9-b4d5-8cf8425095f0" />
