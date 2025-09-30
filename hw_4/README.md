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