#!/usr/bin/env python3
# kafka_streams_app.py

import asyncio
import logging
from datetime import timedelta, datetime
from typing import Dict
import faust

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация приложения
app = faust.App(
    'event-counter',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    web_enabled=True,
    web_port=6066,
    topic_partitions=1,
)

# Определение топика events
events_topic = app.topic('events', value_type=bytes, key_type=bytes)

# Обычная таблица БЕЗ windowing для упрощения
# Будем управлять окнами вручную
event_counts = app.Table(
    'event-counts',
    default=int,
    partitions=1,
)

# Глобальные переменные для управления окном вручную
current_window_counts = {}
window_start_time = datetime.now()
WINDOW_SIZE_MINUTES = 5


@app.agent(events_topic)
async def count_events(stream):
    """
    Agent для обработки событий и подсчета по ключам
    """
    global current_window_counts, window_start_time

    async for event in stream.events():
        try:
            # Получаем ключ из события
            key = event.key
            if key:
                key = key.decode('utf-8') if isinstance(key, bytes) else str(key)
            else:
                key = 'unknown'

            # Получаем значение из события
            value = event.value
            if value:
                value = value.decode('utf-8') if isinstance(value, bytes) else str(value)
            else:
                value = ''

            # Проверяем, не истекло ли окно
            current_time = datetime.now()
            if current_time - window_start_time >= timedelta(minutes=WINDOW_SIZE_MINUTES):
                logger.info("=== Window Expired - Resetting Counters ===")
                logger.info(f"Final stats for expired window: {current_window_counts}")
                current_window_counts = {}
                window_start_time = current_time
                # Очищаем таблицу
                for k in list(event_counts.keys()):
                    event_counts.pop(k, None)

            # Увеличиваем счетчик в таблице
            event_counts[key] += 1

            # Обновляем локальный счетчик для текущего окна
            if key not in current_window_counts:
                current_window_counts[key] = 0
            current_window_counts[key] += 1

            # Получаем текущий счетчик
            current_count = current_window_counts[key]

            # Логируем результат
            logger.info(f"Key: {key} | Count in window: {current_count} | Value: {value}")

            # Выводим статистику каждые 5 событий
            total_events = sum(current_window_counts.values())
            if total_events > 0 and total_events % 5 == 0:
                print_current_stats()

        except Exception as e:
            logger.error(f"Error processing event: {e}")
            logger.debug(f"Event: {event}, Error details: {e}")


def print_current_stats():
    """
    Вывод текущей статистики окна
    """
    global current_window_counts, window_start_time

    if current_window_counts:
        logger.info("=== Current Window Statistics ===")
        logger.info(f"Window started: {window_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(
            f"Time remaining: {WINDOW_SIZE_MINUTES - (datetime.now() - window_start_time).seconds // 60} minutes")
        for key, count in sorted(current_window_counts.items()):
            logger.info(f"  {key}: {count} events")
        logger.info(f"  Total: {sum(current_window_counts.values())} events")
        logger.info("================================")


@app.timer(interval=30.0)  # Каждые 30 секунд
async def periodic_stats():
    """
    Периодический вывод статистики
    """
    print_current_stats()


@app.timer(interval=60.0)  # Каждую минуту
async def check_window():
    """
    Проверка и сброс окна если прошло 5 минут
    """
    global current_window_counts, window_start_time

    if datetime.now() - window_start_time >= timedelta(minutes=WINDOW_SIZE_MINUTES):
        logger.info("=== Timer: Window Expired - Resetting ===")
        if current_window_counts:
            logger.info(f"Final window stats: {current_window_counts}")
        current_window_counts = {}
        window_start_time = datetime.now()
        # Очищаем таблицу
        for key in list(event_counts.keys()):
            event_counts.pop(key, None)
        logger.info("Window reset complete")


@app.page('/stats')
async def stats_page(web, request):
    """
    Web endpoint для просмотра статистики
    """
    global current_window_counts, window_start_time

    time_elapsed = (datetime.now() - window_start_time).seconds
    time_remaining = max(0, WINDOW_SIZE_MINUTES * 60 - time_elapsed)

    return web.json({
        'status': 'ok',
        'window_size_minutes': WINDOW_SIZE_MINUTES,
        'event_counts': current_window_counts.copy() if current_window_counts else {},
        'total_events': sum(current_window_counts.values()) if current_window_counts else 0,
        'window_started_at': window_start_time.isoformat(),
        'window_expires_at': (window_start_time + timedelta(minutes=WINDOW_SIZE_MINUTES)).isoformat(),
        'time_remaining_seconds': time_remaining
    })


@app.page('/health')
async def health_check(web, request):
    """
    Health check endpoint
    """
    return web.json({
        'status': 'healthy',
        'app': 'event-counter',
        'timestamp': datetime.now().isoformat()
    })


@app.page('/reset')
async def reset_window(web, request):
    """
    Endpoint для ручного сброса окна (для тестирования)
    """
    global current_window_counts, window_start_time

    old_stats = current_window_counts.copy()
    current_window_counts = {}
    window_start_time = datetime.now()

    # Очищаем таблицу
    for key in list(event_counts.keys()):
        event_counts.pop(key, None)

    return web.json({
        'status': 'reset',
        'old_stats': old_stats,
        'message': 'Window has been reset'
    })


if __name__ == '__main__':
    logger.info("Starting Kafka Streams Application...")
    logger.info("Web interface available at: http://localhost:6066")
    logger.info("Statistics endpoint: http://localhost:6066/stats")
    logger.info("Health check: http://localhost:6066/health")
    logger.info("Manual reset: http://localhost:6066/reset")
    logger.info(f"Window size: {WINDOW_SIZE_MINUTES} minutes")
    app.main()