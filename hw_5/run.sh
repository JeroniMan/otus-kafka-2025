#!/bin/bash

# run.sh - Запуск Akka Streams приложений

set -e

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Проверка аргументов
if [ $# -eq 0 ]; then
    echo -e "${YELLOW}Использование:${NC}"
    echo "  ./run.sh simple  - запустить простую версию (без Kafka)"
    echo "  ./run.sh kafka   - запустить версию с Kafka"
    echo "  ./run.sh both    - запустить обе версии последовательно"
    exit 1
fi

# Функция запуска простой версии
run_simple() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}   Запуск простой версии (без Kafka)           ${NC}"
    echo -e "${BLUE}================================================${NC}\n"

    sbt "runMain AkkaStreamGraphApp"
}

# Функция запуска версии с Kafka
run_kafka() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}   Запуск версии с Kafka                       ${NC}"
    echo -e "${BLUE}================================================${NC}\n"

    # Проверка Kafka
    if ! docker ps | grep -q kafka; then
        echo -e "${RED}Kafka не запущена!${NC}"
        echo "Запустите: ./setup.sh"
        exit 1
    fi

    # Очистка топиков
    echo -e "${CYAN}Очистка топиков...${NC}"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --delete --topic input-numbers 2>/dev/null || true
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --delete --topic output-results 2>/dev/null || true

    sleep 2

    # Создание топиков
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic input-numbers --partitions 1 --replication-factor 1
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic output-results --partitions 1 --replication-factor 1

    echo -e "${GREEN}Топики готовы${NC}\n"

    sbt "runMain AkkaKafkaStreamApp"
}

# Основная логика
case "$1" in
    simple)
        run_simple
        ;;
    kafka)
        run_kafka
        ;;
    both)
        run_simple
        echo -e "\n${CYAN}Пауза 3 секунды...${NC}\n"
        sleep 3
        run_kafka
        ;;
    *)
        echo -e "${RED}Неизвестная команда: $1${NC}"
        echo "Используйте: simple, kafka или both"
        exit 1
        ;;
esac

echo -e "\n${GREEN}✓ Выполнение завершено!${NC}"