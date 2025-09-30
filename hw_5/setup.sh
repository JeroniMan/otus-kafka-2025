#!/bin/bash

# setup.sh - Настройка и запуск Akka Streams проекта

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Заголовок
echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}     Akka Streams + Kafka Graph DSL Setup      ${NC}"
echo -e "${BLUE}================================================${NC}\n"

# Проверка требований
log_info "Проверка требований..."

# Docker
if ! command -v docker &> /dev/null; then
    log_error "Docker не установлен!"
    exit 1
fi

# Java
if ! command -v java &> /dev/null; then
    log_warn "Java не найдена. Установите JDK 8 или выше для запуска Scala приложения"
fi

# SBT
if ! command -v sbt &> /dev/null; then
    log_warn "SBT не найден. Установка..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install sbt
    else
        echo "Установите SBT вручную: https://www.scala-sbt.org/download.html"
    fi
fi

# Остановка предыдущих контейнеров
log_info "Остановка предыдущих контейнеров..."
docker-compose down 2>/dev/null || true

# Запуск Kafka
log_info "Запуск Kafka инфраструктуры..."
docker-compose up -d

# Ожидание готовности Kafka
log_info "Ожидание готовности Kafka..."
for i in {1..30}; do
    if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        log_info "Kafka готова!"
        break
    fi
    echo -n "."
    sleep 2
done
echo ""

# Создание топиков
log_info "Создание топиков..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic input-numbers \
    --partitions 1 \
    --replication-factor 1 2>/dev/null || true

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic output-results \
    --partitions 1 \
    --replication-factor 1 2>/dev/null || true

log_info "Топики созданы:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}✓ Настройка завершена!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Kafka UI доступен по адресу: http://localhost:9000"
echo ""
echo "Для запуска приложений используйте:"
echo "  1. ./run.sh simple   - запустить простую версию (без Kafka)"
echo "  2. ./run.sh kafka    - запустить версию с Kafka"
echo "  3. sbt run          - выбрать приложение через SBT"
echo ""