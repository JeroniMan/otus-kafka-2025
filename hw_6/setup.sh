#!/bin/bash

# setup.sh - Автоматическая настройка Kafka Connect + PostgreSQL + Debezium

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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
echo -e "${BLUE}  Kafka Connect + PostgreSQL + Debezium CDC    ${NC}"
echo -e "${BLUE}================================================${NC}\n"

# Создание необходимых директорий
log_info "Создание директорий..."
mkdir -p sql connectors

# Остановка и очистка предыдущих запусков
log_info "Очистка предыдущих запусков..."
docker-compose down -v 2>/dev/null || true

# Запуск инфраструктуры
log_info "Запуск инфраструктуры..."
docker-compose up -d

# Ожидание готовности сервисов
log_info "Ожидание готовности PostgreSQL..."
until docker exec postgres pg_isready -U postgres > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo ""
log_info "PostgreSQL готов!"

log_info "Ожидание готовности Kafka..."
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo ""
log_info "Kafka готова!"

log_info "Ожидание готовности Kafka Connect..."
until curl -s http://localhost:8083/ > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo ""
log_info "Kafka Connect готов!"

# Небольшая пауза для стабилизации
sleep 5

# Проверка доступных коннекторов
log_info "Доступные коннекторы:"
curl -s http://localhost:8083/connector-plugins | jq '.[].class' 2>/dev/null || curl -s http://localhost:8083/connector-plugins

# Проверка и очистка существующих публикаций и слотов
log_info "Проверка и очистка PostgreSQL..."
docker exec postgres psql -U postgres -d testdb <<'EOF' 2>/dev/null || true
-- Удаление существующего слота репликации
SELECT pg_drop_replication_slot('debezium_slot')
WHERE EXISTS (
    SELECT 1 FROM pg_replication_slots
    WHERE slot_name = 'debezium_slot'
);

-- Удаление существующей публикации
DROP PUBLICATION IF EXISTS dbz_publication;

-- Создание новой публикации
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
EOF
log_info "PostgreSQL подготовлен для CDC"

# Удаление старого коннектора если существует
log_info "Удаление старого коннектора (если существует)..."
curl -X DELETE http://localhost:8083/connectors/postgres-connector 2>/dev/null || true
sleep 2

# Регистрация Debezium коннектора
log_info "Регистрация Debezium PostgreSQL коннектора..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-connector.json \
  -w "\nHTTP Status: %{http_code}\n"

sleep 3

# Проверка статуса коннектора
log_info "Проверка статуса коннектора..."
curl -s http://localhost:8083/connectors/postgres-connector/status | jq . 2>/dev/null || curl -s http://localhost:8083/connectors/postgres-connector/status

# Список топиков
log_info "Созданные топики в Kafka:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep postgres || echo "Топики еще создаются..."

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}✓ Настройка завершена успешно!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Полезные ссылки:"
echo "  • Kafka UI: http://localhost:8080"
echo "  • Kafka Connect API: http://localhost:8083"
echo "  • PostgreSQL: localhost:5432 (user: postgres, pass: postgres)"
echo ""
echo "Следующие шаги:"
echo "  1. Запустить тестирование: ./test.sh"
echo "  2. Или добавить данные вручную:"
echo "     docker exec -it postgres psql -U postgres -d testdb"
echo ""