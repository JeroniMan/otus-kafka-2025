#!/bin/bash

# cleanup.sh - Очистка публикации и слотов репликации

set -e

# Цвета
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

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  Очистка и перезапуск Debezium коннектора     ${NC}"
echo -e "${BLUE}================================================${NC}\n"

# 1. Удаление существующего коннектора
log_info "Удаление существующего коннектора..."
curl -X DELETE http://localhost:8083/connectors/postgres-connector 2>/dev/null || true
sleep 2

# 2. Очистка в PostgreSQL
log_info "Очистка публикации и слотов репликации в PostgreSQL..."

docker exec postgres psql -U postgres -d testdb <<EOF
-- Удаление слота репликации
SELECT pg_drop_replication_slot('debezium_slot')
WHERE EXISTS (
    SELECT 1 FROM pg_replication_slots
    WHERE slot_name = 'debezium_slot'
);

-- Удаление публикации
DROP PUBLICATION IF EXISTS dbz_publication;

-- Создание новой публикации
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Проверка
SELECT 'Публикация создана: ' || pubname FROM pg_publication WHERE pubname = 'dbz_publication';
EOF

log_info "PostgreSQL очищен и подготовлен"
sleep 2

# 3. Проверка Kafka Connect
log_info "Проверка Kafka Connect..."
if ! curl -s http://localhost:8083/ > /dev/null 2>&1; then
    log_warn "Kafka Connect не запущен. Попытка запустить..."
    docker-compose restart kafka-connect
    sleep 10
fi

# 4. Создание нового коннектора
log_info "Создание нового коннектора..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-connector.json \
  -w "\nHTTP Status: %{http_code}\n"

sleep 5

# 5. Проверка статуса
log_info "Проверка статуса коннектора..."
STATUS=$(curl -s http://localhost:8083/connectors/postgres-connector/status | jq -r '.connector.state' 2>/dev/null)

if [ "$STATUS" = "RUNNING" ]; then
    echo -e "${GREEN}✓ Коннектор успешно запущен!${NC}"
elif [ "$STATUS" = "FAILED" ]; then
    echo -e "${RED}✗ Коннектор в состоянии FAILED${NC}"
    echo "Детали ошибки:"
    curl -s http://localhost:8083/connectors/postgres-connector/status | jq '.trace' -r 2>/dev/null
else
    echo -e "${YELLOW}Статус коннектора: ${STATUS:-UNKNOWN}${NC}"
fi

# 6. Проверка топиков
echo ""
log_info "Проверка топиков Kafka..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep postgres || echo "Топики еще создаются..."

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}Очистка завершена!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Следующие шаги:"
echo "  1. Проверить статус: make status"
echo "  2. Запустить тест: ./test.sh"
echo "  3. Мониторинг: http://localhost:8080"
echo ""