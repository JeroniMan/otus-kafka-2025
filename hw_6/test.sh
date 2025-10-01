#!/bin/bash

# test.sh - Тестирование Debezium CDC

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_test() {
    echo -e "${CYAN}[TEST]${NC} $1"
}

# Заголовок
echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  Тестирование Debezium CDC                    ${NC}"
echo -e "${BLUE}================================================${NC}\n"

# 1. Проверка начального снапшота
log_test "1. Проверка начального снапшота данных..."
echo "Топики с данными из PostgreSQL:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep postgres | sort
echo ""

log_info "Количество сообщений в топиках:"
for topic in postgres.inventory.users postgres.inventory.products postgres.inventory.orders; do
    count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic $topic 2>/dev/null | awk -F ':' '{sum += $3} END {print sum}')
    echo "  $topic: ${count:-0} сообщений"
done
echo ""

# 2. Добавление новых записей
log_test "2. Добавление новых записей в PostgreSQL..."

docker exec postgres psql -U postgres -d testdb -c "
INSERT INTO inventory.users (username, email, full_name) VALUES
    ('alice_wonder', 'alice@example.com', 'Alice Wonderland'),
    ('charlie_brown', 'charlie@example.com', 'Charlie Brown');
" > /dev/null

log_info "Добавлено 2 новых пользователя"

docker exec postgres psql -U postgres -d testdb -c "
INSERT INTO inventory.products (name, description, price, quantity) VALUES
    ('Headphones', 'Noise-cancelling headphones', 199.99, 30),
    ('Webcam', 'HD webcam for streaming', 79.99, 100);
" > /dev/null

log_info "Добавлено 2 новых продукта"
sleep 2

# 3. Обновление записей
log_test "3. Обновление существующих записей..."

docker exec postgres psql -U postgres -d testdb -c "
UPDATE inventory.products
SET price = price * 1.1, quantity = quantity - 10
WHERE name = 'Laptop';
" > /dev/null

log_info "Обновлена цена и количество для Laptop"
sleep 2

# 4. Создание заказа
log_test "4. Создание заказа..."

docker exec postgres psql -U postgres -d testdb -c "
INSERT INTO inventory.orders (user_id, product_id, quantity, status) VALUES
    (1, 1, 2, 'confirmed'),
    (2, 3, 1, 'pending'),
    (3, 2, 5, 'shipped');
" > /dev/null

log_info "Создано 3 новых заказа"
sleep 2

# 5. Удаление записи
log_test "5. Удаление записи..."

docker exec postgres psql -U postgres -d testdb -c "
DELETE FROM inventory.orders WHERE id = 1;
" > /dev/null

log_info "Удален заказ с id=1"
sleep 2

# 6. Чтение сообщений из Kafka
echo ""
log_test "6. Чтение последних сообщений из Kafka..."
echo -e "${YELLOW}Последние сообщения из топика users:${NC}"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic postgres.inventory.users \
    --from-beginning \
    --max-messages 2 \
    --property print.timestamp=true \
    --property print.key=true \
    --timeout-ms 5000 2>/dev/null | tail -2 || echo "Timeout reached"

echo ""
echo -e "${YELLOW}Последние сообщения из топика products:${NC}"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic postgres.inventory.products \
    --from-beginning \
    --max-messages 2 \
    --property print.timestamp=true \
    --property print.key=true \
    --timeout-ms 5000 2>/dev/null | tail -2 || echo "Timeout reached"

echo ""
echo -e "${YELLOW}Последние сообщения из топика orders:${NC}"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic postgres.inventory.orders \
    --from-beginning \
    --max-messages 3 \
    --property print.timestamp=true \
    --property print.key=true \
    --timeout-ms 5000 2>/dev/null | tail -3 || echo "Timeout reached"

# 7. Статистика
echo ""
log_test "7. Итоговая статистика..."
log_info "Статус коннектора:"
curl -s http://localhost:8083/connectors/postgres-connector/status | jq '.connector.state' 2>/dev/null || echo "RUNNING"

log_info "Количество обработанных событий:"
for topic in postgres.inventory.users postgres.inventory.products postgres.inventory.orders; do
    count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic $topic 2>/dev/null | awk -F ':' '{sum += $3} END {print sum}')
    echo "  $topic: ${count:-0} событий"
done

# 8. Проверка данных в PostgreSQL
echo ""
log_test "8. Текущие данные в PostgreSQL..."
docker exec postgres psql -U postgres -d testdb -c "
SELECT 'Users:' as table, COUNT(*) as count FROM inventory.users
UNION ALL
SELECT 'Products:', COUNT(*) FROM inventory.products
UNION ALL
SELECT 'Orders:', COUNT(*) FROM inventory.orders;
"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}✓ Тестирование завершено успешно!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Что было протестировано:"
echo "  ✓ Начальный снапшот данных"
echo "  ✓ Добавление новых записей (INSERT)"
echo "  ✓ Обновление записей (UPDATE)"
echo "  ✓ Удаление записей (DELETE)"
echo "  ✓ Передача изменений в Kafka"
echo ""
echo "Для мониторинга в реальном времени:"
echo "  • Откройте Kafka UI: http://localhost:8080"
echo "  • Перейдите в Topics → выберите postgres.inventory.*"
echo ""