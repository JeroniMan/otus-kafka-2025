#!/bin/bash

# demo.sh - Демонстрация CDC в реальном времени

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

clear

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  ДЕМОНСТРАЦИЯ CDC В РЕАЛЬНОМ ВРЕМЕНИ          ${NC}"
echo -e "${BLUE}================================================${NC}\n"

echo -e "${YELLOW}Инструкция:${NC}"
echo "1. Этот скрипт будет вносить изменения в PostgreSQL"
echo "2. В другом терминале запустите мониторинг Kafka:"
echo -e "${CYAN}   docker exec kafka kafka-console-consumer \\
     --bootstrap-server localhost:9092 \\
     --topic postgres.inventory.users \\
     --from-beginning${NC}"
echo ""
echo "Или откройте Kafka UI: http://localhost:8080"
echo ""
read -p "Нажмите Enter для начала демонстрации..."

# Функция для паузы с сообщением
pause_with_message() {
    echo -e "${MAGENTA}→ $1${NC}"
    sleep 3
}

# 1. INSERT операция
echo ""
echo -e "${GREEN}═══ 1. INSERT: Добавление нового пользователя ═══${NC}"
pause_with_message "Добавляем пользователя 'demo_user'..."

docker exec postgres psql -U postgres -d testdb -c "
INSERT INTO inventory.users (username, email, full_name)
VALUES ('demo_user', 'demo@example.com', 'Demo User');
" > /dev/null

echo "✓ Пользователь добавлен"
echo -e "${YELLOW}Проверьте Kafka - должно появиться событие с op:'c' (create)${NC}"
sleep 2

# 2. UPDATE операция
echo ""
echo -e "${GREEN}═══ 2. UPDATE: Изменение данных пользователя ═══${NC}"
pause_with_message "Меняем email пользователя..."

docker exec postgres psql -U postgres -d testdb -c "
UPDATE inventory.users
SET email = 'demo_updated@example.com',
    full_name = 'Demo User (Updated)'
WHERE username = 'demo_user';
" > /dev/null

echo "✓ Данные обновлены"
echo -e "${YELLOW}Проверьте Kafka - должно появиться событие с op:'u' (update)${NC}"
echo -e "${YELLOW}Обратите внимание на поля 'before' и 'after'${NC}"
sleep 2

# 3. Массовое добавление
echo ""
echo -e "${GREEN}═══ 3. BULK INSERT: Массовое добавление продуктов ═══${NC}"
pause_with_message "Добавляем 5 новых продуктов..."

docker exec postgres psql -U postgres -d testdb -c "
INSERT INTO inventory.products (name, description, price, quantity) VALUES
    ('Product A', 'Description A', 10.99, 100),
    ('Product B', 'Description B', 20.99, 200),
    ('Product C', 'Description C', 30.99, 300),
    ('Product D', 'Description D', 40.99, 400),
    ('Product E', 'Description E', 50.99, 500);
" > /dev/null

echo "✓ Добавлено 5 продуктов"
echo -e "${YELLOW}В топике products должно появиться 5 событий${NC}"
sleep 2

# 4. Транзакционная операция
echo ""
echo -e "${GREEN}═══ 4. TRANSACTION: Создание заказа с обновлением количества ═══${NC}"
pause_with_message "Выполняем транзакцию..."

docker exec postgres psql -U postgres -d testdb -c "
BEGIN;
-- Создаем заказ
INSERT INTO inventory.orders (user_id, product_id, quantity, status)
SELECT u.id, p.id, 2, 'confirmed'
FROM inventory.users u, inventory.products p
WHERE u.username = 'demo_user' AND p.name = 'Product A';

-- Уменьшаем количество товара
UPDATE inventory.products
SET quantity = quantity - 2
WHERE name = 'Product A';

COMMIT;
" > /dev/null

echo "✓ Транзакция выполнена"
echo -e "${YELLOW}Должны появиться события в топиках orders и products${NC}"
sleep 2

# 5. DELETE операция
echo ""
echo -e "${GREEN}═══ 5. DELETE: Удаление пользователя ═══${NC}"
pause_with_message "Удаляем тестового пользователя..."

docker exec postgres psql -U postgres -d testdb -c "
DELETE FROM inventory.users WHERE username = 'demo_user';
" > /dev/null

echo "✓ Пользователь удален"
echo -e "${YELLOW}Проверьте Kafka - должно появиться событие с op:'d' (delete)${NC}"
sleep 2

# 6. Статистика
echo ""
echo -e "${BLUE}═══ СТАТИСТИКА ═══${NC}"
echo "Текущее состояние базы данных:"
docker exec postgres psql -U postgres -d testdb -t -c "
SELECT 'Пользователей: ' || COUNT(*) FROM inventory.users
UNION ALL
SELECT 'Продуктов: ' || COUNT(*) FROM inventory.products
UNION ALL
SELECT 'Заказов: ' || COUNT(*) FROM inventory.orders;
"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}✓ Демонстрация завершена!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Что было продемонстрировано:"
echo "  • INSERT - добавление записей"
echo "  • UPDATE - изменение записей"
echo "  • DELETE - удаление записей"
echo "  • TRANSACTION - транзакционные операции"
echo "  • Все изменения отправлены в Kafka в реальном времени"
echo ""
echo "Для детального просмотра событий:"
echo "  • Kafka UI: http://localhost:8080"
echo "  • Topics → postgres.inventory.*"
echo ""