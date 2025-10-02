#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=====================================${NC}"
echo -e "${BLUE}  ТЕСТИРОВАНИЕ ПРАВ ДОСТУПА         ${NC}"
echo -e "${BLUE}=====================================${NC}\n"

# Функция для теста пользователя
test_user() {
    local USER=$1
    local DESC=$2

    echo -e "${YELLOW}Тестирование: $USER ($DESC)${NC}"
    echo "─────────────────────────────────────"

    # 1. Тест списка топиков
    echo -n "1. Получить список топиков: "
    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --command-config /scripts/${USER}.properties \
        --list > /tmp/test_output 2>&1

    if grep -q "secure-topic" /tmp/test_output; then
        echo -e "${GREEN}✓ Может видеть топики${NC}"
    elif grep -qi "not authorized\|denied" /tmp/test_output; then
        echo -e "${RED}✗ Нет доступа${NC}"
    else
        echo -e "${YELLOW}? Неизвестная ошибка${NC}"
    fi

    # 2. Тест записи
    echo -n "2. Записать сообщение: "
    echo "Test message from $USER" | docker exec -i kafka kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --producer.config /scripts/${USER}.properties \
        --topic secure-topic > /tmp/test_output 2>&1

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Успешно записано${NC}"
    elif grep -qi "not authorized\|denied" /tmp/test_output; then
        echo -e "${RED}✗ Нет прав на запись${NC}"
    else
        echo -e "${YELLOW}? Ошибка записи${NC}"
    fi

    # 3. Тест чтения
    echo -n "3. Прочитать сообщения: "

    # Используем фоновый процесс с ограничением времени
    (docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --consumer.config /scripts/${USER}.properties \
        --topic secure-topic \
        --from-beginning \
        --max-messages 1 > /tmp/test_output 2>&1) &

    PID=$!
    sleep 2
    kill $PID 2>/dev/null

    if grep -q "Test message" /tmp/test_output; then
        echo -e "${GREEN}✓ Успешно прочитано${NC}"
    elif grep -qi "not authorized\|denied" /tmp/test_output; then
        echo -e "${RED}✗ Нет прав на чтение${NC}"
    else
        echo -e "${YELLOW}? Нет сообщений или ошибка${NC}"
    fi

    echo ""
}

# Сначала отправим тестовое сообщение от admin
echo "Подготовка: отправка тестового сообщения..."
echo "Test message from admin" | docker exec -i kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --producer.config /scripts/admin.properties \
    --topic secure-topic 2>/dev/null
echo ""

# Тестируем каждого пользователя
test_user "admin" "суперпользователь"
test_user "producer" "права на запись"
test_user "consumer" "права на чтение"
test_user "restricted" "без прав"

# Итоговая таблица
echo -e "${BLUE}=====================================${NC}"
echo -e "${BLUE}  ОЖИДАЕМЫЕ РЕЗУЛЬТАТЫ              ${NC}"
echo -e "${BLUE}=====================================${NC}"
echo ""
echo "Пользователь  | Список | Запись | Чтение"
echo "─────────────┼────────┼────────┼────────"
echo "admin        |   ✓    |   ✓    |   ✓"
echo "producer     |   ✗    |   ✓    |   ✗"
echo "consumer     |   ✗    |   ✗    |   ✓"
echo "restricted   |   ✗    |   ✗    |   ✗"
echo ""

# Демонстрация работы
echo -e "${BLUE}=====================================${NC}"
echo -e "${BLUE}  ДЕМОНСТРАЦИЯ РАБОТЫ               ${NC}"
echo -e "${BLUE}=====================================${NC}\n"

echo "1. Producer отправляет сообщение:"
echo "   Сообщение: 'Hello from producer!'"
echo "Hello from producer!" | docker exec -i kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --producer.config /scripts/producer.properties \
    --topic secure-topic 2>&1 | grep -v ">"
echo -e "   ${GREEN}✓ Отправлено${NC}\n"

echo "2. Consumer читает сообщение:"
echo -n "   Прочитано: "
docker exec kafka sh -c 'kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --consumer.config /scripts/consumer.properties \
    --topic secure-topic \
    --from-beginning \
    --max-messages 1 2>/dev/null & \
    PID=$!; sleep 2; kill $PID 2>/dev/null'
echo ""

echo "3. Restricted пытается прочитать:"
docker exec kafka sh -c 'kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --consumer.config /scripts/restricted.properties \
    --topic secure-topic \
    --from-beginning \
    --max-messages 1 2>&1 | grep -i "not authorized" | head -1'
echo ""

rm -f /tmp/test_output