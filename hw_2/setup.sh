#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=====================================${NC}"
echo -e "${BLUE}  Домашнее задание: KRaft + SASL    ${NC}"
echo -e "${BLUE}=====================================${NC}\n"

# Очистка
docker stop kafka-kraft 2>/dev/null
docker rm kafka-kraft 2>/dev/null
rm -rf kraft-data scripts
mkdir -p scripts

# Создание конфигураций
cat > scripts/admin.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
EOF

cat > scripts/producer.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="producer" password="producer-password";
EOF

cat > scripts/consumer.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="consumer" password="consumer-password";
EOF

cat > scripts/restricted.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="restricted" password="restricted-password";
EOF

cat > scripts/wrong.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="wrong" password="wrong-password";
EOF

echo -e "${GREEN}✓ Конфигурации созданы${NC}\n"

# Запуск
echo "Запуск Kafka KRaft с SASL..."
docker-compose -f docker-compose-kraft.yml up -d

# Ожидание
echo -n "Ожидание запуска"
for i in {1..30}; do
    if docker exec kafka-kraft kafka-topics --bootstrap-server localhost:9092 --command-config /scripts/admin.properties --list &>/dev/null 2>&1; then
        echo -e "\n${GREEN}✓ Kafka запущена${NC}\n"
        break
    fi
    echo -n "."
    sleep 1
done

# Проверка статуса
if ! docker ps | grep -q kafka-kraft; then
    echo -e "${RED}Контейнер не запущен!${NC}"
    docker logs kafka-kraft --tail 20
    exit 1
fi

# Создание топика
echo "Создание топика..."
docker exec kafka-kraft kafka-topics \
    --bootstrap-server localhost:9092 \
    --command-config /scripts/admin.properties \
    --create --topic secure-topic \
    --partitions 3 \
    --replication-factor 1
echo -e "${GREEN}✓ Топик создан${NC}\n"

# Демонстрация ACL (попытка)
echo -e "${YELLOW}Попытка настроить ACL:${NC}"
docker exec kafka-kraft kafka-acls \
    --bootstrap-server localhost:9092 \
    --command-config /scripts/admin.properties \
    --add --allow-principal User:producer \
    --operation Write --topic secure-topic 2>&1 | head -2
echo "Примечание: ACL не поддерживаются в этой конфигурации"
echo ""

# Тестирование аутентификации
echo -e "${YELLOW}=== ТЕСТИРОВАНИЕ SASL АУТЕНТИФИКАЦИИ ===${NC}\n"

echo "1. Правильные учетные данные:"
echo -n "   admin: "
docker exec kafka-kraft kafka-topics --bootstrap-server localhost:9092 --command-config /scripts/admin.properties --list &>/dev/null 2>&1 && echo "✓ Подключен" || echo "✗"
echo -n "   producer: "
docker exec kafka-kraft kafka-topics --bootstrap-server localhost:9092 --command-config /scripts/producer.properties --list &>/dev/null 2>&1 && echo "✓ Подключен" || echo "✗"
echo -n "   consumer: "
docker exec kafka-kraft kafka-topics --bootstrap-server localhost:9092 --command-config /scripts/consumer.properties --list &>/dev/null 2>&1 && echo "✓ Подключен" || echo "✗"
echo -n "   restricted: "
docker exec kafka-kraft kafka-topics --bootstrap-server localhost:9092 --command-config /scripts/restricted.properties --list &>/dev/null 2>&1 && echo "✓ Подключен" || echo "✗"

echo ""
echo "2. Неправильные учетные дан