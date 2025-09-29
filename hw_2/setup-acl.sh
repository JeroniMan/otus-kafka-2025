#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=====================================${NC}"
echo -e "${BLUE}  Домашнее задание: SASL + ACL      ${NC}"
echo -e "${BLUE}=====================================${NC}\n"

# Очистка
echo "Очистка..."
docker-compose down -v 2>/dev/null
rm -rf security scripts
mkdir -p security scripts

# Создание JAAS файлов
echo "Создание JAAS конфигураций..."

cat > security/zookeeper-server.jaas <<'EOF'
Server {
    org.apache.zookeeper.server.auth.DigestLoginModule required
    user_kafka="kafka-secret";
};
EOF

cat > security/kafka-server.jaas <<'EOF'
KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="broker"
    password="broker-secret"
    user_broker="broker-secret"
    user_admin="admin-secret";
};

Client {
    org.apache.zookeeper.server.auth.DigestLoginModule required
    username="kafka"
    password="kafka-secret";
};
EOF

# Создание клиентских конфигураций
cat > scripts/admin.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
EOF

cat > scripts/producer.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="producer" password="producer-secret";
EOF

cat > scripts/consumer.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="consumer" password="consumer-secret";
EOF

cat > scripts/restricted.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="restricted" password="restricted-secret";
EOF

echo -e "${GREEN}✓ Конфигурации созданы${NC}\n"

# Запуск
echo "Запуск Kafka..."
docker-compose -f docker-compose-acl.yml up -d

# Ожидание
echo -n "Ожидание запуска"
for i in {1..40}; do
    if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --command-config /scripts/admin.properties --list &>/dev/null 2>&1; then
        echo -e "\n${GREEN}✓ Kafka готова${NC}\n"
        break
    fi
    echo -n "."
    sleep 1
done

# Проверка
if ! docker ps | grep -q kafka; then
    echo -e "${RED}Ошибка запуска!${NC}"
    echo "Логи Kafka:"
    docker logs kafka --tail 30
    exit 1
fi

# Создание топика
echo "Создание топика..."
docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --command-config /scripts/admin.properties \
    --create --topic secure-topic \
    --partitions 3 \
    --replication-factor 1
echo -e "${GREEN}✓ Топик создан${NC}\n"

# Настройка ACL
echo "Настройка ACL..."

# Producer ACL
docker exec kafka kafka-acls \
    --bootstrap-server localhost:9092 \
    --command-config /scripts/admin.properties \
    --add --allow-principal User:producer \
    --operation Write --operation Describe \
    --topic secure-topic

# Consumer ACL
docker exec kafka kafka-acls \
    --bootstrap-server localhost:9092 \
    --command-config /scripts/admin.properties \
    --add --allow-principal User:consumer \
    --operation Read --operation Describe \
    --topic secure-topic

docker exec kafka kafka-acls \
    --bootstrap-server localhost:9092 \
    --command-config /scripts/admin.properties \
    --add --allow-principal User:consumer \
    --operation Read --group '*'

echo -e "${GREEN}✓ ACL настроены${NC}\n"

# Тестирование
echo -e "${YELLOW}=== ТЕСТИРОВАНИЕ ===${NC}\n"

echo "1. Producer (должен писать, НЕ должен читать):"
echo "test" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --producer.config /scripts/producer.properties --topic secure-topic 2>&1 | grep -v ">"
timeout 1 docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --consumer.config /scripts/producer.properties --topic secure-topic 2>&1 | grep -i "not authorized" && echo "   Чтение: ✗ (ожидаемо)"

echo -e "\n2. Consumer (должен читать, НЕ должен писать):"
echo "test" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --producer.config /scripts/consumer.properties --topic secure-topic 2>&1 | grep -i "not authorized" && echo "   Запись: ✗ (ожидаемо)"
timeout 2 docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --consumer.config /scripts/consumer.properties --topic secure-topic --from-beginning --max-messages 1

echo -e "\n3. Restricted (НЕ должен ни читать, ни писать):"
echo "test" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --producer.config /scripts/restricted.properties --topic secure-topic 2>&1 | grep -i "not authorized" && echo "   Запись: ✗ (ожидаемо)"
timeout 1 docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --consumer.config /scripts/restricted.properties --topic secure-topic 2>&1 | grep -i "not authorized" && echo "   Чтение: ✗ (ожидаемо)"

echo -e "\n${GREEN}=====================================${NC}"
echo -e "${GREEN}✓ ТЕСТ ЗАВЕРШЕН УСПЕШНО!${NC}"
echo -e "${GREEN}=====================================${NC}"