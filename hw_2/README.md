# Kafka KRaft с SASL/PLAIN и ACL

Настройка Kafka в режиме KRaft с аутентификацией и авторизацией.

## Требования

- Docker
- Bash

## Быстрый запуск

### Вариант 1: KRaft + SASL + ACL (Zookeeper)
```bash
# Запустить и настроить всё автоматически
./setup-acl.sh

# Протестировать права доступа
./test-acl.sh
```

### Вариант 2: KRaft + SASL (без ACL)
```bash
# Запустить KRaft с только аутентификацией
./setup.sh
```

## Что настроено

### Пользователи
- **admin** (admin-secret) - суперпользователь
- **producer** (producer-secret) - права на запись
- **consumer** (consumer-secret) - права на чтение  
- **restricted** (restricted-secret) - без прав

### Топик
- **secure-topic** - 3 партиции

### Права доступа (ACL)
| Пользователь | Список топиков | Запись | Чтение |
|--------------|----------------|--------|--------|
| admin        | ✓              | ✓      | ✓      |
| producer     | ✗              | ✓      | ✗      |
| consumer     | ✗              | ✗      | ✓      |
| restricted   | ✗              | ✗      | ✗      |

## Ручное тестирование

```bash
# От имени producer (может писать)
echo "test" | docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --producer.config /scripts/producer.properties \
  --topic secure-topic

# От имени consumer (может читать)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --consumer.config /scripts/consumer.properties \
  --topic secure-topic --from-beginning

# От имени restricted (не может ничего)
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --command-config /scripts/restricted.properties \
  --list
```

## Структура

```
├── docker-compose-acl.yml   # Kafka с Zookeeper + ACL
├── docker-compose-kraft.yml # Чистый KRaft (без ACL)
├── setup-acl.sh             # Автоматическая настройка ACL
├── setup.sh                 # Настройка KRaft
├── test-acl.sh              # Тестирование прав
├── security/                # JAAS конфигурации
└── scripts/                 # Клиентские конфигурации
    ├── admin.properties
    ├── producer.properties
    ├── consumer.properties
    └── restricted.properties
```

## Остановка

```bash
docker-compose -f docker-compose-acl.yml down -v
```

Результат запуска:
<img width="925" height="474" alt="Screenshot 2025-09-30 at 01 45 27" src="https://github.com/user-attachments/assets/804b6126-f5bd-4b1c-b1ac-6a516a813284" />
