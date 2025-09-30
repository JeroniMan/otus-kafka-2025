# Akka Streams Graph DSL

Демонстрация обработки потоков данных с использованием Akka Streams Graph DSL и Apache Kafka.

## Задача

Реализовать граф обработки данных:
```
Input: 1, 2, 3, 4, 5
         ↓
    Broadcast (×3)
    ↙    ↓    ↘
  ×10   ×2    ×3
    ↘    ↓    ↙
       Zip
        ↓
       Sum
        ↓
Output: 15, 30, 45, 60, 75
```

## Требования

- Java 8+
- SBT 1.9+
- Docker
- Scala 2.13

## Быстрый старт

```bash
# 1. Сделать скрипты исполняемыми
chmod +x setup.sh run.sh

# 2. Настроить инфраструктуру
./setup.sh

# 3. Запустить приложение
./run.sh simple   # без Kafka
./run.sh kafka    # с Kafka
```

## Структура проекта

```
akka-streams-graph/
├── src/main/scala/
│   ├── AkkaStreamGraphApp.scala    # Простая версия
│   └── AkkaKafkaStreamApp.scala    # Версия с Kafka
├── docker-compose.yml               # Kafka инфраструктура
├── build.sbt                        # Зависимости Scala
├── setup.sh                         # Настройка
└── run.sh                          # Запуск
```

## Как работает граф

### Полный поток данных с Kafka:

```
[Producer]          [Kafka]           [Graph Processing]        [Kafka]
Source(1-5) → input-numbers → Consumer → Broadcast → Zip → output-results
                                           ↓  ↓  ↓
                                          ×10 ×2 ×3
```

### 1. Input (отправка в Kafka)
```scala
Source(1 to 5) → ProducerRecord → input-numbers topic
```

### 2. Processing (обработка графом)
- Consumer читает из `input-numbers`
- Broadcast разделяет на 3 потока
- Каждый поток умножает на свой коэффициент
- Zip объединяет результаты
- Sum складывает элементы

### 3. Output (результаты в Kafka)
```scala
Results (15,30,45,60,75) → ProducerRecord → output-results topic
```

## Топики Kafka

- **input-numbers** - входные данные (1,2,3,4,5)
- **output-results** - результаты обработки (15,30,45,60,75)

## Проверка результатов

```bash
# Читать из output топика
make console-output

# Или через Scala приложение
make read-output

# Полная демонстрация
make demo
```

## Версии приложения

### Простая версия (AkkaStreamGraphApp)
- Работает локально без внешних зависимостей
- Использует Source(1 to 5)
- Результат выводится в консоль

### Kafka версия (AkkaKafkaStreamApp)
- Producer отправляет числа в топик `input-numbers`
- Consumer читает и применяет граф обработки
- Интеграция с реальным брокером Kafka

## Команды SBT

```bash
# Компиляция
sbt compile

# Запуск с выбором класса
sbt run

# Запуск конкретного класса
sbt "runMain AkkaStreamGraphApp"

# Тесты
sbt test

# Создание JAR
sbt assembly
```

## Мониторинг

- Kafka UI: http://localhost:9000
- Топики: `input-numbers`, `output-results`

## Устранение проблем

### Kafka не запускается
```bash
docker-compose down -v
docker-compose up -d
```

### Ошибки компиляции
```bash
sbt clean compile
```

### Проверка топиков
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Результаты

При входных данных `1, 2, 3, 4, 5`:

| Input | ×10 | ×2 | ×3 | Sum |
|-------|-----|----|----|-----|
| 1     | 10  | 2  | 3  | 15  |
| 2     | 20  | 4  | 6  | 30  |
| 3     | 30  | 6  | 9  | 45  |
| 4     | 40  | 8  | 12 | 60  |
| 5     | 50  | 10 | 15 | 75  |

## Остановка

```bash
docker-compose down
```