-- sql/init.sql

-- Создание схемы
CREATE SCHEMA IF NOT EXISTS inventory;

-- Создание таблицы пользователей
CREATE TABLE inventory.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы продуктов
CREATE TABLE inventory.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2),
    quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы заказов
CREATE TABLE inventory.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES inventory.users(id),
    product_id INTEGER REFERENCES inventory.products(id),
    quantity INTEGER NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending'
);

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Триггеры для обновления updated_at
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON inventory.users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON inventory.products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Удаление старой публикации если существует
DROP PUBLICATION IF EXISTS dbz_publication;

-- Создание публикации для логической репликации (нужно для Debezium)
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Вставка начальных данных
INSERT INTO inventory.users (username, email, full_name) VALUES
    ('john_doe', 'john@example.com', 'John Doe'),
    ('jane_smith', 'jane@example.com', 'Jane Smith'),
    ('bob_jones', 'bob@example.com', 'Bob Jones');

INSERT INTO inventory.products (name, description, price, quantity) VALUES
    ('Laptop', 'High-performance laptop', 999.99, 50),
    ('Mouse', 'Wireless mouse', 29.99, 200),
    ('Keyboard', 'Mechanical keyboard', 89.99, 150),
    ('Monitor', '27-inch 4K monitor', 399.99, 75);

-- Проверка что всё создано
SELECT 'Database initialized successfully!' as message;