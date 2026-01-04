CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total_amount NUMERIC(10, 2) NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);

INSERT INTO users (email, name) VALUES
    ('alice@example.com', 'Alice Smith'),
    ('bob@example.com', 'Bob Johnson'),
    ('carol@example.com', 'Carol Williams'),
    ('david@example.com', 'David Brown'),
    ('eve@example.com', 'Eve Davis');

INSERT INTO orders (user_id, total_amount, status) VALUES
    (1, 150.00, 'completed'),
    (1, 75.50, 'completed'),
    (2, 200.00, 'pending'),
    (3, 50.00, 'completed'),
    (3, 125.00, 'shipped'),
    (4, 300.00, 'pending'),
    (5, 89.99, 'completed');

INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES
    (1, 'Widget A', 2, 50.00),
    (1, 'Widget B', 1, 50.00),
    (2, 'Gadget X', 1, 75.50),
    (3, 'Widget A', 4, 50.00),
    (4, 'Gadget Y', 2, 25.00),
    (5, 'Widget B', 5, 25.00),
    (6, 'Premium Set', 1, 300.00),
    (7, 'Gadget X', 1, 89.99);

SELECT count(*) FROM users;
SELECT count(*) FROM orders;
SELECT count(*) FROM users u JOIN orders o ON u.id = o.user_id;
SELECT u.name, sum(o.total_amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name;
SELECT status, count(*), sum(total_amount) FROM orders GROUP BY status;
SELECT * FROM users WHERE email = 'alice@example.com';
SELECT * FROM orders WHERE status = 'pending';
SELECT o.id, u.name, o.total_amount FROM orders o JOIN users u ON o.user_id = u.id WHERE o.status = 'completed';
