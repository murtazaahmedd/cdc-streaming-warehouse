CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, tier TEXT NOT NULL DEFAULT 'free');
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers(id), amount NUMERIC(10,2) NOT NULL,
                     status TEXT NOT NULL DEFAULT 'placed');
-- full row images in `before` for updates and deletes
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
INSERT INTO customers SELECT g, 'Customer ' || g, 'c' || g || '@example.com', 'free' FROM generate_series(1, 20) g;
