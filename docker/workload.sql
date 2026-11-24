-- Run while the sink is consuming: inserts, updates, a delete and a schema change.
INSERT INTO orders SELECT g, 1 + (g % 20), round((random() * 500)::numeric, 2), 'placed' FROM generate_series(1, 200) g;
UPDATE orders SET status = 'paid' WHERE id % 3 = 0;
UPDATE customers SET tier = 'pro' WHERE id <= 5;
DELETE FROM orders WHERE id % 10 = 0;
ALTER TABLE customers ADD COLUMN phone TEXT;
UPDATE customers SET phone = '+92-300-' || lpad(id::text, 7, '0') WHERE id <= 3;
