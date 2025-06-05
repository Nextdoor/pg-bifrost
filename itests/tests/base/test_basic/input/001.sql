CREATE TABLE customers (id serial primary key, first_name text, last_name text);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO customers (first_name, last_name) VALUES ('Goodbye', 'World');
UPDATE customers SET last_name = 'Friends' where first_name = 'Hello';
DELETE FROM customers WHERE first_name = 'Goodbye';

-- Nullable columns
CREATE TABLE products (id serial PRIMARY KEY, name text, sku INTEGER NULL);
ALTER TABLE products REPLICA IDENTITY FULL;
INSERT INTO products (name) VALUES ('widget 1');
UPDATE products SET sku = 42 where name = 'widget 1';
UPDATE products SET sku = NULL where name = 'widget 1';