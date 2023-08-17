CREATE TABLE customers (id serial primary key, first_name text, last_name text);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');

TRUNCATE TABLE customers;

CREATE TABLE orders (
    id serial primary key,
    customer_id int references customers(id)
);

TRUNCATE TABLE customers CASCADE;