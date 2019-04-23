CREATE TABLE customers (id serial primary key, first_name text, last_name text);

INSERT INTO customers (first_name, last_name)
SELECT 'foo', 'bar '|| x.id FROM generate_series(1,10000) AS x(id);

SELECT pg_sleep(5);

INSERT INTO customers (first_name, last_name)
SELECT 'foo', 'bar '|| x.id FROM generate_series(10001,20000) AS x(id);

SELECT pg_sleep(5);

INSERT INTO customers (first_name, last_name)
SELECT 'foo', 'bar '|| x.id FROM generate_series(20001,30000) AS x(id);

SELECT pg_sleep(5);

INSERT INTO customers (first_name, last_name)
SELECT 'foo', 'bar '|| x.id FROM generate_series(30001,40000) AS x(id);

SELECT pg_sleep(5);

INSERT INTO customers (first_name, last_name)
SELECT 'foo', 'bar '|| x.id FROM generate_series(40001,50000) AS x(id);
