CREATE TABLE customers (first_name text, last_name text);

SELECT pg_sleep(6);

INSERT INTO customers (first_name, last_name) VALUES ('1', '1');