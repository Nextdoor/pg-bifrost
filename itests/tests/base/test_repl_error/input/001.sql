CREATE TABLE customers (id serial primary key, first_name text, last_name text);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');

BEGIN;
CREATE EXTENSION pglogical;
SELECT pglogical.create_node (node_name := 'provider', dsn := 'host=192.168.1.1 port=5432 dbname=postgres');
SELECT pglogical.drop_node('provider');
DROP EXTENSION pglogical;
DROP SCHEMA pglogical;
COMMIT;

SELECT pg_sleep(10);

INSERT INTO customers (first_name, last_name) VALUES ('Goodbye', 'World');
UPDATE customers SET last_name = 'Friends' where first_name = 'Hello';
DELETE FROM customers WHERE first_name = 'Goodbye';
