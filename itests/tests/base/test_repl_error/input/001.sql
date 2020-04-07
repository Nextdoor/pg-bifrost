CREATE TABLE actions (id serial primary key, msg text);

INSERT INTO actions (msg) VALUES ('Before create extension');
CREATE EXTENSION pglogical;
INSERT INTO actions (msg) VALUES ('After create extension');


INSERT INTO actions (msg) VALUES ('Before pglogical.create_node');
SELECT pglogical.create_node (node_name := 'provider', dsn := 'host=192.168.1.1 port=5432 dbname=postgres');
INSERT INTO actions (msg) VALUES ('After pglogical.create_node');

INSERT INTO actions (msg) VALUES ('Before pglogical.drop_node');
SELECT pglogical.drop_node('provider');
INSERT INTO actions (msg) VALUES ('After pglogical.drop_node');

INSERT INTO actions (msg) VALUES ('Before drop extension');
DROP EXTENSION pglogical;
INSERT INTO actions (msg) VALUES ('After extension');

INSERT INTO actions (msg) VALUES ('Before drop pglogical schema');
DROP SCHEMA pglogical;
INSERT INTO actions (msg) VALUES ('this is a test');

SELECT pg_sleep(10);

CREATE TABLE customers (id serial primary key, first_name text, last_name text);
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO customers (first_name, last_name) VALUES ('Goodbye', 'World');
UPDATE customers SET last_name = 'Friends' where first_name = 'Hello';
DELETE FROM customers WHERE first_name = 'Goodbye';
