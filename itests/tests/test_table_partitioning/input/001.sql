CREATE TABLE customers (id serial primary key, first_name text, last_name text);
CREATE TABLE users (id serial primary key, first_name text, last_name text);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO users (first_name, last_name) VALUES ('Baz', 'Bar');


BEGIN;
INSERT INTO customers (first_name, last_name)
SELECT 'foo', '' || x.id FROM generate_series(1,1000) AS x(id);
INSERT INTO users (first_name, last_name)
SELECT 'bar', '' || x.id FROM generate_series(1,1000) AS x(id);
COMMIT;

BEGIN;
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
COMMIT;

SELECT pg_sleep(5);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');

SELECT pg_sleep(5);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO users (first_name, last_name) VALUES ('Foo', 'Bar');
