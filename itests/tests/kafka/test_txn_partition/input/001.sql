CREATE TABLE customers (id serial primary key, first_name text, last_name text);

BEGIN;
INSERT INTO customers (first_name, last_name)
SELECT 'foo', 'bar '|| x.id FROM generate_series(1,10) AS x(id);
COMMIT;

BEGIN;
INSERT INTO customers (first_name, last_name)
SELECT 'baz', 'qux '|| x.id FROM generate_series(1,10) AS x(id);
COMMIT;

INSERT INTO customers (first_name, last_name) VALUES ('1111', '1111');
