CREATE TABLE customers (id serial primary key, first_name text, last_name text);

BEGIN;
INSERT INTO customers (first_name, last_name)
SELECT 'foo', 'bar '|| x.id FROM generate_series(1,7333) AS x(id);
COMMIT;

BEGIN;
INSERT INTO customers (first_name, last_name)
SELECT 'baz', 'qux '|| x.id FROM generate_series(1,2667) AS x(id);
COMMIT;