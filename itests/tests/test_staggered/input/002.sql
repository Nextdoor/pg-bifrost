SELECT pg_sleep(2);

BEGIN;
INSERT INTO customers (first_name, last_name) VALUES ('2', '2');

SELECT pg_sleep(10);

INSERT INTO customers (first_name, last_name) VALUES ('3', '3');
COMMIT;