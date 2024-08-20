CREATE TABLE customers (id serial primary key, first_name text, last_name text);
ALTER TABLE customers REPLICA IDENTITY FULL;

INSERT INTO customers (first_name, last_name) VALUES ('1111', '1111');
INSERT INTO customers (first_name, last_name) VALUES ('2222', repeat('2', 1048576));
INSERT INTO customers (first_name, last_name) VALUES ('3333', '3333');

BEGIN;
INSERT INTO customers (first_name, last_name) VALUES ('4444', '4444');
INSERT INTO customers (first_name, last_name) VALUES ('5555', repeat('5', 1048576));
INSERT INTO customers (first_name, last_name) VALUES ('6666', '6666');
COMMIT;

BEGIN;
INSERT INTO customers (first_name, last_name) VALUES ('7777', repeat('7', 1048576));
COMMIT;

INSERT INTO customers (first_name, last_name) VALUES ('8888', '8888');
